package job

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/processor"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/google/uuid"
)

const (
	EPGGathererKey  = "epg-gatherer"
	EPGGathererName = "EPG Gatherer"

	EPGGathererDefaultSchedule = "20,50 * * * *"
	// epgStableDuration is the quiet window after the last EITS update before we
	// declare a snapshot complete. ARIB broadcasts may roll the EIT table over the
	// course of a few minutes, so we wait this long to confirm no further updates
	// are in flight.
	epgStableDuration = 30 * time.Second
)

func RegisterEPGGatherer(mgr *JobManager, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig, epgRetentionDays int, retrievalTime time.Duration) {
	mgr.Register(JobDefinition{
		Key:          EPGGathererKey,
		Name:         EPGGathererName,
		Handler:      epgGathererHandler(mgr, pm, sm, stm, channels, epgRetentionDays, retrievalTime),
		IsRerunnable: true,
		RetryDelays:  []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
	})
}

type epgCandidate struct{ typ, channel string }
type epgNetwork struct {
	candidates []epgCandidate
	services   []program.ServiceKey
}

func epgGathererHandler(mgr *JobManager, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig, epgRetentionDays int, retrievalTime time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		services, err := sm.GetServices(ctx)
		if err != nil {
			return fmt.Errorf("get services: %w", err)
		}
		if len(services) == 0 {
			return errors.New("EPG gathering requires scanned services")
		}
		grouped := groupServicesByNetwork(services, channels)
		queued := 0
		for nid, group := range grouped {
			if err := ctx.Err(); err != nil {
				return err
			}
			enqueued, err := enqueueEPGGatherForNetwork(ctx, mgr, pm, sm, stm, channels, retrievalTime, nid, group.candidates, group.services)
			if err != nil {
				return err
			}
			if enqueued {
				queued++
			}
		}
		slog.Info("EPG gatherer dispatched", "networks", len(grouped), "queued", queued)

		if epgRetentionDays > 0 {
			cutoff := time.Now().Add(-time.Duration(epgRetentionDays) * 24 * time.Hour).UnixMilli()
			if err := pm.DeleteEndedBefore(ctx, cutoff); err != nil {
				slog.Warn("failed to clean up old EPG data", "err", err)
			}
		}

		return nil
	}
}

func groupServicesByNetwork(services []*service.Service, channels config.ChannelsConfig) map[uint16]*epgNetwork {
	byChannel := make(map[string][]uint16)
	for _, item := range services {
		key := item.ChannelType + "\x00" + item.ChannelId
		byChannel[key] = append(byChannel[key], item.NetworkId)
	}
	groups := make(map[uint16]*epgNetwork)
	seen := make(map[uint16]map[string]bool)
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := configured.Type + "\x00" + configured.Channel
		for _, nid := range byChannel[key] {
			if groups[nid] == nil {
				groups[nid] = &epgNetwork{}
			}
			if seen[nid] == nil {
				seen[nid] = make(map[string]bool)
			}
			if seen[nid][key] {
				continue
			}
			seen[nid][key] = true
			groups[nid].candidates = append(groups[nid].candidates, epgCandidate{configured.Type, configured.Channel})
		}
	}
	serviceSeen := make(map[program.ServiceKey]bool)
	for _, svc := range services {
		key := program.ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId}
		if groups[svc.NetworkId] != nil && !serviceSeen[key] {
			groups[svc.NetworkId].services = append(groups[svc.NetworkId].services, key)
			serviceSeen[key] = true
		}
	}
	return groups
}

// buildNetworkEPGInputs looks up the channel candidates and services belonging
// to networkID from the currently stored services. Returns the inputs needed
// to enqueue or invoke an EPG gather for that network.
func buildNetworkEPGInputs(ctx context.Context, sm *service.ServiceManager, channels config.ChannelsConfig, networkID uint16) ([]epgCandidate, []program.ServiceKey, error) {
	services, err := sm.GetServices(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get services: %w", err)
	}
	byChannel := make(map[string]bool)
	for _, item := range services {
		if item.NetworkId != networkID {
			continue
		}
		key := item.ChannelType + "\x00" + item.ChannelId
		byChannel[key] = true
	}
	var candidates []epgCandidate
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := configured.Type + "\x00" + configured.Channel
		if byChannel[key] {
			candidates = append(candidates, epgCandidate{configured.Type, configured.Channel})
		}
	}
	serviceSeen := make(map[program.ServiceKey]bool)
	var networkServices []program.ServiceKey
	for _, svc := range services {
		if svc.NetworkId != networkID {
			continue
		}
		key := program.ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId}
		if !serviceSeen[key] {
			serviceSeen[key] = true
			networkServices = append(networkServices, key)
		}
	}
	return candidates, networkServices, nil
}

// enqueueEPGGatherForNetwork enqueues the per-network EPG gather job for the
// given network ID, ignoring ErrJobAlreadyRunning. It is used by both the
// EPGGatherer cron handler and by callers (e.g. the service updater) that
// want to trigger gathering for a freshly discovered network without waiting
// for the next cron tick. Returns true when a job was actually enqueued (not
// already running and not skipped for having no services).
func enqueueEPGGatherForNetwork(ctx context.Context, mgr *JobManager, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig, retrievalTime time.Duration, networkID uint16, presetCandidates []epgCandidate, presetServices []program.ServiceKey) (bool, error) {
	var candidates []epgCandidate
	var services []program.ServiceKey
	if len(presetCandidates) > 0 || len(presetServices) > 0 {
		candidates = presetCandidates
		services = presetServices
	} else {
		var err error
		candidates, services, err = buildNetworkEPGInputs(ctx, sm, channels, networkID)
		if err != nil {
			return false, err
		}
	}
	if len(services) == 0 {
		return false, nil
	}
	nid := networkID
	networkCandidates := append([]epgCandidate(nil), candidates...)
	networkServices := append([]program.ServiceKey(nil), services...)
	definition := JobDefinition{
		Key: fmt.Sprintf("epg-gather:nid:%d", nid), Name: fmt.Sprintf("EPG Gather NID %d", nid), IsRerunnable: true,
		Handler: func(childCtx context.Context) error {
			return gatherNetworkEPG(childCtx, pm, sm, stm, nid, networkCandidates, networkServices, retrievalTime)
		},
		RetryDelays: []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute},
		RetryIf:     retryableEPGError,
	}
	if _, err := mgr.EnqueueDefinition(definition); err != nil {
		if errors.Is(err, ErrJobAlreadyRunning) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func gatherNetworkEPG(ctx context.Context, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, networkID uint16, candidates []epgCandidate, services []program.ServiceKey, retrievalTime time.Duration) error {
	if len(services) == 0 {
		return fmt.Errorf("network %d has no known services", networkID)
	}
	ordered := make([]epgCandidate, 0, len(candidates))
	active := make(map[epgCandidate]bool, len(candidates))
	for _, candidate := range candidates {
		if stm.HasSession(candidate.typ, candidate.channel) {
			active[candidate] = true
			ordered = append(ordered, candidate)
		}
	}
	for _, candidate := range candidates {
		if !active[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	var result error
	for _, candidate := range ordered {
		yes := true
		userCtx := tuner.WithUser(ctx, tuner.User{
			ID: uuid.NewString(), Priority: -1, Agent: "Mahiron EPG Gatherer",
			StreamSetting: tuner.StreamSetting{
				Channel:  &config.ChannelConfig{Type: candidate.typ, Channel: candidate.channel},
				ParseEIT: &yes,
			},
		})
		// GetOrCreate is non-blocking: if no tuner is free we fail fast and rely on
		// RetryDelays to back off. We do not want EPG collection to starve live
		// streams or recording sessions.
		session, err := stm.GetOrCreate(userCtx, candidate.typ, candidate.channel)
		if err == nil {
			err = collectServiceSnapshots(userCtx, pm, sm, session, services, retrievalTime, epgStableDuration)
		}
		if err == nil {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.typ, "channel", candidate.channel)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result = errors.Join(result, fmt.Errorf("%s/%s: %w", candidate.typ, candidate.channel, err))
	}
	if result == nil {
		return fmt.Errorf("network %d has no channel candidates", networkID)
	}
	return result
}

func retryableEPGError(err error) bool {
	return err != nil && !errors.Is(err, processor.ErrMirakcAribRequired)
}

func collectServiceSnapshots(ctx context.Context, pm *program.ProgramManager, sm *service.ServiceManager, session *stream.ChannelSession, expected []program.ServiceKey, retrievalTime, stableDuration time.Duration) error {
	if len(expected) == 0 {
		return errors.New("collectServiceSnapshots: expected is empty")
	}
	expectedByNID := make(map[uint16]map[uint16]struct{}, len(expected))
	for _, key := range expected {
		if expectedByNID[key.NetworkID] == nil {
			expectedByNID[key.NetworkID] = make(map[uint16]struct{})
		}
		expectedByNID[key.NetworkID][key.ServiceID] = struct{}{}
	}
	matchesExpected := func(section *program.EITSection) bool {
		ids, ok := expectedByNID[section.OriginalNetworkID]
		if !ok {
			return false
		}
		_, ok = ids[section.ServiceID]
		return ok
	}

	startedAt := time.Now().UnixMilli()
	for _, key := range expected {
		_ = sm.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, "")
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	eitsR, eitsW := io.Pipe()
	pfR, pfW := io.Pipe()

	collectErrCh := make(chan error, 2)
	go func() {
		collectErrCh <- session.CollectEITS(collectCtx, eitsW)
		_ = eitsW.Close()
	}()
	go func() {
		collectErrCh <- session.CollectEITPF(collectCtx, pfW)
		_ = pfW.Close()
	}()

	type sectionResult struct {
		section *program.EITSection
		err     error
	}
	sectionCh := make(chan sectionResult, 1)
	go func() {
		defer close(sectionCh)
		scanner := bufio.NewScanner(eitsR)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var section program.EITSection
			if err := json.Unmarshal(line, &section); err != nil {
				sectionCh <- sectionResult{err: err}
				return
			}
			select {
			case sectionCh <- sectionResult{section: &section}:
			case <-collectCtx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			sectionCh <- sectionResult{err: err}
		}
	}()

	pfErrCh := make(chan error, 1)
	go func() {
		defer close(pfErrCh)
		scanner := bufio.NewScanner(pfR)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var section program.EITSection
			if err := json.Unmarshal(line, &section); err != nil {
				pfErrCh <- err
				return
			}
			if !matchesExpected(&section) {
				continue
			}
			slog.Debug("upserting EIT section", "source", "eitpf", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
			if err := pm.UpsertEITSection(collectCtx, &section); err != nil {
				pfErrCh <- err
				return
			}
		}
		if err := scanner.Err(); err != nil {
			pfErrCh <- err
		}
	}()

	snapshot := program.NewEITSnapshot()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	finished := false
	for !finished {
		select {
		case result, ok := <-sectionCh:
			if !ok {
				finished = true
				break
			}
			if result.err != nil {
				cancel()
				_ = eitsR.Close()
				_ = pfR.Close()
				now := time.Now().UnixMilli()
				msg := result.err.Error()
				for _, key := range expected {
					_ = sm.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, msg)
				}
				return result.err
			}
			if result.section == nil {
				continue
			}
			if !matchesExpected(result.section) {
				continue
			}
			slog.Debug("observed EIT section", "source", "eits", "networkId", result.section.OriginalNetworkID, "serviceId", result.section.ServiceID, "tableId", result.section.TableID, "sectionNumber", result.section.SectionNumber, "lastSectionNumber", result.section.LastSectionNumber, "version", result.section.VersionNumber, "events", len(result.section.Events))
			snapshot.Observe(result.section, time.Now())
		case now := <-ticker.C:
			if snapshot.AllComplete(expected) && snapshot.StableFor(now, stableDuration) {
				finished = true
			}
		case <-collectCtx.Done():
			finished = true
		}
	}
	cancel()
	_ = eitsR.Close()
	_ = pfR.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-collectErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Debug("EPG collector finished with error", "err", err)
			}
		case <-time.After(2 * time.Second):
		}
	}
	select {
	case err := <-pfErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Debug("EITPF upsert finished with error", "err", err)
		}
	case <-time.After(2 * time.Second):
	}

	now := time.Now().UnixMilli()
	var result error
	for _, key := range expected {
		if snapshot.ServiceComplete(key) {
			programs := snapshot.Programs(key)
			slog.Info("publishing EPG snapshot", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
			if err := pm.ReplaceServicePrograms(ctx, key.NetworkID, key.ServiceID, now, programs); err != nil {
				_ = sm.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
				result = errors.Join(result, fmt.Errorf("service %d: publish: %w", key.ServiceID, err))
				continue
			}
			if err := sm.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
				result = errors.Join(result, err)
			}
		} else {
			slog.Warn("EITS snapshot incomplete",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", snapshot.CompletionReport(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			_ = sm.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error())
			result = errors.Join(result, err)
		}
	}
	return result
}
