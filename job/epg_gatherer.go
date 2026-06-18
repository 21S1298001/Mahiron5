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
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/config"
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
)

func RegisterEPGGatherer(mgr *JobManager, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig, epgRetentionDays int) {
	mgr.Register(JobDefinition{
		Key:          EPGGathererKey,
		Name:         EPGGathererName,
		Handler:      epgGathererHandler(mgr, pm, sm, stm, channels, epgRetentionDays),
		IsRerunnable: true,
	})
}

type epgCandidate struct{ typ, channel string }

func epgGathererHandler(mgr *JobManager, pm *program.ProgramManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig, epgRetentionDays int) func(context.Context) error {
	return func(ctx context.Context) error {
		services, err := sm.GetServices(ctx)
		if err != nil {
			return fmt.Errorf("get services: %w", err)
		}
		byChannel := make(map[string][]uint16)
		for _, item := range services {
			key := item.ChannelType + "\x00" + item.ChannelId
			byChannel[key] = append(byChannel[key], item.NetworkId)
		}
		groups := make(map[uint16][]epgCandidate)
		seen := make(map[uint16]map[string]bool)
		for _, configured := range channels {
			if configured.IsDisabled != nil && *configured.IsDisabled {
				continue
			}
			key := configured.Type + "\x00" + configured.Channel
			for _, nid := range byChannel[key] {
				if seen[nid] == nil {
					seen[nid] = make(map[string]bool)
				}
				if seen[nid][key] {
					continue
				}
				seen[nid][key] = true
				groups[nid] = append(groups[nid], epgCandidate{configured.Type, configured.Channel})
			}
		}
		queued := 0
		for nid, candidates := range groups {
			if err := ctx.Err(); err != nil {
				return err
			}
			networkID := nid
			networkCandidates := append([]epgCandidate(nil), candidates...)
			definition := JobDefinition{
				Key: fmt.Sprintf("epg-gather:nid:%d", networkID), Name: fmt.Sprintf("EPG Gather NID %d", networkID), IsRerunnable: true,
				Handler: func(childCtx context.Context) error {
					return gatherNetworkEPG(childCtx, pm, stm, networkID, networkCandidates)
				},
			}
			if _, err := mgr.EnqueueDefinition(definition); err != nil {
				if errors.Is(err, ErrJobAlreadyRunning) {
					continue
				}
				return err
			}
			queued++
		}
		slog.Info("EPG gatherer dispatched", "networks", len(groups), "queued", queued)

		if epgRetentionDays > 0 {
			cutoff := time.Now().Add(-time.Duration(epgRetentionDays) * 24 * time.Hour).UnixMilli()
			if err := pm.DeleteEndedBefore(ctx, cutoff); err != nil {
				slog.Warn("failed to clean up old EPG data", "err", err)
			}
		}

		return nil
	}
}

func gatherNetworkEPG(ctx context.Context, pm *program.ProgramManager, stm *stream.StreamManager, networkID uint16, candidates []epgCandidate) error {
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
		session, err := stm.GetOrCreateWait(userCtx, candidate.typ, candidate.channel)
		if err == nil {
			err = collectSessionEPG(userCtx, pm, session)
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

func collectSessionEPG(ctx context.Context, pm *program.ProgramManager, session *stream.ChannelSession) error {
	collectCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		slog.Debug("starting EITS collection")
		errCh <- collectEITSUntilComplete(collectCtx, pm, session.CollectEITS)
		slog.Debug("finished EITS collection")
		cancel()
	}()
	go func() {
		defer wg.Done()
		slog.Debug("starting EITPF collection")
		errCh <- collectEITJSONL(collectCtx, pm, session.CollectEITPF)
		slog.Debug("finished EITPF collection")
	}()
	wg.Wait()
	close(errCh)

	var result error
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func collectEITSUntilComplete(ctx context.Context, pm *program.ProgramManager, collect func(context.Context, io.Writer) error) error {
	collectCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r, w := io.Pipe()
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- readEITSUntilComplete(collectCtx, cancel, pm, r)
	}()

	collectErr := collect(collectCtx, w)
	_ = w.Close()
	readErr := <-readErrCh
	_ = r.Close()
	if errors.Is(collectErr, context.Canceled) && readErr == nil {
		collectErr = nil
	}
	return errors.Join(collectErr, readErr)
}

func collectEITJSONL(ctx context.Context, pm *program.ProgramManager, collect func(context.Context, io.Writer) error) error {
	r, w := io.Pipe()
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- pm.ReadEITJSONL(ctx, r)
	}()

	collectErr := collect(ctx, w)
	_ = w.Close()
	readErr := <-readErrCh
	_ = r.Close()
	return errors.Join(collectErr, readErr)
}

func readEITSUntilComplete(ctx context.Context, cancel context.CancelFunc, pm *program.ProgramManager, r io.Reader) error {
	tracker := program.NewEITSCompletionTracker()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var section program.EITSection
		if err := json.Unmarshal(line, &section); err != nil {
			return err
		}
		if err := pm.UpsertEITSection(ctx, &section); err != nil {
			return err
		}
		complete := tracker.Observe(&section)
		collectedSections, totalSections, _ := tracker.Progress(&section)
		slog.Debug("received EITS section",
			"networkId", section.OriginalNetworkID,
			"transportStreamId", section.TransportStreamID,
			"serviceId", section.ServiceID,
			"tableId", section.TableID,
			"versionNumber", section.VersionNumber,
			"sectionNumber", section.SectionNumber,
			"lastSectionNumber", section.LastSectionNumber,
			"collectedSections", collectedSections,
			"totalSections", totalSections,
			"events", len(section.Events),
		)
		if complete {
			slog.Debug("completed EITS collection", "tables", tracker.TableCount())
			cancel()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if tracker.Complete() {
		return nil
	}
	if ctx.Err() == nil {
		return errors.New("EITS stream ended before all sections were collected")
	}
	return ctx.Err()
}
