package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/ts"
)

type Candidate struct {
	Type    string
	Channel string
}

type Network struct {
	Candidates []Candidate
	Services   []ServiceKey
}

type CollectResult struct {
	Observed   []ServiceKey
	Unobserved []ServiceKey
}

type eitClockCollector interface {
	CollectEITWithClock(context.Context, func(*ts.EIT, time.Time) error) error
}

const eitsCollectionBuffer = 4096

var partialEITSFlushInterval = 5 * time.Second
var eitsStableStopDuration = 3 * time.Second

func groupServicesByNetwork(services []*service.Service, channels config.ChannelsConfig) map[uint16]*Network {
	byChannel := make(map[string][]uint16)
	networkTypes := make(map[uint16]map[string]bool)
	typeNetworks := make(map[string]map[uint16]bool)
	for _, item := range services {
		key := epgChannelKey(item.ChannelType, item.ChannelId)
		byChannel[key] = append(byChannel[key], item.NetworkId)
		if networkTypes[item.NetworkId] == nil {
			networkTypes[item.NetworkId] = make(map[string]bool)
		}
		networkTypes[item.NetworkId][item.ChannelType] = true
		if typeNetworks[item.ChannelType] == nil {
			typeNetworks[item.ChannelType] = make(map[uint16]bool)
		}
		typeNetworks[item.ChannelType][item.NetworkId] = true
	}
	groups := make(map[uint16]*Network)
	seen := make(map[uint16]map[string]bool)
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := epgChannelKey(configured.Type, configured.Channel)
		candidateNetworks := byChannel[key]
		if broadEPGCandidateType(configured.Type) && len(typeNetworks[configured.Type]) == 1 {
			candidateNetworks = nil
			for nid := range typeNetworks[configured.Type] {
				candidateNetworks = append(candidateNetworks, nid)
			}
		}
		for _, nid := range candidateNetworks {
			if groups[nid] == nil {
				groups[nid] = &Network{}
			}
			if seen[nid] == nil {
				seen[nid] = make(map[string]bool)
			}
			if seen[nid][key] {
				continue
			}
			seen[nid][key] = true
			groups[nid].Candidates = append(groups[nid].Candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[ServiceKey]bool)
	for _, svc := range services {
		if !svc.EITScheduleFlag {
			continue
		}
		key := ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId, TransportStreamID: svc.TransportStreamId}
		if groups[svc.NetworkId] != nil && !serviceSeen[key] {
			groups[svc.NetworkId].Services = append(groups[svc.NetworkId].Services, key)
			serviceSeen[key] = true
		}
	}
	return groups
}

func buildNetworkInputs(ctx context.Context, serviceStore ServiceStore, channels config.ChannelsConfig, networkID uint16) ([]Candidate, []ServiceKey, error) {
	storedServices, err := serviceStore.GetServices(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get services: %w", err)
	}
	byChannel := make(map[string]bool)
	networkTypes := make(map[string]bool)
	typeNetworks := make(map[string]map[uint16]bool)
	for _, item := range storedServices {
		if typeNetworks[item.ChannelType] == nil {
			typeNetworks[item.ChannelType] = make(map[uint16]bool)
		}
		typeNetworks[item.ChannelType][item.NetworkId] = true
		if item.NetworkId != networkID {
			continue
		}
		key := epgChannelKey(item.ChannelType, item.ChannelId)
		byChannel[key] = true
		networkTypes[item.ChannelType] = true
	}
	var candidates []Candidate
	for _, configured := range channels {
		if configured.IsDisabled != nil && *configured.IsDisabled {
			continue
		}
		key := epgChannelKey(configured.Type, configured.Channel)
		if byChannel[key] || broadEPGCandidateForNetwork(configured.Type, typeNetworks, networkID) && networkTypes[configured.Type] {
			candidates = append(candidates, Candidate{Type: configured.Type, Channel: configured.Channel})
		}
	}
	serviceSeen := make(map[ServiceKey]bool)
	var networkServices []ServiceKey
	for _, svc := range storedServices {
		if svc.NetworkId != networkID {
			continue
		}
		if !svc.EITScheduleFlag {
			continue
		}
		key := ServiceKey{NetworkID: svc.NetworkId, ServiceID: svc.ServiceId, TransportStreamID: svc.TransportStreamId}
		if !serviceSeen[key] {
			serviceSeen[key] = true
			networkServices = append(networkServices, key)
		}
	}
	return candidates, networkServices, nil
}

func broadEPGCandidateType(channelType string) bool {
	return channelType == "BS" || channelType == "CS"
}

func broadEPGCandidateForNetwork(channelType string, typeNetworks map[string]map[uint16]bool, networkID uint16) bool {
	return broadEPGCandidateType(channelType) && len(typeNetworks[channelType]) == 1 && typeNetworks[channelType][networkID]
}

func epgChannelKey(channelType, channelID string) string {
	return channelType + "\x00" + channelID
}

func gatherNetwork(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, streams StreamManager, networkID uint16, candidates []Candidate, serviceKeys []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGGatherNetwork,
		observability.AttrEPGNetworkID.Int(int(networkID)),
		observability.AttrEPGCandidates.Int(len(candidates)),
		observability.AttrEPGServices.Int(len(serviceKeys)),
	)
	defer func() { observability.EndSpan(span, err) }()

	if len(serviceKeys) == 0 {
		return fmt.Errorf("network %d has no known services", networkID)
	}
	ordered := make([]Candidate, 0, len(candidates))
	active := make(map[Candidate]bool, len(candidates))
	for _, candidate := range candidates {
		if streams.HasSession(candidate.Type, candidate.Channel) {
			active[candidate] = true
			ordered = append(ordered, candidate)
		}
	}
	for _, candidate := range candidates {
		if !active[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	remaining := append([]ServiceKey(nil), serviceKeys...)
	var result error
	for _, candidate := range ordered {
		if len(remaining) == 0 {
			return nil
		}
		slog.Info("starting network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "services", len(remaining), "activeSession", active[candidate])
		candidateCtx, candidateSpan := observability.StartSpan(ctx, observability.SpanEPGGatherCandidate,
			observability.AttrEPGNetworkID.Int(int(networkID)),
			observability.AttrChannelType.String(candidate.Type),
			observability.AttrChannelID.String(candidate.Channel),
			observability.AttrStreamActiveSession.Bool(active[candidate]),
		)
		var candidateErr error
		sessionCtx, cancel := context.WithTimeout(candidateCtx, retrievalTime)
		session, candidateErr := streams.GetOrCreateWait(sessionCtx, candidate.Type, candidate.Channel)
		cancel()
		var collectResult *CollectResult
		if candidateErr == nil {
			collectResult, candidateErr = CollectServiceSnapshots(candidateCtx, programStore, serviceStore, session, remaining, retrievalTime)
		}
		observability.EndSpan(candidateSpan, candidateErr)
		if collectResult != nil && len(collectResult.Observed) > 0 {
			remaining = serviceKeyDifference(remaining, collectResult.Observed)
		}
		if candidateErr == nil && len(remaining) == 0 {
			slog.Debug("finished network EPG collection", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if candidateErr != nil {
			slog.Warn("network EPG collection candidate failed", "networkId", networkID, "type", candidate.Type, "channel", candidate.Channel, "remainingServices", len(remaining), "err", candidateErr)
			result = errors.Join(result, fmt.Errorf("%s/%s: %w", candidate.Type, candidate.Channel, candidateErr))
		}
	}
	if result == nil {
		if len(ordered) == 0 {
			return fmt.Errorf("network %d has no channel candidates", networkID)
		}
		result = fmt.Errorf("network %d EITS incomplete for %d services", networkID, len(remaining))
	}
	slog.Warn("network EPG collection failed", "networkId", networkID, "candidates", len(ordered), "err", result)
	return result
}

func serviceKeyDifference(keys, remove []ServiceKey) []ServiceKey {
	seen := make(map[ServiceKey]struct{}, len(remove))
	for _, key := range remove {
		seen[key] = struct{}{}
	}
	out := keys[:0]
	for _, key := range keys {
		if _, ok := seen[key]; !ok {
			out = append(out, key)
		}
	}
	return out
}

func CollectServiceSnapshots(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, session interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, expected []ServiceKey, retrievalTime time.Duration) (result *CollectResult, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGCollectServiceSnapshots,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

	if len(expected) == 0 {
		return nil, errors.New("collectServiceSnapshots: expected is empty")
	}
	result = &CollectResult{}
	expectedByNID := make(map[uint16]map[uint16]map[uint16]struct{}, len(expected))
	expectedNetworks := make(map[uint16]struct{}, len(expected))
	for _, key := range expected {
		expectedNetworks[key.NetworkID] = struct{}{}
		if expectedByNID[key.NetworkID] == nil {
			expectedByNID[key.NetworkID] = make(map[uint16]map[uint16]struct{})
		}
		if expectedByNID[key.NetworkID][key.TransportStreamID] == nil {
			expectedByNID[key.NetworkID][key.TransportStreamID] = make(map[uint16]struct{})
		}
		expectedByNID[key.NetworkID][key.TransportStreamID][key.ServiceID] = struct{}{}
	}
	matchesExpected := func(section *EITSection) bool {
		byTSID, ok := expectedByNID[section.OriginalNetworkID]
		if !ok {
			return false
		}
		ids, ok := byTSID[section.TransportStreamID]
		if !ok {
			// A zero TSID is only used by older tests and in-memory fakes. Real
			// scanned services always carry the ARIB transport_stream_id.
			ids, ok = byTSID[0]
		}
		if !ok {
			return false
		}
		_, ok = ids[section.ServiceID]
		return ok
	}
	matchesCollectionNetwork := func(section *EITSection) bool {
		_, ok := expectedNetworks[section.OriginalNetworkID]
		return ok
	}

	var latestClock time.Time
	var collectMu sync.Mutex
	now := func() time.Time {
		collectMu.Lock()
		defer collectMu.Unlock()
		if !latestClock.IsZero() {
			return latestClock
		}
		return time.Now()
	}
	nowMillis := func() int64 {
		return now().UnixMilli()
	}

	startedAt := nowMillis()
	source := "eits"
	lister, hasStoredPrograms := session.(StoredProgramLister)
	if hasStoredPrograms {
		source = "remote"
	}
	for _, key := range expected {
		if err := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, startedAt, ""); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, source, "attempt")
		}
	}
	if hasStoredPrograms {
		err := syncStoredServicePrograms(ctx, programStore, serviceStore, lister, expected, retrievalTime)
		if err == nil {
			result.Observed = append(result.Observed, expected...)
		}
		return result, err
	}
	collectCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	type collectionResult struct {
		collectErr error
	}
	collectDone := make(chan collectionResult, 1)

	sectionCh := make(chan *EITSection, eitsCollectionBuffer)
	go func() {
		observeEIT := func(eit *ts.EIT, clock time.Time) error {
			if !clock.IsZero() {
				collectMu.Lock()
				latestClock = clock
				collectMu.Unlock()
			}
			section := EITSectionFromTS(eit)
			if section == nil || !matchesCollectionNetwork(section) {
				return nil
			}
			select {
			case sectionCh <- section:
			case <-collectCtx.Done():
				return collectCtx.Err()
			}
			return nil
		}
		var collectErr error
		if collector, ok := session.(eitClockCollector); ok {
			collectErr = collector.CollectEITWithClock(collectCtx, observeEIT)
		} else {
			collectErr = session.CollectEIT(collectCtx, func(eit *ts.EIT) error {
				return observeEIT(eit, time.Time{})
			})
		}
		collectDone <- collectionResult{collectErr: collectErr}
	}()

	snapshot := NewSnapshot()
	pfUpserts := newEITPFUpserter(collectCtx, programStore)
	defer pfUpserts.wait()
	defer pfUpserts.stop()
	partialFlushes := newPartialEITSFlusher(collectCtx, programStore)
	defer partialFlushes.wait()
	defer partialFlushes.stop()
	flushTicker := time.NewTicker(partialEITSFlushInterval)
	defer flushTicker.Stop()
	dirtyServices := make(map[ServiceKey]struct{})
	observedServices := make(map[ServiceKey]struct{})
	finished := false
	var collectorResult collectionResult
	collectorDone := false
	for !finished {
		select {
		case section := <-sectionCh:
			if section == nil || !matchesCollectionNetwork(section) {
				continue
			}
			if ts.IsEITPF(section.TableID) {
				if !matchesExpected(section) {
					continue
				}
				slog.Debug("upserting EIT section", "source", "eitpf", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
				pfUpserts.enqueue(section.Programs())
				continue
			}
			slog.Debug("observed EIT section", "source", "eits", "networkId", section.OriginalNetworkID, "serviceId", section.ServiceID, "tableId", section.TableID, "sectionNumber", section.SectionNumber, "lastSectionNumber", section.LastSectionNumber, "version", section.VersionNumber, "events", len(section.Events))
			snapshot.Observe(section, now())
			key := ServiceKey{NetworkID: section.OriginalNetworkID, ServiceID: section.ServiceID, TransportStreamID: section.TransportStreamID}
			dirtyServices[key] = struct{}{}
			observedServices[key] = struct{}{}
			if shouldStopEITSCollection(snapshot, expected) && snapshot.StableFor(now(), eitsStableStopDuration) {
				cancel()
			}
		case <-flushTicker.C:
			if partialFlushes.flush(snapshot, dirtyServices) {
				dirtyServices = make(map[ServiceKey]struct{})
			}
			if shouldStopEITSCollection(snapshot, expected) && snapshot.StableFor(now(), eitsStableStopDuration) {
				cancel()
			}
		case collectorResult = <-collectDone:
			collectorDone = true
			finished = true
			cancel()
		case <-collectCtx.Done():
			finished = true
		}
	}
	cancel()
	dirtyServices = nil
	pfUpserts.stop()
	pfUpserts.wait()
	partialFlushes.stop()
	partialFlushes.wait()
	if !collectorDone {
		if ctx.Err() != nil {
			slog.Debug("skipping EPG collector drain during shutdown", "err", ctx.Err())
		} else {
			select {
			case collectorResult = <-collectDone:
				collectorDone = true
			case <-time.After(2 * time.Second):
			}
		}
	}
	if collectorDone {
		if collectorResult.collectErr != nil && !errors.Is(collectorResult.collectErr, context.Canceled) {
			slog.Debug("EPG collector finished with error", "err", collectorResult.collectErr)
		}
		if pfErr := pfUpserts.Err(); pfErr != nil {
			slog.Debug("EITPF upsert finished with error", "err", pfErr)
		}
	}

	updatedAt := nowMillis()
	var collectErr error
	observed := 0
	expectedObserved := 0
	var unobserved error
	observedPrograms := make(map[ServiceKey][]*program.Program)
	var allObservedPrograms []*program.Program
	mergeKeys := append([]ServiceKey(nil), expected...)
	expectedSeen := make(map[ServiceKey]struct{}, len(expected))
	for _, key := range expected {
		expectedSeen[key] = struct{}{}
	}
	for key := range observedServices {
		if _, ok := expectedSeen[key]; !ok && snapshot.Observed(key) {
			mergeKeys = append(mergeKeys, key)
		}
	}
	for _, key := range mergeKeys {
		if !snapshot.Observed(key) {
			continue
		}
		programs := snapshot.Programs(key)
		observedPrograms[key] = programs
		allObservedPrograms = append(allObservedPrograms, programs...)
	}
	fillProgramsFromSharedPeers(allObservedPrograms)
	for _, key := range mergeKeys {
		_, isExpected := expectedSeen[key]
		if snapshot.Observed(key) {
			if isExpected {
				result.Observed = append(result.Observed, key)
				expectedObserved++
			}
			observed++
			programs := observedPrograms[key]
			report := snapshot.CompletionReport(key)
			basicComplete := snapshot.ServiceComplete(key)
			observedExtendedComplete := snapshot.ObservedExtendedReady([]ServiceKey{key})
			missingTitles, titleTotal := programTitleCounts(programs)
			if !basicComplete {
				slog.Warn("flushing incomplete EITS collection",
					"networkId", key.NetworkID,
					"serviceId", key.ServiceID,
					"report", report)
			}
			slog.Info("finished EITS collection",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"programs", len(programs),
				"missingTitles", missingTitles,
				"titleTotal", titleTotal,
				"basicComplete", basicComplete,
				"observedExtendedComplete", observedExtendedComplete,
				"report", report)
			mergeCtx, mergeSpan := observability.StartSpan(ctx, observability.SpanEPGMergeServicePrograms,
				observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
				observability.AttrEPGServiceID.Int(int(key.ServiceID)),
				observability.AttrProgramCount.Int(len(programs)),
			)
			mergeCtx = observability.ContextWithEPGMetricSource(mergeCtx, "eits")
			err := programStore.UpsertPrograms(mergeCtx, programs)
			observability.EndSpan(mergeSpan, err)
			if err != nil {
				if isExpected {
					if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
						observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
					}
				}
				collectErr = errors.Join(collectErr, fmt.Errorf("service %d: merge: %w", key.ServiceID, err))
				continue
			}
			if isExpected {
				if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, updatedAt); err != nil {
					observability.RecordEPGServiceUpdateError(ctx, "eits", "success")
					collectErr = errors.Join(collectErr, err)
				}
				if warning := lowQualityProgramWarning(programs); warning != "" {
					slog.Warn("EITS collection quality is low", "networkId", key.NetworkID, "serviceId", key.ServiceID, "warning", warning)
					if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, warning); attemptErr != nil {
						observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
					}
				}
			} else if warning := lowQualityProgramWarning(programs); warning != "" {
				slog.Warn("EITS collection quality is low", "networkId", key.NetworkID, "serviceId", key.ServiceID, "warning", warning)
			}
		} else if isExpected {
			result.Unobserved = append(result.Unobserved, key)
			slog.Warn("EITS snapshot incomplete",
				"networkId", key.NetworkID,
				"serviceId", key.ServiceID,
				"report", snapshot.CompletionReport(key))
			err := fmt.Errorf("service %d EITS incomplete", key.ServiceID)
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, updatedAt, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "eits", "attempt")
			}
			unobserved = errors.Join(unobserved, err)
		}
	}
	if expectedObserved == 0 {
		collectErr = errors.Join(collectErr, unobserved)
	}
	return result, collectErr
}

type partialEITSFlusher struct {
	ctx       context.Context
	program   ProgramStore
	requests  chan []*program.Program
	done      chan struct{}
	closeOnce sync.Once
}

func newPartialEITSFlusher(ctx context.Context, programStore ProgramStore) *partialEITSFlusher {
	f := &partialEITSFlusher{
		ctx:      observability.ContextWithEPGMetricSource(ctx, "eits"),
		program:  programStore,
		requests: make(chan []*program.Program, 1),
		done:     make(chan struct{}),
	}
	go f.run()
	return f
}

func (f *partialEITSFlusher) flush(snapshot *Snapshot, dirty map[ServiceKey]struct{}) bool {
	if snapshot == nil || len(dirty) == 0 {
		return true
	}
	var programs []*program.Program
	for key := range dirty {
		programs = append(programs, snapshot.Programs(key)...)
	}
	if len(programs) == 0 {
		return true
	}
	select {
	case f.requests <- programs:
		return true
	default:
		slog.Debug("skipping partial EITS flush while previous flush is still running", "programs", len(programs))
		return false
	}
}

func (f *partialEITSFlusher) stop() {
	f.closeOnce.Do(func() {
		close(f.requests)
	})
}

func (f *partialEITSFlusher) wait() {
	<-f.done
}

func (f *partialEITSFlusher) run() {
	defer close(f.done)
	for programs := range f.requests {
		slog.Debug("upserting partial EITS snapshot", "programs", len(programs))
		if err := f.program.UpsertPrograms(f.ctx, programs); err != nil {
			slog.Debug("partial EITS upsert finished with error", "err", err)
		}
	}
}

type eitPFUpserter struct {
	ctx       context.Context
	program   ProgramStore
	requests  chan []*program.Program
	done      chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	err     error
	pending int
}

func newEITPFUpserter(ctx context.Context, programStore ProgramStore) *eitPFUpserter {
	u := &eitPFUpserter{
		ctx:      observability.ContextWithEPGMetricSource(ctx, "eitpf"),
		program:  programStore,
		requests: make(chan []*program.Program, eitsCollectionBuffer),
		done:     make(chan struct{}),
	}
	go u.run()
	return u
}

func (u *eitPFUpserter) enqueue(programs []*program.Program) {
	if len(programs) == 0 {
		return
	}
	u.mu.Lock()
	if u.err != nil || u.pending != 0 {
		u.mu.Unlock()
		return
	}
	u.pending++
	u.mu.Unlock()
	select {
	case u.requests <- programs:
	default:
		u.mu.Lock()
		u.pending--
		u.mu.Unlock()
		u.setErr(ErrEITPFQueueOverflow)
	}
}

func (u *eitPFUpserter) stop() {
	u.closeOnce.Do(func() {
		close(u.requests)
	})
}

func (u *eitPFUpserter) wait() {
	<-u.done
}

func (u *eitPFUpserter) Err() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.err
}

func (u *eitPFUpserter) setErr(err error) {
	if err == nil {
		return
	}
	u.mu.Lock()
	if u.err == nil {
		u.err = err
	}
	u.mu.Unlock()
}

func (u *eitPFUpserter) run() {
	defer close(u.done)
	for programs := range u.requests {
		if err := u.program.UpsertPrograms(u.ctx, programs); err != nil {
			u.setErr(err)
			slog.Debug("EITPF upsert finished with error", "err", err)
		}
		u.mu.Lock()
		u.pending--
		u.mu.Unlock()
	}
}

var ErrEITPFQueueOverflow = errors.New("eitpf upsert queue overflow")

func shouldStopEITSCollection(snapshot *Snapshot, expected []ServiceKey) bool {
	if snapshot == nil || !snapshot.AllReady(expected) {
		return false
	}
	if !snapshot.ObservedExtendedReady(expected) {
		return false
	}
	var programs []*program.Program
	for _, key := range expected {
		programs = append(programs, snapshot.Programs(key)...)
	}
	fillProgramsFromSharedPeers(programs)
	return lowQualityProgramWarning(programs) == ""
}

func syncStoredServicePrograms(ctx context.Context, programStore ProgramStore, serviceStore ServiceStore, lister StoredProgramLister, expected []ServiceKey, retrievalTime time.Duration) (err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanEPGSyncStoredServicePrograms,
		observability.AttrEPGServices.Int(len(expected)),
		observability.AttrEPGRetrievalTimeMS.Int64(retrievalTime.Milliseconds()),
	)
	defer func() { observability.EndSpan(span, err) }()

	syncCtx, cancel := context.WithTimeout(ctx, retrievalTime)
	defer cancel()

	var result error
	for _, key := range expected {
		if err := syncCtx.Err(); err != nil {
			return errors.Join(result, err)
		}
		programs, err := lister.ListServicePrograms(syncCtx, key.NetworkID, key.ServiceID)
		now := time.Now().UnixMilli()
		if err != nil {
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: list remote programs: %w", key.ServiceID, err))
			continue
		}
		slog.Info("syncing stored remote EPG", "networkId", key.NetworkID, "serviceId", key.ServiceID, "programs", len(programs))
		replaceCtx, replaceSpan := observability.StartSpan(ctx, observability.SpanEPGReplaceRemoteServicePrograms,
			observability.AttrEPGNetworkID.Int(int(key.NetworkID)),
			observability.AttrEPGServiceID.Int(int(key.ServiceID)),
			observability.AttrProgramCount.Int(len(programs)),
		)
		replaceCtx = observability.ContextWithEPGMetricSource(replaceCtx, "remote")
		err = programStore.ReplaceServicePrograms(replaceCtx, key.NetworkID, key.ServiceID, 0, programs)
		observability.EndSpan(replaceSpan, err)
		if err != nil {
			if attemptErr := serviceStore.SetEPGAttempt(ctx, key.NetworkID, key.ServiceID, now, err.Error()); attemptErr != nil {
				observability.RecordEPGServiceUpdateError(ctx, "remote", "attempt")
			}
			result = errors.Join(result, fmt.Errorf("service %d: replace remote programs: %w", key.ServiceID, err))
			continue
		}
		if err := serviceStore.SetEPGSuccess(ctx, key.NetworkID, key.ServiceID, now); err != nil {
			observability.RecordEPGServiceUpdateError(ctx, "remote", "success")
			result = errors.Join(result, err)
		}
	}
	return result
}
