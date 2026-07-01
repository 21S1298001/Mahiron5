package epg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
)

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
