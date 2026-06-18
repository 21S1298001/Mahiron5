package api

import (
	"context"
	"strconv"
	"strings"
	"time"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

const epgGatherJobKeyPrefix = "epg-gather:nid:"

func GetStatus(ctx context.Context, h *Handler) (apigen.GetStatusRes, error) {
	now := time.Now()
	result := &apigen.Status{
		Time: apigen.NewOptInt(int(now.UnixMilli())),
	}

	epg, err := buildStatusEpg(ctx, h, now)
	if err != nil {
		return nil, err
	}
	result.Epg = apigen.NewOptStatusEpg(*epg)

	return result, nil
}

func buildStatusEpg(ctx context.Context, h *Handler, now time.Time) (*apigen.StatusEpg, error) {
	epg := &apigen.StatusEpg{}

	for _, key := range h.jobManager.GetActiveJobKeysByPrefix(epgGatherJobKeyPrefix) {
		nidStr := strings.TrimPrefix(key, epgGatherJobKeyPrefix)
		nid, err := strconv.ParseUint(nidStr, 10, 16)
		if err != nil {
			continue
		}
		epg.GatheringNetworks = append(epg.GatheringNetworks, apigen.NetworkId(nid))
	}

	if count, err := h.programManager.Count(ctx); err == nil {
		epg.StoredEvents = apigen.NewOptInt(count)
	}

	if h.serviceManager != nil {
		stale, failed, lastSuccess, err := h.serviceManager.EPGSummary(ctx, h.epgStaleAfter, now.UnixMilli())
		if err == nil {
			epg.StaleServices = apigen.NewOptInt(stale)
			epg.FailedServices = apigen.NewOptInt(failed)
			if lastSuccess != nil {
				epg.LastUpdatedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*lastSuccess))
			}
		}
	}

	return epg, nil
}
