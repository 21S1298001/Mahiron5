package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/ts"
)

const (
	LogoGathererKey             = "logo-gatherer"
	LogoGathererName            = "Logo Gatherer"
	LogoGathererDefaultSchedule = "5 3 * * *"
)

var errLogoTargetsComplete = errors.New("logo targets complete")

func RegisterLogoGatherer(registry Registry, collector LogoCollector, store LogoStore, timeout time.Duration) {
	if timeout <= 0 {
		timeout = 20 * time.Minute
	}
	registry.Register(JobDefinition{
		Key: LogoGathererKey, Name: LogoGathererName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			targets, err := store.MissingLogoTargets(ctx)
			if err != nil {
				return err
			}
			grouped := make(map[string][]service.LogoTarget)
			for _, target := range targets {
				key := target.ChannelType + "\x00" + target.ChannelId
				grouped[key] = append(grouped[key], target)
			}
			queued := 0
			for _, channelTargets := range grouped {
				if err := ctx.Err(); err != nil {
					return err
				}
				channelTargets := append([]service.LogoTarget(nil), channelTargets...)
				channelType, channelID := channelTargets[0].ChannelType, channelTargets[0].ChannelId
				definition := JobDefinition{
					Key:          fmt.Sprintf("logo-gather:%s:%s", channelType, channelID),
					Name:         fmt.Sprintf("Logo Gather %s/%s", channelType, channelID),
					IsRerunnable: true,
					Handler: func(childCtx context.Context) error {
						gatherCtx, cancel := context.WithTimeout(childCtx, timeout)
						defer cancel()
						remaining := make(map[logoTargetKey]struct{}, len(channelTargets))
						for _, target := range channelTargets {
							remaining[newLogoTargetKey(target)] = struct{}{}
						}
						count := 0
						err := collector.ObserveLogos(gatherCtx, channelType, channelID, func(image *ts.LogoImage) error {
							if image.IsDeleted {
								return nil
							}
							count++
							delete(remaining, logoTargetKey{int64(image.OriginalNetworkID), int64(image.LogoID), int64(image.LogoVersion), int64(image.DownloadDataID)})
							if len(remaining) == 0 {
								return errLogoTargetsComplete
							}
							return nil
						})
						if errors.Is(err, errLogoTargetsComplete) || errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && childCtx.Err() == nil) {
							err = nil
						}
						if err != nil {
							return err
						}
						slog.Info("logo gather completed", "channel", fmt.Sprintf("%s/%s", channelType, channelID), "logos", count, "remaining", len(remaining), "timeout", timeout)
						return nil
					},
				}
				if _, err := registry.EnqueueDefinition(definition); err != nil {
					if errors.Is(err, ErrJobAlreadyRunning) {
						continue
					}
					return err
				}
				queued++
			}
			slog.Info("logo gatherer dispatched", "queued", queued)
			return nil
		},
	})
}

type logoTargetKey struct {
	networkID, logoID, logoVersion, downloadDataID int64
}

func newLogoTargetKey(target service.LogoTarget) logoTargetKey {
	return logoTargetKey{int64(target.NetworkId), target.LogoId, target.LogoVersion, target.LogoDownloadDataId}
}
