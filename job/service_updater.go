package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
)

const (
	ServiceUpdaterKey             = "service-updater"
	ServiceUpdaterName            = "Service Updater"
	ServiceUpdaterDefaultSchedule = "5 6 * * *"
)

func RegisterServiceUpdater(mgr *JobManager, sm *service.ServiceManager, stm *stream.StreamManager, channels config.ChannelsConfig) {
	mgr.Register(JobDefinition{
		Key: ServiceUpdaterKey, Name: ServiceUpdaterName, IsRerunnable: true,
		Handler: func(ctx context.Context) error {
			queued := 0
			for _, configured := range channels {
				if err := ctx.Err(); err != nil {
					return err
				}
				if configured.IsDisabled != nil && *configured.IsDisabled {
					continue
				}
				channel := configured
				definition := JobDefinition{
					Key:          fmt.Sprintf("service-scan:%s:%s", channel.Type, channel.Channel),
					Name:         fmt.Sprintf("Service Scan %s/%s", channel.Type, channel.Channel),
					IsRerunnable: true,
					Handler: func(childCtx context.Context) error {
						return sm.ScanServicesWait(childCtx, stm, channel.Type, channel.Channel)
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
			slog.Info("service updater dispatched", "queued", queued)
			return nil
		},
	})
}
