package tuner

import (
	"context"
	"log/slog"
	"slices"

	"github.com/21S1298001/Mahiron5/config"
	"golang.org/x/sync/errgroup"
)

type TunerManager struct {
	tuners []*Tuner
}

type TunerManagerConfig struct {
	TunersConfig config.TunersConfig
}

func NewTunerManager(config *TunerManagerConfig) *TunerManager {
	tuners := make([]*Tuner, len(config.TunersConfig))
	for i, tunerConfig := range config.TunersConfig {
		tuners[i] = NewTuner(tunerConfig)
	}

	return &TunerManager{
		tuners: tuners,
	}
}

func (tm *TunerManager) Shutdown(ctx context.Context) error {
	var eg errgroup.Group
	for _, tuner := range tm.tuners {
		eg.Go(func() error {
			if err := tuner.Shutdown(ctx); err != nil {
				slog.Error("failed to shutdown tuner", "name", tuner.Name(), "err", err)
				return err
			}
			return nil
		})
	}
	return eg.Wait()
}

func (tm *TunerManager) GetTuner(name string) *Tuner {
	for _, tuner := range tm.tuners {
		if tuner.Name() == name {
			return tuner
		}
	}
	return nil
}

func (tm *TunerManager) GetTunerByGroup(group string) *Tuner {
	for _, tuner := range tm.tuners {
		if slices.Contains(tuner.Groups(), group) {
			return tuner
		}
	}
	return nil
}

func (tm *TunerManager) CountTunersByGroup() map[string]int {
	counts := make(map[string]int)
	for _, tuner := range tm.tuners {
		for _, g := range tuner.Groups() {
			counts[g]++
		}
	}
	return counts
}
