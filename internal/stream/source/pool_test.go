package source

import (
	"context"
	"sync"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/runtimecontext"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type fakeTunerUserDevice struct {
	fakeStopErrorDevice
	mu    sync.Mutex
	added []tuner.User
}

func (d *fakeTunerUserDevice) AddUser(user tuner.User) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.added = append(d.added, user)
}

func (d *fakeTunerUserDevice) RemoveUser(string) {}

func (d *fakeTunerUserDevice) lastUser() tuner.User {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.added[len(d.added)-1]
}

func TestTunerLiveSourceWithUserDefaultsFallbackUserToLowestPriority(t *testing.T) {
	done := make(chan struct{})
	close(done)
	device := &fakeTunerUserDevice{fakeStopErrorDevice: fakeStopErrorDevice{done: done}}
	src := &tunerLiveSource{
		channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
		device:  device,
	}

	ctx := runtimecontext.WithJob(context.Background(), runtimecontext.JobInfo{Name: "EPG Gather NID 6"})
	if err := src.WithUser(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	user := device.lastUser()
	if user.Priority != -1 {
		t.Fatalf("fallback user priority = %d, want -1", user.Priority)
	}
	if user.Agent != "EPG Gather NID 6" {
		t.Fatalf("fallback user agent = %q, want %q", user.Agent, "EPG Gather NID 6")
	}
}

func TestTunerLiveSourceWithUserPassesThroughExplicitUser(t *testing.T) {
	done := make(chan struct{})
	close(done)
	device := &fakeTunerUserDevice{fakeStopErrorDevice: fakeStopErrorDevice{done: done}}
	src := &tunerLiveSource{
		channel: &config.ChannelConfig{Type: "GR", Channel: "27"},
		device:  device,
	}

	ctx := tuner.WithUser(context.Background(), tuner.User{ID: "explicit", Priority: 42})
	if err := src.WithUser(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	user := device.lastUser()
	if user.ID != "explicit" || user.Priority != 42 {
		t.Fatalf("user = %+v, want ID=explicit Priority=42", user)
	}
}
