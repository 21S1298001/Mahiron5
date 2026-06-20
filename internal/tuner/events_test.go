package tuner

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/eventhub"
)

func TestTunerManagerPublishesCreateAndUpdateEvents(t *testing.T) {
	hub := eventhub.New()
	mgr := NewTunerManager(&TunerManagerConfig{
		TunersConfig: config.TunersConfig{{Name: "first", Types: []string{"GR"}, Command: "true"}},
		EventHub:     hub,
	})

	mgr.SeedEventLog()
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	device.(interface{ AddUser(User) }).AddUser(User{ID: "viewer", Priority: 1})
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	events := hub.Log()
	if len(events) < 4 {
		t.Fatalf("events length = %d, want at least 4: %#v", len(events), events)
	}
	if events[0].Resource != eventhub.ResourceTuner || events[0].Type != eventhub.TypeCreate {
		t.Fatalf("first event = %s/%s, want tuner/create", events[0].Resource, events[0].Type)
	}
	for i, event := range events[1:] {
		if event.Resource != eventhub.ResourceTuner || event.Type != eventhub.TypeUpdate {
			t.Fatalf("event %d = %s/%s, want tuner/update", i+1, event.Resource, event.Type)
		}
	}
}
