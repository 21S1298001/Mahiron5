package event

import "testing"

func TestHubDropsEventsForFullSubscriber(t *testing.T) {
	hub := NewWithCapacity(200)
	events, unsubscribe := hub.Subscribe()
	defer unsubscribe()

	for i := range 129 {
		hub.PublishEvent(ResourceProgram, TypeUpdate, map[string]any{"id": i})
	}

	count := 0
	for {
		select {
		case <-events:
			count++
		default:
			if count != 128 {
				t.Fatalf("buffered events = %d, want 128", count)
			}
			return
		}
	}
}
