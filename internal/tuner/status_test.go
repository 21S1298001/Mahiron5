package tuner

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
)

func TestTunerStatusTracksChannelsProcessAndUsers(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "test", Types: []string{"CATV"}, Command: "sleep 10"},
	}})
	requested := &config.ChannelConfig{Name: "Logical", Type: "BS", Channel: "101"}
	tuned := &config.ChannelConfig{Name: "Logical", Type: "CATV", Channel: "C13"}
	device, _, err := mgr.AcquireDevice(context.Background(), "CATV", requested, tuned, false)
	if err != nil {
		t.Fatal(err)
	}
	tracked := device.(interface {
		AddUser(User)
		RemoveUser(string)
	})
	tracked.AddUser(User{ID: "viewer", Priority: 1, Agent: "test"})
	if err := device.Start(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}

	status, ok := mgr.Status(0)
	if !ok {
		t.Fatal("status not found")
	}
	if status.CurrentChannelType != "BS" || status.CurrentChannel != "101" {
		t.Fatalf("current channel = %s/%s", status.CurrentChannelType, status.CurrentChannel)
	}
	if status.TunedChannelType != "CATV" || status.TunedChannel != "C13" {
		t.Fatalf("tuned channel = %s/%s", status.TunedChannelType, status.TunedChannel)
	}
	if status.PID <= 0 || status.Command != "sleep 10" {
		t.Fatalf("pid = %d, command = %q", status.PID, status.Command)
	}
	if !status.IsAvailable || !status.IsUsing || status.IsFree || len(status.Users) != 1 {
		t.Fatalf("unexpected active status: %+v", status)
	}

	tracked.RemoveUser("viewer")
	if err := device.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, _ = mgr.Status(0)
	if !status.IsAvailable || !status.IsFree || status.IsUsing || status.PID != 0 || len(status.Users) != 0 {
		t.Fatalf("unexpected released status: %+v", status)
	}
	if status.CurrentChannel != "" || status.TunedChannel != "" {
		t.Fatalf("released channels were not cleared: %+v", status)
	}
}

func TestTunerStatusMarksUnexpectedProcessExitAsFault(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "broken", Types: []string{"GR"}, Command: "command-that-does-not-exist"},
	}})
	channel := &config.ChannelConfig{Type: "GR", Channel: "27"}
	device, _, err := mgr.AcquireDevice(context.Background(), "GR", channel, channel, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := device.Start(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner process did not exit")
	}
	deadline := time.Now().Add(time.Second)
	for {
		status, _ := mgr.Status(0)
		if status.IsFault {
			if status.IsAvailable {
				t.Fatalf("faulted tuner is available: %+v", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tuner was not marked faulted: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
	_ = device.Stop(context.Background())
}

func TestDisabledAndRemoteTunerStatus(t *testing.T) {
	mgr := NewTunerManager(&TunerManagerConfig{TunersConfig: config.TunersConfig{
		{Name: "disabled", Types: []string{"GR"}, Command: "sleep 1", IsDisabled: true},
		{Name: "remote", Types: []string{"BS"}, RemoteMirakurunHost: "localhost", RemoteMirakurunPort: 40772},
	}})
	statuses := mgr.Statuses()
	if statuses[0].IsAvailable || statuses[0].IsFree {
		t.Fatalf("disabled tuner is available: %+v", statuses[0])
	}
	if !statuses[1].IsRemote || statuses[1].IsAvailable {
		t.Fatalf("unexpected remote tuner status: %+v", statuses[1])
	}
	if _, ok := mgr.Status(2); ok {
		t.Fatal("out-of-range tuner status found")
	}
}
