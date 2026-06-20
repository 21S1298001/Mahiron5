package tuner

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
)

func TestTunerDeviceCopiesCommandStdout(t *testing.T) {
	device := NewCommandDevice(nil, "printf command-ts")
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "command-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

func TestTunerDeviceCopiesDVBDevicePath(t *testing.T) {
	path := t.TempDir() + "/dvr0"
	if err := os.WriteFile(path, []byte("dvb-ts"), 0o600); err != nil {
		t.Fatal(err)
	}
	device := NewDVBDevice(nil, "printf command-ts", path)
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "dvb-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

func TestTunerDeviceStopTerminatesDVBCommand(t *testing.T) {
	device := NewDVBDevice(nil, "sleep 10", "/dev/null")
	if err := device.Start(context.Background(), bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := device.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not stop")
	}
}

func TestReplaceCommandTemplateMirakurunCompatibilityAliases(t *testing.T) {
	channel := &config.ChannelConfig{
		Type:        "BS",
		Channel:     "101",
		CommandVars: map[string]any{"satellite": "JCSAT3A", "space": uint8(1)},
	}

	got := replaceCommandTemplate("tuner <satellite> <satelite> <space> <missing>", channel)
	if want := "tuner JCSAT3A JCSAT3A 1 "; got != want {
		t.Fatalf("replaceCommandTemplate() = %q, want %q", got, want)
	}

	got = replaceCommandTemplate("tuner <space> <satelite>", &config.ChannelConfig{CommandVars: map[string]any{}})
	if want := "tuner 0 "; got != want {
		t.Fatalf("replaceCommandTemplate() default = %q, want %q", got, want)
	}
}
