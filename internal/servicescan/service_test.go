package servicescan

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/ts"
)

func TestServiceScanChannelStoresScannedServicesAndReturnsNewNetworks(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "BS 101", Type: 1, RemoteControlKeyId: uint8Ptr(1)},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "BS 102", Type: 1, RemoteControlKeyId: uint8Ptr(2)},
		{Nid: 5, Tsid: 2, Sid: 201, Name: "BS 201", Type: 2, RemoteControlKeyId: uint8Ptr(3)},
	}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", true)
	if err != nil {
		t.Fatal(err)
	}
	assertNIDs(t, got, map[uint16]bool{4: true, 5: true})

	services, err := store.GetByChannel(ctx, "BS", "BS01")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 3; got != want {
		t.Fatalf("stored services = %d, want %d", got, want)
	}
	if got, want := services[0].Id, "0000400101"; got != want {
		t.Fatalf("service id = %q, want %q", got, want)
	}
	if got, want := services[0].RemoteControlKeyId, uint8(1); got != want {
		t.Fatalf("remoteControlKeyId = %d, want %d", got, want)
	}
	if !scanner.wait {
		t.Fatal("scanner wait = false, want true")
	}
}

func TestServiceScanChannelReturnsOnlyNewNetworks(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{services: []ts.ServiceInfo{
		{Nid: 4, Tsid: 1, Sid: 101, Name: "known", Type: 1},
		{Nid: 4, Tsid: 1, Sid: 102, Name: "new same network", Type: 1},
		{Nid: 5, Tsid: 1, Sid: 201, Name: "new network", Type: 1},
		{Nid: 5, Tsid: 1, Sid: 202, Name: "new network duplicate", Type: 1},
	}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	assertNIDs(t, got, map[uint16]bool{4: true, 5: true})
}

func TestServiceScanChannelReturnsNoNetworksWhenAllServicesKnown(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: idFor(4, 101), NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	scanner := &staticScanner{services: []ts.ServiceInfo{{Nid: 4, Tsid: 1, Sid: 101, Name: "known", Type: 1}}}

	got, err := NewService(manager, scanner, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("new networks = %v, want nil", got)
	}
}

func TestServiceScanChannelReturnsScannerError(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	want := errors.New("scan failed")

	_, err = NewService(manager, &staticScanner{err: want}, nil, time.Second).ScanChannel(ctx, "BS", "BS01", false)
	if !errors.Is(err, want) {
		t.Fatalf("ScanChannel error = %v, want %v", err, want)
	}
}

func TestServiceScanChannelTimesOutAndPreservesStoredServices(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := service.NewSQLiteStore(database)
	manager := service.NewServiceManager(store, nil)
	want := &service.Service{
		Id: idFor(4, 101), NetworkId: 4, ServiceId: 101,
		ChannelType: "BS", ChannelId: "BS01", Name: "stored",
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{want}); err != nil {
		t.Fatal(err)
	}

	_, err = NewService(manager, blockingScanner{}, nil, 10*time.Millisecond).ScanChannel(ctx, "BS", "BS01", true)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ScanChannel error = %v, want context deadline exceeded", err)
	}
	got, err := store.GetByChannel(ctx, "BS", "BS01")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Id != want.Id || got[0].Name != want.Name {
		t.Fatalf("stored services after timeout = %#v, want preserved %#v", got, want)
	}
}

func TestServiceChannelsExcludesDisabledChannels(t *testing.T) {
	disabled := true
	channels := NewService(nil, nil, config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "28", IsDisabled: &disabled},
	}, time.Second).Channels()

	if len(channels) != 1 || channels[0] != (Channel{Type: "GR", ID: "27"}) {
		t.Fatalf("channels = %#v, want only GR/27", channels)
	}
}

func TestNewNetworkIDsFromDiffEmptyInputs(t *testing.T) {
	if got := newNetworkIDsFromDiff(nil, nil); got != nil {
		t.Errorf("nil scanned = %v, want nil", got)
	}
	before := map[string]struct{}{idFor(1, 101): {}}
	allKnown := []*service.Service{
		{Id: idFor(1, 101), NetworkId: 1, ServiceId: 101},
	}
	if got := newNetworkIDsFromDiff(before, allKnown); got != nil {
		t.Errorf("all-known scanned = %v, want nil", got)
	}
}

type staticScanner struct {
	err      error
	services []ts.ServiceInfo
	wait     bool
}

type blockingScanner struct{}

func (blockingScanner) ScanServices(ctx context.Context, _, _ string, _ bool) ([]ts.ServiceInfo, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *staticScanner) ScanServices(_ context.Context, _ string, _ string, wait bool) ([]ts.ServiceInfo, error) {
	s.wait = wait
	if s.err != nil {
		return nil, s.err
	}
	return s.services, nil
}

func assertNIDs(t *testing.T, got []uint16, want map[uint16]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("new networks = %v, want %v", got, want)
	}
	for _, nid := range got {
		if !want[nid] {
			t.Errorf("unexpected NID %d in result %v", nid, got)
		}
	}
}

func idFor(nid, sid uint16) string {
	return fmt.Sprintf("%05d%05d", nid, sid)
}

func uint8Ptr(v uint8) *uint8 { return &v }
