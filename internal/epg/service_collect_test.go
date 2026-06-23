package epg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/ts"
)

func TestCollectServiceSnapshotsRoutesEITSAndEITPF(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	store := &collectProgramStore{}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, ServiceKey{NetworkID: 4, ServiceID: 102}, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
		testEIT(ts.TableIDEITSStart, ServiceKey{NetworkID: 4, ServiceID: 102}, 20),
	}}

	if err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if session.collectCalls != 1 {
		t.Fatalf("CollectEIT calls = %d, want 1", session.collectCalls)
	}
	if got, want := store.eventIDs(), []uint16{1, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
	if got, want := store.sources, []string{"eitpf", "eits"}; !equalStrings(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}
}

func TestCollectServiceSnapshotsContinuesEITSAfterEITPFFailure(t *testing.T) {
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	pfErr := errors.New("p/f upsert failed")
	store := &collectProgramStore{failEventID: 1, failErr: pfErr}
	session := &collectEITSession{sections: []*ts.EIT{
		testEIT(ts.TableIDEITPF0, key, 1),
		testEIT(ts.TableIDEITPF0, key, 2),
		testEIT(ts.TableIDEITSStart, key, 10),
	}}

	if err := CollectServiceSnapshots(context.Background(), store, newRemoteSyncServiceStore(), session, []ServiceKey{key}, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := store.eventIDs(), []uint16{1, 10}; !equalEventIDs(got, want) {
		t.Fatalf("upserted event IDs = %v, want %v", got, want)
	}
}

func TestGatherNetworkTimesOutWhileWaitingForSession(t *testing.T) {
	streams := blockingEPGStreams{}
	key := ServiceKey{NetworkID: 4, ServiceID: 101}
	started := time.Now()
	err := gatherNetwork(
		context.Background(),
		&collectProgramStore{},
		newRemoteSyncServiceStore(),
		streams,
		key.NetworkID,
		[]Candidate{{Type: "GR", Channel: "27"}},
		[]ServiceKey{key},
		20*time.Millisecond,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gatherNetwork error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("gatherNetwork took %s, want session wait bounded by retrieval time", elapsed)
	}
}

func TestServiceCleanupUsesCleanupMetricSource(t *testing.T) {
	store := &collectProgramStore{}
	service := NewService(store, newRemoteSyncServiceStore(), nil, nil, 1, time.Second)

	if err := service.Cleanup(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if store.deleteSource != "cleanup" {
		t.Fatalf("delete source = %q, want cleanup", store.deleteSource)
	}
}

type blockingEPGStreams struct{}

func (blockingEPGStreams) HasSession(string, string) bool { return false }

func (blockingEPGStreams) GetOrCreateWait(ctx context.Context, _, _ string) (interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type collectEITSession struct {
	sections     []*ts.EIT
	collectCalls int
}

func (s *collectEITSession) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	s.collectCalls++
	for _, section := range s.sections {
		if err := observe(section); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type collectProgramStore struct {
	calls        [][]*program.Program
	failEventID  uint16
	failErr      error
	sources      []string
	deleteSource string
}

func (s *collectProgramStore) UpsertPrograms(ctx context.Context, programs []*program.Program) error {
	s.calls = append(s.calls, append([]*program.Program(nil), programs...))
	s.sources = append(s.sources, observability.EPGMetricSource(ctx))
	if len(programs) > 0 && programs[0].EventID == s.failEventID {
		return s.failErr
	}
	return nil
}

func (s *collectProgramStore) DeleteEndedBefore(ctx context.Context, _ int64) error {
	s.deleteSource = observability.EPGMetricSource(ctx)
	return nil
}

func (s *collectProgramStore) ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error {
	return nil
}

func (s *collectProgramStore) eventIDs() []uint16 {
	var ids []uint16
	for _, call := range s.calls {
		for _, item := range call {
			ids = append(ids, item.EventID)
		}
	}
	return ids
}

func testEIT(tableID byte, key ServiceKey, eventID uint16) *ts.EIT {
	return &ts.EIT{
		OriginalNetworkID:        key.NetworkID,
		ServiceID:                key.ServiceID,
		TableID:                  tableID,
		SectionNumber:            0,
		LastSectionNumber:        0,
		SegmentLastSectionNumber: 0,
		Events: []ts.EITEvent{{
			EventID:   eventID,
			StartTime: time.Unix(int64(eventID), 0),
			Duration:  time.Minute,
		}},
	}
}

func equalEventIDs(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
