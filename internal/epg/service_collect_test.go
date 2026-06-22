package epg

import (
	"context"
	"errors"
	"testing"
	"time"

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
	calls       [][]*program.Program
	failEventID uint16
	failErr     error
}

func (s *collectProgramStore) UpsertPrograms(_ context.Context, programs []*program.Program) error {
	s.calls = append(s.calls, append([]*program.Program(nil), programs...))
	if len(programs) > 0 && programs[0].EventID == s.failEventID {
		return s.failErr
	}
	return nil
}

func (s *collectProgramStore) DeleteEndedBefore(context.Context, int64) error { return nil }

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
