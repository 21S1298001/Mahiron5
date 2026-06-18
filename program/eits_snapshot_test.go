package program

import (
	"testing"
	"time"
)

func makeSection(nid, sid, tsid uint16, tableID, section, lastSection, version uint8, events ...EITEvent) *EITSection {
	return &EITSection{
		OriginalNetworkID:        nid,
		TransportStreamID:        tsid,
		ServiceID:                sid,
		TableID:                  tableID,
		SectionNumber:            section,
		LastSectionNumber:        lastSection,
		SegmentLastSectionNumber: lastSection,
		VersionNumber:            version,
		Events:                   events,
	}
}

func ev(id uint16, start, dur int) EITEvent {
	return EITEvent{EventID: id, StartTime: int64(start), Duration: dur, Scrambled: false}
}

func TestEITSnapshotObserveBuildsPrograms(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(2, 2000, 1000)), now)
	progs := snap.Programs(ServiceKey{1, 100})
	if got, want := len(progs), 2; got != want {
		t.Fatalf("programs = %d, want %d", got, want)
	}
}

func TestEITSSnapshotServiceCompleteHappyPath(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(2, 2000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x51, 0, 0, 1, ev(3, 2000, 1000)), now)
	if !snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("ServiceComplete should be true for two complete sub-tables")
	}
}

func TestEITSnapshotServiceCompleteFalseOnMissingSegment(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	if snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("ServiceComplete should be false when section 1 is missing")
	}
}

func TestEITSnapshotServiceCompleteFalseOnUnknownTable(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x40, 0, 0, 1, ev(1, 1000, 1000)), now)
	if snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("ServiceComplete should be false when only unknown tables observed")
	}
	if got := len(snap.Programs(ServiceKey{1, 100})); got != 0 {
		t.Fatalf("unknown table programs = %d, want 0", got)
	}
}

func TestEITSnapshotVersionChangeReplacesPrograms(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 0, 1, ev(1, 1000, 1000)), now)
	progs := snap.Programs(ServiceKey{1, 100})
	if len(progs) != 1 {
		t.Fatalf("first version programs = %d, want 1", len(progs))
	}
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 0, 2, ev(99, 9999, 1000)), now)
	progs = snap.Programs(ServiceKey{1, 100})
	if len(progs) != 1 {
		t.Fatalf("after version change programs = %d, want 1", len(progs))
	}
	if progs[0].ID != ProgramID(1, 100, 99) {
		t.Fatalf("after version change program id = %d, want %d", progs[0].ID, ProgramID(1, 100, 99))
	}
}

func TestEITSSnapshotDuplicateSectionIsIdempotent(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(2, 2000, 1000)), now)
	if !snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("ServiceComplete should be true after duplicates")
	}
	if got, want := len(snap.Programs(ServiceKey{1, 100})), 2; got != want {
		t.Fatalf("programs = %d, want %d", got, want)
	}
}

func TestEITSnapshotAllComplete(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 0, 1, ev(1, 1000, 1000)), now)
	expected := []ServiceKey{{1, 100}, {1, 101}}
	if snap.AllComplete(expected) {
		t.Fatal("AllComplete should be false when one service is unobserved")
	}
	snap.Observe(makeSection(1, 101, 2, 0x50, 0, 0, 1, ev(2, 1000, 1000)), now)
	if !snap.AllComplete(expected) {
		t.Fatal("AllComplete should be true when both services complete 0x50")
	}
}

func TestEITSSnapshotStableFor(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	if snap.StableFor(now, time.Second) {
		t.Fatal("StableFor should be false before any progress")
	}
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 0, 1, ev(1, 1000, 1000)), now)
	if snap.StableFor(now, time.Second) {
		t.Fatal("StableFor should be false at the moment of progress")
	}
	if !snap.StableFor(now.Add(time.Second+time.Millisecond), time.Second) {
		t.Fatal("StableFor should be true after duration elapsed")
	}
}

// TestEITSnapshotVersionChangeWithShrinkingLastSectionPurgesOldPrograms is a
// regression test for the per-sub-table version model. ARIB may roll the
// version_number of a sub-table while also shrinking the lastSectionNumber;
// programs that lived in the dropped sections must not survive into the new
// snapshot.
func TestEITSnapshotVersionChangeWithShrinkingLastSectionPurgesOldPrograms(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(10, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(11, 2000, 1000)), now)
	if !snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("setup: ServiceComplete should be true after observing all sections")
	}
	progs := snap.Programs(ServiceKey{1, 100})
	if len(progs) != 2 {
		t.Fatalf("setup: programs = %d, want 2", len(progs))
	}
	// Roll the version while shrinking lastSectionNumber from 1 to 0.
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 0, 2, ev(20, 5000, 1000)), now)
	progs = snap.Programs(ServiceKey{1, 100})
	if len(progs) != 1 {
		t.Fatalf("after shrink programs = %d, want 1 (stale section 1 should be purged)", len(progs))
	}
	if progs[0].EventID != 20 {
		t.Errorf("surviving program eventId = %d, want 20", progs[0].EventID)
	}
	if !snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("after shrink ServiceComplete should be true: new lastSection=0, section 0 observed")
	}
}
