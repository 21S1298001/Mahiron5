package epg

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

func TestEITSnapshotServiceCompleteAllowsElapsedLeadingSegments(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	for segment := uint8(7); segment < 32; segment++ {
		section := segment * 8
		item := makeSection(1, 100, 2, 0x50, section, 248, 1)
		item.SegmentLastSectionNumber = section
		snap.Observe(item, now)
	}

	key := ServiceKey{1, 100}
	if !snap.ServiceComplete(key) {
		t.Fatalf("ServiceComplete should allow elapsed leading segments: %+v", snap.CompletionReport(key))
	}
	report := snap.CompletionReport(key)
	if got := report.Tables[0].MissingSegmentInfo; len(got) != 0 {
		t.Fatalf("MissingSegmentInfo = %v, want none", got)
	}
}

func TestEITSnapshotServiceCompleteRejectsMissingMiddleSegment(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	for segment := uint8(7); segment < 32; segment++ {
		if segment == 12 {
			continue
		}
		section := segment * 8
		item := makeSection(1, 100, 2, 0x50, section, 248, 1)
		item.SegmentLastSectionNumber = section
		snap.Observe(item, now)
	}

	key := ServiceKey{1, 100}
	if snap.ServiceComplete(key) {
		t.Fatal("ServiceComplete should reject a missing middle segment")
	}
	report := snap.CompletionReport(key)
	if got := report.Tables[0].MissingSegmentInfo; len(got) != 1 || got[0] != 12 {
		t.Fatalf("MissingSegmentInfo = %v, want [12]", got)
	}
}

func TestEITSnapshotCompletionReport(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	section := makeSection(1, 100, 2, 0x51, 0, 9, 3, ev(1, 1000, 1000))
	section.SegmentLastSectionNumber = 1
	snap.Observe(section, now)

	report := snap.CompletionReport(ServiceKey{1, 100})
	if report.ObservedTables != 1 {
		t.Fatalf("ObservedTables = %d, want 1", report.ObservedTables)
	}
	if len(report.MissingTableIDs) != 1 || report.MissingTableIDs[0] != 0x50 {
		t.Fatalf("MissingTableIDs = %v, want [80]", report.MissingTableIDs)
	}
	if len(report.Tables) != 1 {
		t.Fatalf("Tables = %d, want 1", len(report.Tables))
	}
	table := report.Tables[0]
	if table.TableID != 0x51 || table.Version != 3 || table.LastSection != 9 || table.ObservedSections != 1 {
		t.Fatalf("table report = %+v", table)
	}
	if len(table.MissingSections) != 1 || table.MissingSections[0] != 1 {
		t.Errorf("MissingSections = %v, want [1]", table.MissingSections)
	}
	if len(table.MissingSegmentInfo) != 1 || table.MissingSegmentInfo[0] != 1 {
		t.Errorf("MissingSegmentInfo = %v, want [1]", table.MissingSegmentInfo)
	}
}

func TestEITSnapshotCompletionReportForUnobservedService(t *testing.T) {
	report := NewEITSnapshot().CompletionReport(ServiceKey{1, 100})
	if report.ObservedTables != 0 || len(report.Tables) != 0 {
		t.Fatalf("report = %+v, want empty", report)
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

func TestEITSnapshotVersionChangeReplacesOnlyItsSection(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(2, 2000, 1000)), now)
	progs := snap.Programs(ServiceKey{1, 100})
	if len(progs) != 2 {
		t.Fatalf("first version programs = %d, want 2", len(progs))
	}
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 2, ev(99, 9999, 1000)), now)
	progs = snap.Programs(ServiceKey{1, 100})
	if len(progs) != 2 {
		t.Fatalf("after version change programs = %d, want 2", len(progs))
	}
	got := map[uint16]bool{}
	for _, p := range progs {
		got[p.EventID] = true
	}
	if got[1] || !got[2] || !got[99] {
		t.Fatalf("after version change event IDs = %v, want 2 and 99", got)
	}
}

func TestEITSnapshotEmptySectionReplacementRemovesOnlyThatSection(t *testing.T) {
	snap := NewEITSnapshot()
	now := time.Unix(0, 0)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 1, ev(1, 1000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 1, 1, 1, ev(2, 2000, 1000)), now)
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 2), now)

	programs := snap.Programs(ServiceKey{1, 100})
	if len(programs) != 1 || programs[0].EventID != 2 {
		t.Fatalf("programs after empty replacement = %#v, want only event 2", programs)
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

func TestEITSnapshotMixedVersionsDoNotResetTable(t *testing.T) {
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
	// A version roll can arrive one section at a time. Updating section 0 must
	// retain section 1 until its replacement arrives.
	snap.Observe(makeSection(1, 100, 2, 0x50, 0, 1, 2, ev(20, 5000, 1000)), now)
	progs = snap.Programs(ServiceKey{1, 100})
	if len(progs) != 2 {
		t.Fatalf("during version roll programs = %d, want 2", len(progs))
	}
	if !snap.ServiceComplete(ServiceKey{1, 100}) {
		t.Fatal("mixed-version table should retain its section coverage")
	}
}
