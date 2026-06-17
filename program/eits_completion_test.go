package program

import "testing"

func TestEITSCompletionTrackerWaitsForAllSections(t *testing.T) {
	tracker := NewEITSCompletionTracker()
	if tracker.Complete() {
		t.Fatal("empty tracker should not be complete")
	}

	if tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     0,
		LastSectionNumber: 2,
		VersionNumber:     1,
	}) {
		t.Fatal("tracker should wait for section 1 and 2")
	}
	if tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     2,
		LastSectionNumber: 2,
		VersionNumber:     1,
	}) {
		t.Fatal("tracker should wait for section 1")
	}
	if !tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     1,
		LastSectionNumber: 2,
		VersionNumber:     1,
	}) {
		t.Fatal("tracker should be complete")
	}
}

func TestEITSCompletionTrackerResetsOnVersionChange(t *testing.T) {
	tracker := NewEITSCompletionTracker()
	if tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     0,
		LastSectionNumber: 1,
		VersionNumber:     1,
	}) {
		t.Fatal("tracker should wait for section 1")
	}
	if tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     1,
		LastSectionNumber: 1,
		VersionNumber:     2,
	}) {
		t.Fatal("tracker should reset when version changes")
	}
	if !tracker.Observe(&EITSection{
		OriginalNetworkID: 1,
		TransportStreamID: 2,
		ServiceID:         3,
		TableID:           0x50,
		SectionNumber:     0,
		LastSectionNumber: 1,
		VersionNumber:     2,
	}) {
		t.Fatal("tracker should complete after all sections for new version")
	}
}
