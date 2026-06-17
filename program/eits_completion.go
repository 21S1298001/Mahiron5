package program

type EITSCompletionTracker struct {
	tables map[eitsTableKey]*eitsTableState
}

type eitsTableKey struct {
	originalNetworkID uint16
	transportStreamID uint16
	serviceID         uint16
	tableID           uint8
}

type eitsTableState struct {
	versionNumber     uint8
	lastSectionNumber uint8
	sections          map[uint8]struct{}
}

func NewEITSCompletionTracker() *EITSCompletionTracker {
	return &EITSCompletionTracker{
		tables: map[eitsTableKey]*eitsTableState{},
	}
}

func (t *EITSCompletionTracker) Observe(section *EITSection) bool {
	if section == nil {
		return t.Complete()
	}

	key := eitsTableKey{
		originalNetworkID: section.OriginalNetworkID,
		transportStreamID: section.TransportStreamID,
		serviceID:         section.ServiceID,
		tableID:           section.TableID,
	}
	state := t.tables[key]
	if state == nil {
		state = &eitsTableState{
			versionNumber:     section.VersionNumber,
			lastSectionNumber: section.LastSectionNumber,
			sections:          map[uint8]struct{}{},
		}
		t.tables[key] = state
	} else if state.versionNumber != section.VersionNumber {
		state.versionNumber = section.VersionNumber
		state.sections = map[uint8]struct{}{}
	}
	state.lastSectionNumber = section.LastSectionNumber
	state.sections[section.SectionNumber] = struct{}{}

	return t.Complete()
}

func (t *EITSCompletionTracker) Complete() bool {
	if len(t.tables) == 0 {
		return false
	}
	for _, table := range t.tables {
		for sectionNumber := uint8(0); sectionNumber <= table.lastSectionNumber; sectionNumber++ {
			if _, ok := table.sections[sectionNumber]; !ok {
				return false
			}
			if sectionNumber == 255 {
				break
			}
		}
	}
	return true
}

func (t *EITSCompletionTracker) Progress(section *EITSection) (collected int, total int, complete bool) {
	if section == nil {
		return 0, 0, t.Complete()
	}
	key := eitsTableKey{
		originalNetworkID: section.OriginalNetworkID,
		transportStreamID: section.TransportStreamID,
		serviceID:         section.ServiceID,
		tableID:           section.TableID,
	}
	state := t.tables[key]
	if state == nil {
		return 0, 0, false
	}
	return len(state.sections), int(state.lastSectionNumber) + 1, t.Complete()
}

func (t *EITSCompletionTracker) TableCount() int {
	return len(t.tables)
}
