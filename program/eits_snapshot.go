package program

import (
	"log/slog"
	"sort"
	"time"
)

type ServiceKey struct {
	NetworkID uint16
	ServiceID uint16
}

type EITSnapshot struct {
	services     map[ServiceKey]*snapshotService
	lastProgress time.Time
}

type snapshotService struct {
	tables   map[uint8]*snapshotTable
	programs map[int64]*Program
}

type snapshotTable struct {
	version     uint8
	hasVersion  bool
	lastSection uint8
	segmentLast map[uint8]uint8
	// sections records which section numbers we have already observed at the
	// current version. Used both to drive snapshotTableComplete and to purge
	// stale programs on version / shrink events.
	sections map[uint8]struct{}
	// sectionPrograms records the set of program IDs contributed by each
	// section. When a section is purged (version change, lastSection shrink)
	// its programs are removed from the service-level map.
	sectionPrograms map[uint8][]int64
}

// EITSnapshotReport describes why a service snapshot is not complete.  It is
// intentionally safe to pass directly to slog as a structured value.
type EITSnapshotReport struct {
	ObservedTables  int                      `json:"observedTables"`
	MissingTableIDs []int                    `json:"missingTableIds,omitempty"`
	Tables          []EITSnapshotTableReport `json:"tables,omitempty"`
}

type EITSnapshotTableReport struct {
	TableID            int   `json:"tableId"`
	Version            int   `json:"version"`
	LastSection        int   `json:"lastSection"`
	ObservedSections   int   `json:"observedSections"`
	MissingSections    []int `json:"missingSections,omitempty"`
	MissingSegmentInfo []int `json:"missingSegmentInfo,omitempty"`
	Complete           bool  `json:"complete"`
}

func NewEITSnapshot() *EITSnapshot {
	return &EITSnapshot{services: make(map[ServiceKey]*snapshotService)}
}

func (s *EITSnapshot) Observe(section *EITSection, now time.Time) bool {
	if section == nil {
		return false
	}
	if section.TableID < 0x50 || section.TableID > 0x6f {
		slog.Warn("ignoring EITS section for unsupported table",
			"networkId", section.OriginalNetworkID,
			"serviceId", section.ServiceID,
			"tableId", section.TableID)
		return false
	}
	key := ServiceKey{NetworkID: section.OriginalNetworkID, ServiceID: section.ServiceID}
	service := s.services[key]
	if service == nil {
		service = &snapshotService{
			tables:   make(map[uint8]*snapshotTable),
			programs: make(map[int64]*Program),
		}
		s.services[key] = service
	}
	table := service.tables[section.TableID]
	if table == nil {
		table = &snapshotTable{
			segmentLast:     make(map[uint8]uint8),
			sections:        make(map[uint8]struct{}),
			sectionPrograms: make(map[uint8][]int64),
		}
		service.tables[section.TableID] = table
	}

	// Detect a sub-table version roll. ARIB version_number is per-sub-table
	// (per tableID), so we must drop every program and section that came from
	// the previous version before absorbing the new section.
	versionChanged := !table.hasVersion || table.version != section.VersionNumber
	// A shrink in lastSectionNumber also invalidates any program carried by
	// sections above the new last section.
	shrunk := section.LastSectionNumber < table.lastSection
	if versionChanged || shrunk {
		s.purgeTable(service, table)
	}

	// Observe this section.
	changed := false
	if _, ok := table.sections[section.SectionNumber]; !ok {
		changed = true
	}
	table.sections[section.SectionNumber] = struct{}{}
	progs := section.Programs()
	ids := make([]int64, 0, len(progs))
	for _, item := range progs {
		service.programs[item.ID] = item
		ids = append(ids, item.ID)
	}
	table.sectionPrograms[section.SectionNumber] = ids

	table.version = section.VersionNumber
	table.hasVersion = true
	table.lastSection = section.LastSectionNumber
	table.segmentLast[section.SectionNumber/8] = section.SegmentLastSectionNumber

	if changed {
		s.lastProgress = now
	}
	return changed
}

func (s *EITSnapshot) purgeTable(service *snapshotService, table *snapshotTable) {
	for section, ids := range table.sectionPrograms {
		for _, id := range ids {
			delete(service.programs, id)
		}
		delete(table.sections, section)
	}
	table.sectionPrograms = make(map[uint8][]int64)
	// Reset segmentLast so completion logic re-evaluates against the new
	// lastSectionNumber.
	table.segmentLast = make(map[uint8]uint8)
}

func (s *EITSnapshot) ServiceComplete(key ServiceKey) bool {
	service := s.services[key]
	if service == nil || len(service.tables) == 0 {
		return false
	}
	groups := make(map[uint8]map[uint8]*snapshotTable)
	for tableID, table := range service.tables {
		if tableID < 0x50 || tableID > 0x6f {
			return false
		}
		base := tableID & 0xf8
		if groups[base] == nil {
			groups[base] = make(map[uint8]*snapshotTable)
		}
		groups[base][tableID] = table
	}
	if len(groups) == 0 {
		return false
	}
	for base, tables := range groups {
		maxTable := base
		for tableID := range tables {
			if tableID > maxTable {
				maxTable = tableID
			}
		}
		for tableID := base; tableID <= maxTable; tableID++ {
			table := tables[tableID]
			if table == nil || !snapshotTableComplete(table) {
				return false
			}
		}
	}
	return true
}

// CompletionReport returns the table and section gaps used by
// ServiceComplete.  MissingSegmentInfo contains segment indexes for which no
// segment_last_section_number has been observed yet.
func (s *EITSnapshot) CompletionReport(key ServiceKey) EITSnapshotReport {
	service := s.services[key]
	if service == nil {
		return EITSnapshotReport{}
	}

	tableIDs := make([]int, 0, len(service.tables))
	for tableID := range service.tables {
		tableIDs = append(tableIDs, int(tableID))
	}
	sort.Ints(tableIDs)

	report := EITSnapshotReport{ObservedTables: len(tableIDs)}
	groups := make(map[uint8]map[uint8]struct{})
	for _, id := range tableIDs {
		tableID := uint8(id)
		table := service.tables[tableID]
		base := tableID & 0xf8
		if groups[base] == nil {
			groups[base] = make(map[uint8]struct{})
		}
		groups[base][tableID] = struct{}{}

		tableReport := EITSnapshotTableReport{
			TableID:          id,
			Version:          int(table.version),
			LastSection:      int(table.lastSection),
			ObservedSections: len(table.sections),
			Complete:         snapshotTableComplete(table),
		}
		lastSegment := table.lastSection / 8
		for segment := uint8(0); segment <= lastSegment; segment++ {
			segmentLast, ok := table.segmentLast[segment]
			if !ok {
				tableReport.MissingSegmentInfo = append(tableReport.MissingSegmentInfo, int(segment))
			} else {
				first := segment * 8
				last := segmentLast
				if last > table.lastSection {
					last = table.lastSection
				}
				for section := first; section <= last && last >= first; section++ {
					if _, ok := table.sections[section]; !ok {
						tableReport.MissingSections = append(tableReport.MissingSections, int(section))
					}
					if section == 255 {
						break
					}
				}
			}
			if segment == 31 {
				break
			}
		}
		report.Tables = append(report.Tables, tableReport)
	}

	for base, tables := range groups {
		maxTable := base
		for tableID := range tables {
			if tableID > maxTable {
				maxTable = tableID
			}
		}
		for tableID := base; tableID <= maxTable; tableID++ {
			if _, ok := tables[tableID]; !ok {
				report.MissingTableIDs = append(report.MissingTableIDs, int(tableID))
			}
		}
	}
	sort.Ints(report.MissingTableIDs)
	return report
}

func snapshotTableComplete(table *snapshotTable) bool {
	lastSegment := table.lastSection / 8
	for segment := uint8(0); segment <= lastSegment; segment++ {
		segmentLast, ok := table.segmentLast[segment]
		if !ok {
			return false
		}
		first := segment * 8
		last := segmentLast
		if last > table.lastSection {
			last = table.lastSection
		}
		if last < first {
			continue
		}
		for section := first; section <= last; section++ {
			if _, ok := table.sections[section]; !ok {
				return false
			}
			if section == 255 {
				break
			}
		}
		if segment == 31 {
			break
		}
	}
	return true
}

func (s *EITSnapshot) AllComplete(expected []ServiceKey) bool {
	if len(expected) == 0 {
		return false
	}
	for _, key := range expected {
		if !s.ServiceComplete(key) {
			return false
		}
	}
	return true
}

func (s *EITSnapshot) StableFor(now time.Time, duration time.Duration) bool {
	return !s.lastProgress.IsZero() && now.Sub(s.lastProgress) >= duration
}

func (s *EITSnapshot) Programs(key ServiceKey) []*Program {
	service := s.services[key]
	if service == nil {
		return nil
	}
	result := make([]*Program, 0, len(service.programs))
	for _, item := range service.programs {
		result = append(result, item)
	}
	return result
}
