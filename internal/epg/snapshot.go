package epg

import (
	"log/slog"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
)

type ServiceKey struct {
	NetworkID uint16
	ServiceID uint16
}

type Snapshot struct {
	services     map[ServiceKey]*snapshotService
	lastProgress time.Time
}

type EITSnapshot = Snapshot

type snapshotService struct {
	tables   map[uint8]*snapshotTable
	programs map[int64]*program.Program
}

type snapshotTable struct {
	version         uint8
	hasVersion      bool
	lastSection     uint8
	segmentLast     map[uint8]uint8
	sections        map[uint8]struct{}
	sectionPrograms map[uint8][]*program.Program
	sectionVersions map[uint8]uint8
}

type SnapshotReport struct {
	ObservedTables  int                   `json:"observedTables"`
	MissingTableIDs []int                 `json:"missingTableIds,omitempty"`
	Tables          []SnapshotTableReport `json:"tables,omitempty"`
}

type SnapshotTableReport struct {
	TableID            int   `json:"tableId"`
	Version            int   `json:"version"`
	LastSection        int   `json:"lastSection"`
	ObservedSections   int   `json:"observedSections"`
	MissingSections    []int `json:"missingSections,omitempty"`
	MissingSegmentInfo []int `json:"missingSegmentInfo,omitempty"`
	Complete           bool  `json:"complete"`
}

func NewSnapshot() *Snapshot {
	return &Snapshot{services: make(map[ServiceKey]*snapshotService)}
}

func NewEITSnapshot() *Snapshot {
	return NewSnapshot()
}

func (s *Snapshot) Observe(section *EITSection, now time.Time) bool {
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
			programs: make(map[int64]*program.Program),
		}
		s.services[key] = service
	}
	table := service.tables[section.TableID]
	if table == nil {
		table = &snapshotTable{
			segmentLast:     make(map[uint8]uint8),
			sections:        make(map[uint8]struct{}),
			sectionPrograms: make(map[uint8][]*program.Program),
			sectionVersions: make(map[uint8]uint8),
		}
		service.tables[section.TableID] = table
	}

	previousVersion, existed := table.sectionVersions[section.SectionNumber]
	changed := !existed || previousVersion != section.VersionNumber
	table.sections[section.SectionNumber] = struct{}{}
	table.sectionPrograms[section.SectionNumber] = section.Programs()
	table.sectionVersions[section.SectionNumber] = section.VersionNumber
	rebuildServicePrograms(service)

	table.version = section.VersionNumber
	table.hasVersion = true
	table.lastSection = section.LastSectionNumber
	table.segmentLast[section.SectionNumber/8] = section.SegmentLastSectionNumber

	if changed {
		s.lastProgress = now
	}
	return changed
}

func rebuildServicePrograms(service *snapshotService) {
	service.programs = make(map[int64]*program.Program)
	for _, table := range service.tables {
		for _, programs := range table.sectionPrograms {
			for _, item := range programs {
				service.programs[item.ID] = item
			}
		}
	}
}

func (s *Snapshot) ServiceComplete(key ServiceKey) bool {
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
			if table == nil || !snapshotTableComplete(table, tableID == base) {
				return false
			}
		}
	}
	return true
}

func (s *Snapshot) CompletionReport(key ServiceKey) SnapshotReport {
	service := s.services[key]
	if service == nil {
		return SnapshotReport{}
	}

	tableIDs := make([]int, 0, len(service.tables))
	for tableID := range service.tables {
		tableIDs = append(tableIDs, int(tableID))
	}
	sort.Ints(tableIDs)

	report := SnapshotReport{ObservedTables: len(tableIDs)}
	groups := make(map[uint8]map[uint8]struct{})
	for _, id := range tableIDs {
		tableID := uint8(id)
		table := service.tables[tableID]
		base := tableID & 0xf8
		if groups[base] == nil {
			groups[base] = make(map[uint8]struct{})
		}
		groups[base][tableID] = struct{}{}

		tableReport := SnapshotTableReport{
			TableID:          id,
			Version:          int(table.version),
			LastSection:      int(table.lastSection),
			ObservedSections: len(table.sections),
			Complete:         snapshotTableComplete(table, tableID == base),
		}
		firstObservedSegment := uint8(0)
		if tableID == base {
			firstObservedSegment = firstSegmentWithInfo(table)
		}
		lastSegment := table.lastSection / 8
		for segment := uint8(0); segment <= lastSegment; segment++ {
			segmentLast, ok := table.segmentLast[segment]
			if !ok {
				if tableID != base || segment >= firstObservedSegment {
					tableReport.MissingSegmentInfo = append(tableReport.MissingSegmentInfo, int(segment))
				}
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

func snapshotTableComplete(table *snapshotTable, allowLeadingMissing bool) bool {
	firstObservedSegment := uint8(0)
	if allowLeadingMissing {
		firstObservedSegment = firstSegmentWithInfo(table)
	}
	lastSegment := table.lastSection / 8
	for segment := uint8(0); segment <= lastSegment; segment++ {
		segmentLast, ok := table.segmentLast[segment]
		if !ok {
			if allowLeadingMissing && segment < firstObservedSegment {
				continue
			}
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

func firstSegmentWithInfo(table *snapshotTable) uint8 {
	if len(table.segmentLast) == 0 {
		return 0
	}
	first := table.lastSection/8 + 1
	for segment := range table.segmentLast {
		if segment < first {
			first = segment
		}
	}
	return first
}

func (s *Snapshot) AllComplete(expected []ServiceKey) bool {
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

func (s *Snapshot) StableFor(now time.Time, duration time.Duration) bool {
	return !s.lastProgress.IsZero() && now.Sub(s.lastProgress) >= duration
}

func (s *Snapshot) Programs(key ServiceKey) []*program.Program {
	service := s.services[key]
	if service == nil {
		return nil
	}
	result := make([]*program.Program, 0, len(service.programs))
	for _, item := range service.programs {
		result = append(result, item)
	}
	return result
}

func (s *Snapshot) Observed(key ServiceKey) bool {
	service := s.services[key]
	return service != nil && len(service.tables) > 0
}
