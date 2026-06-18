package ts

// PSI common constants and helpers.

// TableID constants for SI/PSI tables used in ARIB broadcasts.
const (
	TableIDPAT  = 0x00
	TableIDPMT  = 0x02
	TableIDSDT0 = 0x42 // actual TS
	TableIDSDT1 = 0x46 // other TS
	TableIDEITPF0       = 0x4E // present/following, actual TS
	TableIDEITPF1       = 0x4F // present/following, other TS
	TableIDEITSStart    = 0x50 // schedule, actual TS
	TableIDEITSEnd      = 0x5F // schedule, actual TS
	TableIDEITSOtherStart = 0x60 // schedule, other TS
	TableIDEITSOtherEnd   = 0x6F // schedule, other TS
	TableIDNIT0 = 0x40 // actual network
	TableIDNIT1 = 0x41 // other network
)

// IsEITPF reports whether the table_id is an EIT present/following table.
func IsEITPF(tableID byte) bool {
	return tableID == TableIDEITPF0 || tableID == TableIDEITPF1
}

// IsEITS reports whether the table_id is an EIT schedule table.
func IsEITS(tableID byte) bool {
	return (tableID >= TableIDEITSStart && tableID <= TableIDEITSEnd) ||
		(tableID >= TableIDEITSOtherStart && tableID <= TableIDEITSOtherEnd)
}

// SectionHeader holds common fields from the long section syntax.
type SectionHeader struct {
	TableID                byte
	SectionSyntaxIndicator bool
	SectionLength          int
	TransportStreamID      uint16
	OriginalNetworkID      uint16
	ServiceID              uint16
	VersionNumber          byte
	CurrentNextIndicator   bool
	SectionNumber          byte
	LastSectionNumber      byte
}

// ParseSectionHeader parses the common header of a long-syntax section.
// Table-specific fields are parsed by each table parser.
func ParseSectionHeader(s Section) (SectionHeader, error) {
	// TODO: implement common header parsing.
	return SectionHeader{}, nil
}
