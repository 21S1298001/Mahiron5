package ts

import "time"

// EITEvent represents an event entry in an EIT section.
type EITEvent struct {
	EventID       uint16
	StartTime     time.Time
	Duration      time.Duration
	RunningStatus byte
	FreeCAMode    bool
	Descriptors   []Descriptor
}

// EIT represents an Event Information Table section.
type EIT struct {
	ServiceID              uint16
	TransportStreamID      uint16
	OriginalNetworkID      uint16
	TableID                byte
	VersionNumber          byte
	SectionNumber          byte
	LastSectionNumber      byte
	SegmentLastSectionNumber byte
	LastTableID            byte
	Events                 []EITEvent
}

// ParseEIT parses an EIT section.
func ParseEIT(s Section) (*EIT, error) {
	// TODO: implement EIT parsing.
	return nil, nil
}
