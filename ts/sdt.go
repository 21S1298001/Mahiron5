package ts

// SDTService represents a service entry in an SDT.
type SDTService struct {
	ServiceID           uint16
	EITScheduleFlag     bool
	EITPresentFollowing bool
	RunningStatus       byte
	FreeCAMode          bool
	Descriptors         []Descriptor
}

// SDT represents a Service Description Table.
type SDT struct {
	TransportStreamID uint16
	OriginalNetworkID uint16
	VersionNumber     byte
	Services          []SDTService
}

// ParseSDT parses an SDT section.
func ParseSDT(s Section) (*SDT, error) {
	// TODO: implement SDT parsing.
	return nil, nil
}
