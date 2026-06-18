package ts

// PAT represents a Program Association Table.
type PAT struct {
	TransportStreamID uint16
	VersionNumber     byte
	NetworkPID        uint16
	Programs          map[uint16]uint16 // program_number -> PMT PID
}

// ParsePAT parses a PAT section.
func ParsePAT(s Section) (*PAT, error) {
	// TODO: implement PAT parsing.
	return nil, nil
}
