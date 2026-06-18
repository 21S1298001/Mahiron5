package ts

// PMTElement represents an elementary stream entry in a PMT.
type PMTElement struct {
	StreamType    byte
	ElementaryPID uint16
	ESInfo        []byte
}

// PMT represents a Program Map Table.
type PMT struct {
	ProgramNumber uint16
	VersionNumber byte
	PCRPID        uint16
	ProgramInfo   []byte
	Elements      []PMTElement
}

// ParsePMT parses a PMT section.
func ParsePMT(s Section) (*PMT, error) {
	// TODO: implement PMT parsing.
	return nil, nil
}
