package ts

// PMTElement represents an elementary stream entry in a PMT.
type PMTElement struct {
	StreamType    byte
	ElementaryPID uint16
	ESInfo        []byte
	Descriptors   []Descriptor
}

// PMT represents a Program Map Table.
type PMT struct {
	ProgramNumber uint16
	VersionNumber byte
	PCRPID        uint16
	ProgramInfo   []byte
	Descriptors   []Descriptor
	Elements      []PMTElement
}

// ParsePMT parses a PMT section.
func ParsePMT(s Section) (*PMT, error) {
	if len(s) < 16 || s.TableID() != TableIDPMT || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	programInfoLen := int(uint16(s[10]&0x0f)<<8 | uint16(s[11]))
	infoStart := 12
	infoEnd := infoStart + programInfoLen
	sectionEnd := s.TotalLength() - 4
	if infoEnd > sectionEnd {
		return nil, ErrInvalidSection
	}
	pmt := &PMT{
		ProgramNumber: header.ServiceID,
		VersionNumber: header.VersionNumber,
		PCRPID:        uint16(s[8]&0x1f)<<8 | uint16(s[9]),
		ProgramInfo:   append([]byte(nil), s[infoStart:infoEnd]...),
		Descriptors:   ParseDescriptors(s[infoStart:infoEnd]),
	}
	for off := infoEnd; off+5 <= sectionEnd; {
		esInfoLen := int(uint16(s[off+3]&0x0f)<<8 | uint16(s[off+4]))
		esInfoStart := off + 5
		esInfoEnd := esInfoStart + esInfoLen
		if esInfoEnd > sectionEnd {
			return nil, ErrInvalidSection
		}
		esInfo := append([]byte(nil), s[esInfoStart:esInfoEnd]...)
		pmt.Elements = append(pmt.Elements, PMTElement{
			StreamType:    s[off],
			ElementaryPID: uint16(s[off+1]&0x1f)<<8 | uint16(s[off+2]),
			ESInfo:        esInfo,
			Descriptors:   ParseDescriptors(esInfo),
		})
		off = esInfoEnd
	}
	return pmt, nil
}
