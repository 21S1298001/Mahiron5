package ts

// NITTransportStream represents one transport stream entry in an NIT TS loop.
type NITTransportStream struct {
	TransportStreamID uint16
	OriginalNetworkID uint16
	Descriptors       []Descriptor
}

// NIT represents a Network Information Table section.
type NIT struct {
	NetworkID          uint16
	TableID            byte
	VersionNumber      byte
	SectionNumber      byte
	LastSectionNumber  byte
	NetworkDescriptors []Descriptor
	TransportStreams   []NITTransportStream
}

// ParseNIT parses an NIT actual-network or other-network section.
func ParseNIT(s Section) (*NIT, error) {
	if len(s) < 16 || (s.TableID() != TableIDNIT0 && s.TableID() != TableIDNIT1) || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	sectionEnd := s.TotalLength() - 4
	networkDescriptorsLen := int(uint16(s[8]&0x0f)<<8 | uint16(s[9]))
	descStart := 10
	descEnd := descStart + networkDescriptorsLen
	if descEnd+2 > sectionEnd {
		return nil, ErrInvalidSection
	}
	networkDescriptors, err := parseDescriptorsStrict(s[descStart:descEnd])
	if err != nil {
		return nil, err
	}

	off := descEnd
	transportStreamLoopLen := int(uint16(s[off]&0x0f)<<8 | uint16(s[off+1]))
	off += 2
	loopEnd := off + transportStreamLoopLen
	if loopEnd != sectionEnd {
		return nil, ErrInvalidSection
	}

	nit := &NIT{
		NetworkID:          uint16(s[3])<<8 | uint16(s[4]),
		TableID:            header.TableID,
		VersionNumber:      header.VersionNumber,
		SectionNumber:      header.SectionNumber,
		LastSectionNumber:  header.LastSectionNumber,
		NetworkDescriptors: networkDescriptors,
	}
	for off+6 <= loopEnd {
		descriptorsLen := int(uint16(s[off+4]&0x0f)<<8 | uint16(s[off+5]))
		descStart := off + 6
		descEnd := descStart + descriptorsLen
		if descEnd > loopEnd {
			return nil, ErrInvalidSection
		}
		descriptors, err := parseDescriptorsStrict(s[descStart:descEnd])
		if err != nil {
			return nil, err
		}
		nit.TransportStreams = append(nit.TransportStreams, NITTransportStream{
			TransportStreamID: uint16(s[off])<<8 | uint16(s[off+1]),
			OriginalNetworkID: uint16(s[off+2])<<8 | uint16(s[off+3]),
			Descriptors:       descriptors,
		})
		off = descEnd
	}
	if off != loopEnd {
		return nil, ErrInvalidSection
	}
	return nit, nil
}

func parseDescriptorsStrict(b []byte) ([]Descriptor, error) {
	var descriptors []Descriptor
	for len(b) > 0 {
		if len(b) < 2 {
			return nil, ErrInvalidSection
		}
		length := int(b[1])
		if len(b) < 2+length {
			return nil, ErrInvalidSection
		}
		descriptors = append(descriptors, Descriptor(append([]byte(nil), b[:2+length]...)))
		b = b[2+length:]
	}
	return descriptors, nil
}
