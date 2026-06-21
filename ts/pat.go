package ts

import "fmt"

// PAT represents a Program Association Table.
type PAT struct {
	TransportStreamID uint16
	VersionNumber     byte
	NetworkPID        uint16
	Programs          map[uint16]uint16 // program_number -> PMT PID
}

// ParsePAT parses a PAT section.
func ParsePAT(s Section) (*PAT, error) {
	if len(s) < 12 || s.TableID() != TableIDPAT || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	pat := &PAT{
		TransportStreamID: header.TransportStreamID,
		VersionNumber:     header.VersionNumber,
		Programs:          map[uint16]uint16{},
	}
	end := s.TotalLength() - 4
	for off := 8; off+4 <= end; off += 4 {
		program := uint16(s[off])<<8 | uint16(s[off+1])
		pid := uint16(s[off+2]&0x1f)<<8 | uint16(s[off+3])
		if program == 0 {
			pat.NetworkPID = pid
			continue
		}
		pat.Programs[program] = pid
	}
	return pat, nil
}

func BuildPATSection(transportStreamID, serviceID, pmtPID uint16, version byte) (Section, error) {
	if pmtPID > 0x1fff {
		return nil, fmt.Errorf("ts: invalid PMT PID %d", pmtPID)
	}
	sectionLength := 5 + 4 + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDPAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(transportStreamID >> 8)
	s[4] = byte(transportStreamID)
	s[5] = 0xc1 | ((version & 0x1f) << 1)
	s[6] = 0
	s[7] = 0
	s[8] = byte(serviceID >> 8)
	s[9] = byte(serviceID)
	s[10] = 0xe0 | byte(pmtPID>>8)
	s[11] = byte(pmtPID)
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
	return Section(s), nil
}
