package ts

import "time"

// TOT represents a Time Offset Table section.
type TOT struct {
	JSTTime     time.Time
	Descriptors []Descriptor
}

// ParseTOT parses a TOT section.
func ParseTOT(s Section) (*TOT, error) {
	if len(s) < 14 || s.TableID() != TableIDTOT || s.SectionSyntaxIndicator() || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	sectionEnd := s.TotalLength() - 4
	jstTime, err := parseMJDTime(s[3:8])
	if err != nil || jstTime.IsZero() {
		return nil, ErrInvalidSection
	}
	descriptorsLoopLen := int(uint16(s[8]&0x0f)<<8 | uint16(s[9]))
	descStart := 10
	descEnd := descStart + descriptorsLoopLen
	if descEnd != sectionEnd {
		return nil, ErrInvalidSection
	}
	descriptors := append([]byte(nil), s[descStart:descEnd]...)
	return &TOT{
		JSTTime:     jstTime,
		Descriptors: ParseDescriptors(descriptors),
	}, nil
}
