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
	ServiceID                uint16
	TransportStreamID        uint16
	OriginalNetworkID        uint16
	TableID                  byte
	VersionNumber            byte
	SectionNumber            byte
	LastSectionNumber        byte
	SegmentLastSectionNumber byte
	LastTableID              byte
	Events                   []EITEvent
}

// ParseEIT parses an EIT section.
func ParseEIT(s Section) (*EIT, error) {
	if len(s) < 18 || (!IsEITPF(s.TableID()) && !IsEITS(s.TableID())) || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	sectionEnd := s.TotalLength() - 4
	eit := &EIT{
		ServiceID:                header.ServiceID,
		TransportStreamID:        uint16(s[8])<<8 | uint16(s[9]),
		OriginalNetworkID:        uint16(s[10])<<8 | uint16(s[11]),
		TableID:                  header.TableID,
		VersionNumber:            header.VersionNumber,
		SectionNumber:            header.SectionNumber,
		LastSectionNumber:        header.LastSectionNumber,
		SegmentLastSectionNumber: s[12],
		LastTableID:              s[13],
	}
	off := 14
	for off+12 <= sectionEnd {
		start, err := parseMJDTime(s[off+2 : off+7])
		if err != nil {
			return nil, err
		}
		duration, err := parseBCDDuration(s[off+7 : off+10])
		if err != nil {
			return nil, err
		}
		descriptorsLoopLen := int(uint16(s[off+10]&0x0f)<<8 | uint16(s[off+11]))
		descStart := off + 12
		descEnd := descStart + descriptorsLoopLen
		if descEnd > sectionEnd {
			return nil, ErrInvalidSection
		}
		descriptors := append([]byte(nil), s[descStart:descEnd]...)
		eit.Events = append(eit.Events, EITEvent{
			EventID:       uint16(s[off])<<8 | uint16(s[off+1]),
			StartTime:     start,
			Duration:      duration,
			RunningStatus: (s[off+10] >> 5) & 0x07,
			FreeCAMode:    s[off+10]&0x10 != 0,
			Descriptors:   ParseDescriptors(descriptors),
		})
		off = descEnd
	}
	if off != sectionEnd {
		return nil, ErrInvalidSection
	}
	return eit, nil
}

var jst = time.FixedZone("JST", 9*60*60)

func parseMJDTime(b []byte) (time.Time, error) {
	if len(b) != 5 {
		return time.Time{}, ErrInvalidSection
	}
	if allBytes(b, 0xff) {
		return time.Time{}, nil
	}
	mjd := int(uint16(b[0])<<8 | uint16(b[1]))
	hour, ok := decodeBCDByte(b[2])
	if !ok || hour > 23 {
		return time.Time{}, ErrInvalidSection
	}
	minute, ok := decodeBCDByte(b[3])
	if !ok || minute > 59 {
		return time.Time{}, ErrInvalidSection
	}
	second, ok := decodeBCDByte(b[4])
	if !ok || second > 59 {
		return time.Time{}, ErrInvalidSection
	}

	yp := int((float64(mjd) - 15078.2) / 365.25)
	mp := int((float64(mjd) - 14956.1 - float64(int(float64(yp)*365.25))) / 30.6001)
	day := mjd - 14956 - int(float64(yp)*365.25) - int(float64(mp)*30.6001)
	k := 0
	if mp == 14 || mp == 15 {
		k = 1
	}
	year := yp + k + 1900
	month := time.Month(mp - 1 - k*12)
	return time.Date(year, month, day, hour, minute, second, 0, jst), nil
}

func parseBCDDuration(b []byte) (time.Duration, error) {
	if len(b) != 3 {
		return 0, ErrInvalidSection
	}
	if allBytes(b, 0xff) {
		return 0, nil
	}
	hour, ok := decodeBCDByte(b[0])
	if !ok {
		return 0, ErrInvalidSection
	}
	minute, ok := decodeBCDByte(b[1])
	if !ok || minute > 59 {
		return 0, ErrInvalidSection
	}
	second, ok := decodeBCDByte(b[2])
	if !ok || second > 59 {
		return 0, ErrInvalidSection
	}
	return time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second, nil
}

func decodeBCDByte(b byte) (int, bool) {
	high := int(b >> 4)
	low := int(b & 0x0f)
	if high > 9 || low > 9 {
		return 0, false
	}
	return high*10 + low, true
}

func allBytes(b []byte, want byte) bool {
	for _, v := range b {
		if v != want {
			return false
		}
	}
	return true
}
