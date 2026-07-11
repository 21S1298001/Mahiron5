package ts

import "encoding/binary"

const (
	StreamDescriptorTagNPTReference = 0x17
	StreamDescriptorTagGeneralEvent = 0x40
)

// DSMCCStream describes a table_id 0x3d section carrying event-message or NPT descriptors.
type DSMCCStream struct {
	DataEventID         byte
	EventMessageGroupID uint16
	VersionNumber       byte
	CurrentNext         bool
	SectionNumber       byte
	LastSectionNumber   byte
	Descriptors         []Descriptor
}

type DSMCCGeneralEvent struct {
	EventMessageGroupID uint16
	TimeMode            byte
	TimeValue           []byte
	EventMessageType    byte
	EventMessageID      uint16
	PrivateData         []byte
}

type DSMCCNPTReference struct {
	PostDiscontinuityIndicator bool
	DSMContentID               byte
	STCReference               uint64
	NPTReference               uint64
	ScaleNumerator             int16
	ScaleDenominator           int16
}

func ParseDSMCCStream(s Section) (*DSMCCStream, error) {
	if len(s) < 12 || s.TableID() != TableIDDSMCCStream || !s.SectionSyntaxIndicator() || s[1]&0x40 != 0 || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	end := s.TotalLength() - 4
	out := &DSMCCStream{
		DataEventID: s[3] >> 4, EventMessageGroupID: uint16(s[3]&0x0f)<<8 | uint16(s[4]),
		VersionNumber: (s[5] >> 1) & 0x1f, CurrentNext: s[5]&1 != 0,
		SectionNumber: s[6], LastSectionNumber: s[7],
	}
	for off := 8; off < end; {
		if off+2 > end || off+2+int(s[off+1]) > end {
			return nil, ErrInvalidSection
		}
		d := append(Descriptor(nil), s[off:off+2+int(s[off+1])]...)
		out.Descriptors = append(out.Descriptors, d)
		off += len(d)
	}
	return out, nil
}

func ParseDSMCCGeneralEvent(d Descriptor) (DSMCCGeneralEvent, bool) {
	data := d.Data()
	if d.Tag() != StreamDescriptorTagGeneralEvent || len(data) < 6 {
		return DSMCCGeneralEvent{}, false
	}
	out := DSMCCGeneralEvent{EventMessageGroupID: uint16(data[0])<<4 | uint16(data[1]>>4), TimeMode: data[2]}
	off := 3
	switch out.TimeMode {
	case 0x00, 0x01, 0x02, 0x03, 0x05:
		if len(data) < off+8 {
			return DSMCCGeneralEvent{}, false
		}
		out.TimeValue = append([]byte(nil), data[off:off+5]...)
		off += 5
	}
	out.EventMessageType = data[off]
	out.EventMessageID = binary.BigEndian.Uint16(data[off+1 : off+3])
	out.PrivateData = append([]byte(nil), data[off+3:]...)
	return out, true
}

func ParseDSMCCNPTReference(d Descriptor) (DSMCCNPTReference, bool) {
	data := d.Data()
	if d.Tag() != StreamDescriptorTagNPTReference || len(data) < 18 {
		return DSMCCNPTReference{}, false
	}
	return DSMCCNPTReference{
		PostDiscontinuityIndicator: data[0]&0x80 != 0, DSMContentID: data[0] & 0x7f,
		STCReference:   uint64(data[1]&1)<<32 | uint64(binary.BigEndian.Uint32(data[2:6])),
		NPTReference:   uint64(data[9]&1)<<32 | uint64(binary.BigEndian.Uint32(data[10:14])),
		ScaleNumerator: int16(binary.BigEndian.Uint16(data[14:16])), ScaleDenominator: int16(binary.BigEndian.Uint16(data[16:18])),
	}, true
}

func (e DSMCCGeneralEvent) EventMessageNPT() (uint64, bool) {
	if e.TimeMode != 0x02 || len(e.TimeValue) != 5 {
		return 0, false
	}
	return uint64(e.TimeValue[0]&1)<<32 | uint64(binary.BigEndian.Uint32(e.TimeValue[1:])), true
}
