package ts

import "testing"

func TestParseDSMCCStreamGeneralEvent(t *testing.T) {
	descriptor := []byte{StreamDescriptorTagGeneralEvent, 13, 0x12, 0x3f, 0x00, 0, 0, 0, 0, 0, 0x04, 0x45, 0x67, 0xaa, 0xbb}
	sectionLength := 5 + len(descriptor) + 4
	s := make(Section, 3+sectionLength)
	s[0], s[1], s[2] = TableIDDSMCCStream, 0xb0|byte(sectionLength>>8), byte(sectionLength)
	s[3], s[4] = 0xa1, 0x23
	s[5], s[6], s[7] = 0xc7, 0, 0
	copy(s[8:], descriptor)
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4], s[len(s)-3], s[len(s)-2], s[len(s)-1] = byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc)

	stream, err := ParseDSMCCStream(s)
	if err != nil {
		t.Fatal(err)
	}
	if stream.DataEventID != 0xa || stream.EventMessageGroupID != 0x123 || stream.VersionNumber != 3 || !stream.CurrentNext {
		t.Fatalf("header = %#v", stream)
	}
	if len(stream.Descriptors) != 1 {
		t.Fatalf("descriptors = %d", len(stream.Descriptors))
	}
	event, ok := ParseDSMCCGeneralEvent(stream.Descriptors[0])
	if !ok {
		t.Fatal("general event was not parsed")
	}
	if event.EventMessageGroupID != 0x123 || event.TimeMode != 0 || event.EventMessageType != 4 || event.EventMessageID != 0x4567 || len(event.PrivateData) != 2 || event.PrivateData[0] != 0xaa {
		t.Fatalf("event = %#v", event)
	}
}

func TestParseDSMCCStreamRejectsTruncatedDescriptor(t *testing.T) {
	s := Section{TableIDDSMCCStream, 0xb0, 11, 0, 0, 0xc1, 0, 0, 0x40, 10, 0, 0, 0, 0}
	if _, err := ParseDSMCCStream(s); err == nil {
		t.Fatal("expected invalid section")
	}
}

func TestParseDSMCCGeneralEventReservedTimeModeHasNoTimeField(t *testing.T) {
	d := Descriptor{StreamDescriptorTagGeneralEvent, 7, 0x12, 0x3f, 0x06, 0x04, 0x45, 0x67, 0xaa}
	event, ok := ParseDSMCCGeneralEvent(d)
	if !ok {
		t.Fatal("reserved time mode event was not parsed")
	}
	if len(event.TimeValue) != 0 || event.EventMessageType != 4 || event.EventMessageID != 0x4567 || len(event.PrivateData) != 1 {
		t.Fatalf("event = %#v", event)
	}
}

func TestParseDSMCCNPTReference(t *testing.T) {
	data := make([]byte, 18)
	data[0] = 0x85
	data[1], data[2], data[3], data[4], data[5] = 0xff, 0x23, 0x45, 0x67, 0x89
	data[9], data[10], data[11], data[12], data[13] = 0xff, 0x12, 0x34, 0x56, 0x78
	data[14], data[15], data[16], data[17] = 0, 1, 0, 1
	reference, ok := ParseDSMCCNPTReference(descriptor(StreamDescriptorTagNPTReference, data))
	if !ok {
		t.Fatal("NPT reference was not parsed")
	}
	if !reference.PostDiscontinuityIndicator || reference.DSMContentID != 5 || reference.STCReference != 0x123456789 || reference.NPTReference != 0x112345678 || reference.ScaleNumerator != 1 || reference.ScaleDenominator != 1 {
		t.Fatalf("reference = %#v", reference)
	}
}

func TestDSMCCGeneralEventNPT(t *testing.T) {
	d := descriptor(StreamDescriptorTagGeneralEvent, []byte{0x12, 0x3f, 2, 0xff, 0x12, 0x34, 0x56, 0x78, 4, 0x45, 0x67})
	event, ok := ParseDSMCCGeneralEvent(d)
	if !ok {
		t.Fatal("NPT event was not parsed")
	}
	npt, ok := event.EventMessageNPT()
	if !ok || npt != 0x112345678 {
		t.Fatalf("NPT = %#x, %v", npt, ok)
	}
}
