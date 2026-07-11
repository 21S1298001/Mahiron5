package ts

import "testing"

func TestParseBITAndExtendedBroadcasterDescriptor(t *testing.T) {
	extended := descriptor(DescriptorTagExtendedBroadcaster, []byte{0x1f, 0x12, 0x34, 0x21, 0x10, 0x20, 0x7f, 0xe0, 0x05, 0xaa})
	services := descriptor(DescriptorTagServiceList, []byte{0x00, 0x65, 0x01})
	name := descriptor(DescriptorTagBroadcasterName, []byte{0x0e, 'T', 'V'})
	descriptors := append(append(services, extended...), name...)
	sectionLength := 7 + 3 + len(descriptors) + 4
	s := make(Section, 3+sectionLength)
	s[0], s[1], s[2] = TableIDBIT, 0xb0|byte(sectionLength>>8), byte(sectionLength)
	s[3], s[4], s[5], s[6], s[7] = 0x7f, 0xe0, 0xc3, 0, 0
	s[8], s[9] = 0xf0, 0
	s[10], s[11], s[12] = 0xff, 0xf0|byte(len(descriptors)>>8), byte(len(descriptors))
	copy(s[13:], descriptors)
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4], s[len(s)-3], s[len(s)-2], s[len(s)-1] = byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc)

	bit, err := ParseBIT(s)
	if err != nil {
		t.Fatal(err)
	}
	if bit.OriginalNetworkID != 0x7fe0 || bit.VersionNumber != 1 || !bit.CurrentNext || !bit.BroadcastViewPropriety || len(bit.Broadcasters) != 1 {
		t.Fatalf("BIT = %#v", bit)
	}
	if bit.Broadcasters[0].BroadcasterID != 0xff || len(bit.Broadcasters[0].Descriptors) != 3 {
		t.Fatalf("broadcaster = %#v", bit.Broadcasters[0])
	}
	ext, err := ParseExtendedBroadcasterDescriptor(bit.Broadcasters[0].Descriptors[1])
	if err != nil {
		t.Fatal(err)
	}
	if ext.BroadcasterType != 1 || ext.TerrestrialBroadcasterID != 0x1234 || len(ext.AffiliationIDs) != 2 || ext.AffiliationIDs[1] != 0x20 || len(ext.Broadcasters) != 1 || ext.Broadcasters[0].OriginalNetworkID != 0x7fe0 || ext.Broadcasters[0].BroadcasterID != 5 || len(ext.PrivateData) != 1 {
		t.Fatalf("extended = %#v", ext)
	}
}

func TestParseExtendedBroadcasterDescriptorRejectsTruncation(t *testing.T) {
	d := descriptor(DescriptorTagExtendedBroadcaster, []byte{0x1f, 0, 1, 0x20, 1})
	if _, err := ParseExtendedBroadcasterDescriptor(d); err == nil {
		t.Fatal("expected invalid descriptor")
	}
}
