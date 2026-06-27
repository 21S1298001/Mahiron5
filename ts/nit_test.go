package ts

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseNITParsesDescriptorLoops(t *testing.T) {
	networkDescriptors := descriptor(DescriptorTagServiceList, []byte{0x00, 0x64, 0x01})
	transports := []nitTransportSpec{
		{
			tsid:        0x1111,
			onid:        0x5678,
			descriptors: descriptor(DescriptorTagTSInformation, []byte{4, 0}),
		},
		{
			tsid:        0x2222,
			onid:        0x5679,
			descriptors: descriptor(DescriptorTagTerrestrialDeliverySystem, []byte{0x12, 0x39, 0x04, 0x10}),
		},
	}
	section := buildNITSection(t, TableIDNIT0, 0x3456, networkDescriptors, transports)

	got, err := ParseNIT(section)
	if err != nil {
		t.Fatal(err)
	}
	if got.NetworkID != 0x3456 || got.TableID != TableIDNIT0 || got.VersionNumber != 0 || got.SectionNumber != 0 || got.LastSectionNumber != 0 {
		t.Fatalf("NIT header = %#v", got)
	}
	if !reflect.DeepEqual(got.NetworkDescriptors, []Descriptor{networkDescriptors}) {
		t.Fatalf("network descriptors = %#v, want %#v", got.NetworkDescriptors, []Descriptor{networkDescriptors})
	}
	wantStreams := []NITTransportStream{
		{
			TransportStreamID: 0x1111,
			OriginalNetworkID: 0x5678,
			Descriptors:       []Descriptor{descriptor(DescriptorTagTSInformation, []byte{4, 0})},
		},
		{
			TransportStreamID: 0x2222,
			OriginalNetworkID: 0x5679,
			Descriptors:       []Descriptor{descriptor(DescriptorTagTerrestrialDeliverySystem, []byte{0x12, 0x39, 0x04, 0x10})},
		},
	}
	if !reflect.DeepEqual(got.TransportStreams, wantStreams) {
		t.Fatalf("transport streams = %#v, want %#v", got.TransportStreams, wantStreams)
	}
}

func TestParseNITAcceptsOtherNetworkTable(t *testing.T) {
	section := buildNITSection(t, TableIDNIT1, 0x3456, nil, nil)
	got, err := ParseNIT(section)
	if err != nil {
		t.Fatal(err)
	}
	if got.TableID != TableIDNIT1 {
		t.Fatalf("table id = %#02x, want %#02x", got.TableID, TableIDNIT1)
	}
}

func TestParseNITRejectsInvalidSections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(Section) Section
	}{
		{
			name: "bad CRC",
			mutate: func(section Section) Section {
				section[len(section)-1] ^= 0xff
				return section
			},
		},
		{
			name: "network loop too long",
			mutate: func(section Section) Section {
				section[9] = 0xff
				writeCRC(section)
				return section
			},
		},
		{
			name: "transport stream descriptor loop too long",
			mutate: func(section Section) Section {
				section[16] = 0xff
				writeCRC(section)
				return section
			},
		},
		{
			name: "transport stream loop has partial entry",
			mutate: func(section Section) Section {
				section[11] = 5
				writeCRC(section)
				return section
			},
		},
		{
			name: "trailing bytes after transport stream loop",
			mutate: func(section Section) Section {
				section[11] = 0
				writeCRC(section)
				return section
			},
		},
		{
			name: "malformed descriptor",
			mutate: func(section Section) Section {
				section[19] = 4
				writeCRC(section)
				return section
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			section := buildNITSection(t, TableIDNIT0, 0x3456, nil, []nitTransportSpec{
				{tsid: 0x1111, onid: 0x5678, descriptors: descriptor(DescriptorTagTSInformation, []byte{4, 0})},
			})
			section = tt.mutate(append(Section(nil), section...))
			if _, err := ParseNIT(section); !errors.Is(err, ErrInvalidSection) {
				t.Fatalf("ParseNIT error = %v, want ErrInvalidSection", err)
			}
		})
	}
}

func TestParseServiceListDescriptor(t *testing.T) {
	desc := descriptor(DescriptorTagServiceList, []byte{0x00, 0x64, 0x01, 0x00, 0x65, 0xad})
	got, err := ParseServiceListDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	want := &ServiceListDescriptor{
		Services: []ServiceListEntry{
			{ServiceID: 100, ServiceType: 0x01},
			{ServiceID: 101, ServiceType: 0xad},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("service list descriptor = %#v, want %#v", got, want)
	}
	if _, err := ParseServiceListDescriptor(descriptor(DescriptorTagServiceList, []byte{0x00})); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("invalid service list error = %v, want ErrInvalidSection", err)
	}
}

func TestParseTerrestrialDeliverySystemDescriptor(t *testing.T) {
	desc := descriptor(DescriptorTagTerrestrialDeliverySystem, []byte{0x12, 0x39, 0x04, 0x10, 0x04, 0x20})
	got, err := ParseTerrestrialDeliverySystemDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	want := &TerrestrialDeliverySystemDescriptor{
		AreaCode:         0x123,
		GuardInterval:    0x02,
		TransmissionMode: 0x01,
		Frequencies:      []uint16{0x0410, 0x0420},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("terrestrial delivery system descriptor = %#v, want %#v", got, want)
	}
	if _, err := ParseTerrestrialDeliverySystemDescriptor(descriptor(DescriptorTagTerrestrialDeliverySystem, []byte{0x12, 0x39, 0x04})); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("invalid terrestrial delivery system error = %v, want ErrInvalidSection", err)
	}
}

func TestParseTSInformationDescriptorAllowsReservedTail(t *testing.T) {
	desc := descriptor(DescriptorTagTSInformation, []byte{7, 0, 0xff, 0xff})
	got, err := ParseTSInformationDescriptor(desc)
	if err != nil {
		t.Fatal(err)
	}
	if got.RemoteControlKeyID != 7 || got.TSName != "" || len(got.TransmissionTypes) != 0 {
		t.Fatalf("TS information descriptor = %#v", got)
	}
}

func buildNITSection(t *testing.T, tableID byte, networkID uint16, networkDescriptors []byte, transports []nitTransportSpec) Section {
	t.Helper()
	tsLoopLen := 0
	for _, transport := range transports {
		tsLoopLen += 6 + len(transport.descriptors)
	}
	sectionLength := 9 + len(networkDescriptors) + tsLoopLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = tableID
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(networkID >> 8)
	s[4] = byte(networkID)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = 0xf0 | byte(len(networkDescriptors)>>8)
	s[9] = byte(len(networkDescriptors))
	copy(s[10:], networkDescriptors)
	off := 10 + len(networkDescriptors)
	s[off] = 0xf0 | byte(tsLoopLen>>8)
	s[off+1] = byte(tsLoopLen)
	off += 2
	for _, transport := range transports {
		s[off] = byte(transport.tsid >> 8)
		s[off+1] = byte(transport.tsid)
		s[off+2] = byte(transport.onid >> 8)
		s[off+3] = byte(transport.onid)
		s[off+4] = 0xf0 | byte(len(transport.descriptors)>>8)
		s[off+5] = byte(len(transport.descriptors))
		copy(s[off+6:], transport.descriptors)
		off += 6 + len(transport.descriptors)
	}
	writeCRC(s)
	return Section(s)
}
