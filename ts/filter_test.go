package ts

// Transport packet, PAT and PMT vectors cover the MPEG-TS framing used by the
// current ARIB STD-B10/TR-B14/TR-B15 broadcast profiles.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestPacketReaderNormalizesPacketSizes(t *testing.T) {
	packet := payloadPacket(0x0100, []byte{1, 2, 3}, 0)
	for _, tc := range []struct {
		name   string
		input  []byte
		prefix []byte
	}{
		{name: "188", input: packet},
		{name: "192", input: append(append([]byte{0, 1, 2, 3}, packet...), 0, 1, 2, 3)},
		{name: "204", input: append(append([]byte{}, packet...), bytes.Repeat([]byte{0xee}, 16)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewPacketReader(bytes.NewReader(tc.input))
			got, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != PacketSize {
				t.Fatalf("packet length = %d, want %d", len(got), PacketSize)
			}
			if !bytes.Equal(got, packet) {
				t.Fatalf("packet mismatch")
			}
		})
	}
}

func TestServiceFilterFiltersAndRewritesPAT(t *testing.T) {
	input := buildFilterInput(t)

	var out bytes.Buffer
	if err := NewServiceFilter(100).Filter(context.Background(), bytes.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}

	packets := readAllPackets(t, out.Bytes())
	pids := map[uint16]int{}
	for _, p := range packets {
		pids[p.PID()]++
	}
	for _, pid := range []uint16{PIDPAT, PIDCAT, PIDNIT, PIDSDT, PIDEIT, PIDTOT, PIDBIT, PIDCDT, 0x0100, 0x0101, 0x0102, 0x1abc, 0x1abd} {
		if pids[pid] == 0 {
			t.Fatalf("PID %#04x was not passed; got PIDs %#v", pid, pids)
		}
	}
	for _, pid := range []uint16{0x0200, 0x0201, 0x0300} {
		if pids[pid] != 0 {
			t.Fatalf("PID %#04x was unexpectedly passed; got PIDs %#v", pid, pids)
		}
	}

	sections := collectSections(t, packets, PIDPAT)
	if len(sections) == 0 {
		t.Fatal("no PAT section in output")
	}
	pat, err := ParsePAT(sections[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(pat.Programs) != 1 || pat.Programs[100] != 0x0100 {
		t.Fatalf("rewritten PAT programs = %#v, want only service 100 -> 0x0100", pat.Programs)
	}
	if !sections[0].ValidateCRC() {
		t.Fatal("rewritten PAT CRC is invalid")
	}
}

func TestServiceFilterSkipsBrokenPacketsAndSections(t *testing.T) {
	brokenPAT := buildPAT(t, map[uint16]uint16{100: 0x0100})
	brokenPAT[len(brokenPAT)-1] ^= 0xff
	tei := payloadPacket(0x0102, []byte{9}, 0)
	tei[1] |= 0x80

	var input []byte
	input = append(input, []byte{1, 2, 3}...)
	input = append(input, sectionPackets(PIDPAT, brokenPAT, 0)...)
	input = append(input, tei...)
	input = append(input, sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}), 1)...)
	input = append(input, sectionPackets(0x0100, buildPMT(t, 100, 0x0101, []uint16{0x0102}, nil), 0)...)
	input = append(input, payloadPacket(0x0102, []byte{1}, 1)...)

	var out bytes.Buffer
	if err := NewServiceFilter(100).Filter(context.Background(), bytes.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	pids := map[uint16]int{}
	for _, p := range readAllPackets(t, out.Bytes()) {
		pids[p.PID()]++
	}
	if pids[0x0102] != 1 {
		t.Fatalf("PID 0x0102 count = %d, want only the non-TEI packet", pids[0x0102])
	}
}

func TestServiceFilterHandlesPMTUpdate(t *testing.T) {
	var input []byte
	input = append(input, sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}), 0)...)
	input = append(input, sectionPackets(0x0100, buildPMT(t, 100, 0x0101, []uint16{0x0102}, nil), 0)...)
	input = append(input, payloadPacket(0x0102, []byte{1}, 0)...)
	input = append(input, sectionPackets(0x0100, buildPMT(t, 100, 0x0103, []uint16{0x0104}, nil), 1)...)
	input = append(input, payloadPacket(0x0102, []byte{2}, 1)...)
	input = append(input, payloadPacket(0x0104, []byte{3}, 0)...)

	var out bytes.Buffer
	if err := NewServiceFilter(100).Filter(context.Background(), bytes.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	pids := map[uint16]int{}
	for _, p := range readAllPackets(t, out.Bytes()) {
		pids[p.PID()]++
	}
	if pids[0x0102] != 1 {
		t.Fatalf("old ES PID count = %d, want 1", pids[0x0102])
	}
	if pids[0x0104] != 1 {
		t.Fatalf("new ES PID count = %d, want 1", pids[0x0104])
	}
}

func TestServiceFilterReportsMissingService(t *testing.T) {
	input := sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{200: 0x0200}), 0)
	err := NewServiceFilter(100).Filter(context.Background(), bytes.NewReader(input), io.Discard)
	if !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("Filter error = %v, want ErrServiceNotFound", err)
	}
}

func TestServiceFilterHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewServiceFilter(100).Filter(ctx, bytes.NewReader(nil), io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Filter error = %v, want context.Canceled", err)
	}
}

func TestPacketReaderResyncsAfterGarbage(t *testing.T) {
	packet := payloadPacket(0x0100, []byte{1}, 0)
	input := append([]byte{0, 1, 2, 3, 4}, packet...)
	reader := NewPacketReader(bytes.NewReader(input))
	got, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.PID() != 0x0100 {
		t.Fatalf("PID = %#04x, want 0x0100", got.PID())
	}
}

func buildFilterInput(t *testing.T) []byte {
	t.Helper()
	var input []byte
	input = append(input, sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100, 200: 0x0200}), 0)...)
	input = append(input, sectionPackets(PIDCAT, buildCAT(t, 0x1abc), 0)...)
	input = append(input, sectionPackets(0x0100, buildPMT(t, 100, 0x0101, []uint16{0x0102}, []uint16{0x1abd}), 0)...)
	input = append(input, sectionPackets(0x0200, buildPMT(t, 200, 0x0201, []uint16{0x0202}, nil), 0)...)
	for _, pid := range []uint16{PIDNIT, PIDSDT, PIDEIT, PIDTOT, PIDBIT, PIDCDT, 0x0101, 0x0102, 0x1abc, 0x1abd, 0x0201, 0x0300} {
		input = append(input, payloadPacket(pid, []byte{byte(pid), byte(pid >> 8)}, 0)...)
	}
	return input
}

func readAllPackets(t *testing.T, data []byte) []Packet {
	t.Helper()
	reader := NewPacketReader(bytes.NewReader(data))
	var packets []Packet
	for {
		packet, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return packets
		}
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
	}
}

func collectSections(t *testing.T, packets []Packet, pid uint16) []Section {
	t.Helper()
	assembler := NewSectionAssembler(pid)
	var sections []Section
	for _, p := range packets {
		if p.PID() != pid {
			continue
		}
		got, err := assembler.FeedAll(p)
		if err != nil {
			t.Fatal(err)
		}
		sections = append(sections, got...)
	}
	return sections
}

func buildPAT(t *testing.T, programs map[uint16]uint16) Section {
	t.Helper()
	sectionLength := 5 + len(programs)*4 + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDPAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x12, 0x34
	s[5], s[6], s[7] = 0xc1, 0, 0
	off := 8
	for serviceID, pmtPID := range programs {
		s[off] = byte(serviceID >> 8)
		s[off+1] = byte(serviceID)
		s[off+2] = 0xe0 | byte(pmtPID>>8)
		s[off+3] = byte(pmtPID)
		off += 4
	}
	writeCRC(s)
	return Section(s)
}

func buildCAT(t *testing.T, emmPID uint16) Section {
	t.Helper()
	descriptors := caDescriptor(emmPID)
	sectionLength := 5 + len(descriptors) + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDCAT
	s[1] = 0xb0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[5], s[6], s[7] = 0xc1, 0, 0
	copy(s[8:], descriptors)
	writeCRC(s)
	return Section(s)
}

func buildPMT(t *testing.T, serviceID, pcrPID uint16, esPIDs []uint16, caPIDs []uint16) Section {
	t.Helper()
	var descriptors []byte
	for _, pid := range caPIDs {
		descriptors = append(descriptors, caDescriptor(pid)...)
	}
	bodyLen := 9 + len(descriptors) + len(esPIDs)*5 + 4
	s := make([]byte, 3+bodyLen)
	s[0] = TableIDPMT
	s[1] = 0xb0 | byte(bodyLen>>8)
	s[2] = byte(bodyLen)
	s[3] = byte(serviceID >> 8)
	s[4] = byte(serviceID)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = 0xe0 | byte(pcrPID>>8)
	s[9] = byte(pcrPID)
	s[10] = 0xf0 | byte(len(descriptors)>>8)
	s[11] = byte(len(descriptors))
	copy(s[12:], descriptors)
	off := 12 + len(descriptors)
	for _, pid := range esPIDs {
		s[off] = 0x1b
		s[off+1] = 0xe0 | byte(pid>>8)
		s[off+2] = byte(pid)
		s[off+3] = 0xf0
		s[off+4] = 0
		off += 5
	}
	writeCRC(s)
	return Section(s)
}

func caDescriptor(pid uint16) []byte {
	return []byte{DescriptorTagCA, 4, 0, 5, 0xe0 | byte(pid>>8), byte(pid)}
}

func writeCRC(s []byte) {
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func sectionPackets(pid uint16, section Section, counter byte) []byte {
	packets := packetizeSection(pid, section, &counter)
	var out []byte
	for _, p := range packets {
		out = append(out, p...)
	}
	return out
}

func payloadPacket(pid uint16, payload []byte, counter byte) Packet {
	packet := make([]byte, PacketSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0] = SyncByte
	packet[1] = byte(pid >> 8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | (counter & 0x0f)
	copy(packet[4:], payload)
	return Packet(packet)
}
