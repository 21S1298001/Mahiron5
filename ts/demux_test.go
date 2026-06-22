package ts

import (
	"bytes"
	"testing"
)

func TestDemuxerSharesProgramStateAcrossServices(t *testing.T) {
	d := NewDemuxer()
	feedDemuxSection(t, d, PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100, 200: 0x0200}))
	feedDemuxSection(t, d, 0x0100, buildPMT(t, 100, 0x0101, []uint16{0x0102}, []uint16{0x1abc}))
	feedDemuxSection(t, d, 0x0200, buildPMT(t, 200, 0x0201, []uint16{0x0202}, nil))
	feedDemuxSection(t, d, PIDCAT, buildCAT(t, 0x1abd))

	for _, tc := range []struct {
		pid       uint16
		serviceID uint16
		want      bool
	}{
		{0x0102, 100, true},
		{0x0102, 200, false},
		{0x0202, 200, true},
		{0x0202, 100, false},
		{0x1abc, 100, true},
		{0x1abc, 200, false},
		{0x1abd, 100, true},
		{0x1abd, 200, true},
		{PIDEIT, 100, true},
	} {
		packet := payloadPacket(tc.pid, []byte{1}, 0)
		got := d.ServicePacket(tc.serviceID, packet) != nil
		if got != tc.want {
			t.Errorf("ServicePacket(%d, PID %#x) present = %v, want %v", tc.serviceID, tc.pid, got, tc.want)
		}
	}
}

func TestDemuxerPATOutputContinuityAndProgramUpdate(t *testing.T) {
	d := NewDemuxer()
	pat := buildPAT(t, map[uint16]uint16{100: 0x0100})
	feedDemuxSection(t, d, PIDPAT, pat)
	input := payloadPacket(PIDPAT, nil, 7)
	first := d.ServicePacket(100, input)
	second := d.ServicePacket(100, input)
	if first == nil || second == nil || first.ContinuityCounter() != 0 || second.ContinuityCounter() != 1 {
		t.Fatalf("rewritten PAT continuity = %v/%v", first, second)
	}
	parsed := collectSections(t, []Packet{first, second}, PIDPAT)
	if len(parsed) != 2 {
		t.Fatalf("rewritten PAT sections = %d, want 2", len(parsed))
	}
	for _, section := range parsed {
		got, err := ParsePAT(section)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Programs) != 1 || got.Programs[100] != 0x0100 {
			t.Fatalf("rewritten PAT = %#v", got.Programs)
		}
	}

	updated := buildPAT(t, map[uint16]uint16{200: 0x0200})
	updated[5] = 0xc3
	writeCRC(updated)
	feedDemuxSection(t, d, PIDPAT, updated)
	if d.HasService(100) || !d.HasService(200) {
		t.Fatalf("services after PAT update: 100=%v 200=%v", d.HasService(100), d.HasService(200))
	}
}

func TestServiceDemuxPATContinuityIsIndependentPerSubscriber(t *testing.T) {
	d := NewDemuxer()
	feedDemuxSection(t, d, PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}))
	first := d.Service(100)
	second := d.Service(100)
	input := payloadPacket(PIDPAT, nil, 0)

	if got := first.Packet(input).ContinuityCounter(); got != 0 {
		t.Fatalf("first subscriber initial continuity = %d", got)
	}
	if got := second.Packet(input).ContinuityCounter(); got != 0 {
		t.Fatalf("second subscriber initial continuity = %d", got)
	}
	if got := first.Packet(input).ContinuityCounter(); got != 1 {
		t.Fatalf("first subscriber next continuity = %d", got)
	}
	if got := second.Packet(input).ContinuityCounter(); got != 1 {
		t.Fatalf("second subscriber next continuity = %d", got)
	}
}

func TestDemuxerPMTUpdateRemovesStalePID(t *testing.T) {
	d := NewDemuxer()
	feedDemuxSection(t, d, PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}))
	feedDemuxSection(t, d, 0x0100, buildPMT(t, 100, 0x0101, []uint16{0x0102}, nil))
	if d.ServicePacket(100, payloadPacket(0x0102, nil, 0)) == nil {
		t.Fatal("initial ES PID was not selected")
	}
	updated := buildPMT(t, 100, 0x0103, []uint16{0x0104}, nil)
	updated[5] = 0xc3
	writeCRC(updated)
	feedDemuxSection(t, d, 0x0100, updated)
	if d.ServicePacket(100, payloadPacket(0x0102, nil, 1)) != nil {
		t.Fatal("stale ES PID remained selected")
	}
	if d.ServicePacket(100, payloadPacket(0x0104, nil, 1)) == nil {
		t.Fatal("updated ES PID was not selected")
	}
}

func TestDemuxerWaitsForEveryPATSection(t *testing.T) {
	d := NewDemuxer()
	first := buildPAT(t, map[uint16]uint16{100: 0x0100})
	first[6], first[7] = 0, 1
	writeCRC(first)
	second := buildPAT(t, map[uint16]uint16{200: 0x0200})
	second[6], second[7] = 1, 1
	writeCRC(second)

	feedDemuxSection(t, d, PIDPAT, first)
	if d.PATReady() {
		t.Fatal("PAT became ready before last_section_number arrived")
	}
	feedDemuxSection(t, d, PIDPAT, second)
	if !d.PATReady() || !d.HasService(100) || !d.HasService(200) {
		t.Fatalf("combined PAT services: ready=%v 100=%v 200=%v", d.PATReady(), d.HasService(100), d.HasService(200))
	}
}

func feedDemuxSection(t *testing.T, d *Demuxer, pid uint16, section Section) {
	t.Helper()
	reader := NewPacketReader(bytes.NewReader(sectionPackets(pid, section, 0)))
	for {
		packet, err := reader.Next()
		if err != nil {
			break
		}
		if _, err := d.Feed(packet); err != nil {
			t.Fatal(err)
		}
	}
}
