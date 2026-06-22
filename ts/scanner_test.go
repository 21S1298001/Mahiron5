package ts

// PSI/SI service-discovery scenarios follow ARIB STD-B10 and the terrestrial
// and satellite operational constraints in TR-B14/TR-B15.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestParseSDTParsesServiceDescriptors(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID: 100,
			descriptors: serviceDescriptor(1, nil, []byte{
				0x0e, 'N', 'H', 'K', 0x0f, 0x41, 0x6d,
			}),
		},
	})

	sdt, err := ParseSDT(section)
	if err != nil {
		t.Fatal(err)
	}
	if sdt.TransportStreamID != 0x1234 || sdt.OriginalNetworkID != 0x5678 {
		t.Fatalf("SDT ids = %#v/%#v, want 0x1234/0x5678", sdt.TransportStreamID, sdt.OriginalNetworkID)
	}
	if len(sdt.Services) != 1 || sdt.Services[0].ServiceID != 100 {
		t.Fatalf("SDT services = %#v", sdt.Services)
	}
	desc, err := ParseServiceDescriptor(sdt.Services[0].Descriptors[0])
	if err != nil {
		t.Fatal(err)
	}
	if desc.ServiceType != 1 || desc.ServiceName != "ＮＨＫ総" {
		t.Fatalf("service descriptor = %#v", desc)
	}
}

func TestParseSDTRejectsBrokenCRC(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, nil)
	section[len(section)-1] ^= 0xff
	if _, err := ParseSDT(section); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("ParseSDT error = %v, want ErrInvalidSection", err)
	}
}

func TestServiceScannerSkipsBrokenServiceDescriptor(t *testing.T) {
	section := buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID:   100,
			descriptors: []byte{DescriptorTagService, 2, 1, 5},
		},
	})
	input := append(sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}), 0), sectionPackets(PIDSDT, section, 0)...)

	got, err := NewServiceScanner().ScanServices(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ScanServices returned %#v, want no services", got)
	}
}

func TestServiceScannerDoesNotFilterServiceTypes(t *testing.T) {
	var input []byte
	input = append(input, sectionPackets(PIDPAT, buildPAT(t, map[uint16]uint16{
		100: 0x0100,
		101: 0x0101,
	}), 0)...)
	input = append(input, sectionPackets(PIDSDT, buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{
		{
			serviceID:   100,
			descriptors: serviceDescriptor(0xAD, nil, []byte{0x0e, '4', 'K'}),
		},
		{
			serviceID:   101,
			descriptors: serviceDescriptor(0xC0, nil, []byte{0x0e, 'D', 'A', 'T', 'A'}),
		},
	}), 0)...)

	got, err := NewServiceScanner().ScanServices(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	want := []ServiceInfo{
		{Nid: 0x5678, Tsid: 0x1234, Sid: 100, Name: "４Ｋ", Type: 0xAD, LogoId: -1},
		{Nid: 0x5678, Tsid: 0x1234, Sid: 101, Name: "ＤＡＴＡ", Type: 0xC0, LogoId: -1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ScanServices returned %#v, want %#v", got, want)
	}
}

func TestServiceScannerHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewServiceScanner().ScanServices(ctx, bytes.NewReader(nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ScanServices error = %v, want context.Canceled", err)
	}
}

func TestServiceScannerCompletesWithoutSDTForEveryPATService(t *testing.T) {
	state := newServiceScanState()
	observeTable(t, state, PIDPAT, buildPAT(t, map[uint16]uint16{
		100:    0x0100,
		0xfff0: 0x0101,
	}))
	observeTable(t, state, PIDSDT, buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID:   100,
		descriptors: serviceDescriptor(1, nil, []byte{0x0e, 'N', 'H', 'K'}),
	}}))
	observeTable(t, state, PIDNIT, buildNIT(t))

	if !state.complete() {
		t.Fatal("service scan did not complete after complete PAT, SDT, and NIT tables")
	}
	services := state.serviceList()
	if len(services) != 1 || services[0].Sid != 100 {
		t.Fatalf("service list = %#v, want only SID 100", services)
	}
}

func TestServiceScannerWaitsForEveryTableSection(t *testing.T) {
	state := newServiceScanState()
	pat0 := withTableHeader(buildPAT(t, map[uint16]uint16{100: 0x0100}), TableIDPAT, 0, 0, 1)
	pat1 := withTableHeader(buildPAT(t, map[uint16]uint16{101: 0x0101}), TableIDPAT, 0, 1, 1)
	sdt0 := withTableHeader(buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID:   100,
		descriptors: serviceDescriptor(1, nil, []byte{0x0e, 'A'}),
	}}), TableIDSDT0, 0, 0, 1)
	sdt1 := withTableHeader(buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID:   101,
		descriptors: serviceDescriptor(1, nil, []byte{0x0e, 'B'}),
	}}), TableIDSDT0, 0, 1, 1)
	nit0 := withTableHeader(buildNIT(t), TableIDNIT0, 0, 0, 1)
	nit1 := withTableHeader(buildNIT(t), TableIDNIT0, 0, 1, 1)

	for _, table := range []struct {
		pid     uint16
		section Section
	}{{PIDPAT, pat0}, {PIDSDT, sdt0}, {PIDNIT, nit0}, {PIDPAT, pat1}, {PIDSDT, sdt1}} {
		observeTable(t, state, table.pid, table.section)
		if state.complete() {
			t.Fatal("service scan completed before all NIT sections arrived")
		}
	}
	observeTable(t, state, PIDNIT, nit1)
	if !state.complete() {
		t.Fatal("service scan did not complete after all table sections arrived")
	}
	services := state.serviceList()
	if len(services) != 2 || services[0].Sid != 100 || services[1].Sid != 101 {
		t.Fatalf("service list = %#v, want SIDs 100 and 101", services)
	}
}

func TestTableSectionSetResetsOnVersionChange(t *testing.T) {
	var sections tableSectionSet
	v0s0 := withTableHeader(buildNIT(t), TableIDNIT0, 0, 0, 1)
	v1s1 := withTableHeader(buildNIT(t), TableIDNIT0, 1, 1, 1)
	v1s0 := withTableHeader(buildNIT(t), TableIDNIT0, 1, 0, 1)

	if _, ready := sections.add(v0s0); ready {
		t.Fatal("table became ready with only version 0 section 0")
	}
	if reset, ready := sections.add(v1s1); !reset || ready {
		t.Fatalf("version change = reset %v, ready %v; want true, false", reset, ready)
	}
	if _, ready := sections.add(v1s0); !ready {
		t.Fatal("version 1 table did not become ready with both version 1 sections")
	}
}

func TestServiceScannerIgnoresOtherTransportSDT(t *testing.T) {
	state := newServiceScanState()
	observeTable(t, state, PIDPAT, buildPAT(t, map[uint16]uint16{100: 0x0100}))
	observeTable(t, state, PIDNIT, buildNIT(t))
	other := withTableHeader(buildSDT(t, 0x9999, 0x5678, []sdtServiceSpec{{
		serviceID:   100,
		descriptors: serviceDescriptor(1, nil, []byte{0x0e, 'X'}),
	}}), TableIDSDT1, 0, 0, 0)
	observeTable(t, state, PIDSDT, other)
	if state.complete() || len(state.services) != 0 {
		t.Fatalf("other-TS SDT changed scan state: complete=%v services=%#v", state.complete(), state.services)
	}

	observeTable(t, state, PIDSDT, buildSDT(t, 0x1234, 0x5678, []sdtServiceSpec{{
		serviceID:   100,
		descriptors: serviceDescriptor(1, nil, []byte{0x0e, 'A'}),
	}}))
	if !state.complete() {
		t.Fatal("actual-TS SDT did not complete service scan")
	}
}

func TestServiceScannerReturnsCanceledWhenIncompleteInputCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := NewServiceScanner().ScanServices(ctx, reader)
		done <- err
	}()

	cancel()
	_ = writer.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ScanServices error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ScanServices did not return after cancellation and input close")
	}
}

type sdtServiceSpec struct {
	serviceID   uint16
	descriptors []byte
}

func buildSDT(t *testing.T, tsid, onid uint16, services []sdtServiceSpec) Section {
	t.Helper()
	serviceLoopLen := 0
	for _, svc := range services {
		serviceLoopLen += 5 + len(svc.descriptors)
	}
	sectionLength := 8 + serviceLoopLen + 4
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDSDT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3] = byte(tsid >> 8)
	s[4] = byte(tsid)
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8] = byte(onid >> 8)
	s[9] = byte(onid)
	s[10] = 0xff
	off := 11
	for _, svc := range services {
		s[off] = byte(svc.serviceID >> 8)
		s[off+1] = byte(svc.serviceID)
		s[off+2] = 0xff
		s[off+3] = 0xf0 | byte(len(svc.descriptors)>>8)
		s[off+4] = byte(len(svc.descriptors))
		copy(s[off+5:], svc.descriptors)
		off += 5 + len(svc.descriptors)
	}
	writeCRC(s)
	return Section(s)
}

func buildNIT(t *testing.T) Section {
	t.Helper()
	const sectionLength = 13
	s := make([]byte, 3+sectionLength)
	s[0] = TableIDNIT0
	s[1] = 0xf0 | byte(sectionLength>>8)
	s[2] = byte(sectionLength)
	s[3], s[4] = 0x56, 0x78
	s[5], s[6], s[7] = 0xc1, 0, 0
	s[8], s[9] = 0xf0, 0
	s[10], s[11] = 0xf0, 0
	writeCRC(s)
	return Section(s)
}

func withTableHeader(section Section, tableID, version, number, last byte) Section {
	section = append(Section(nil), section...)
	section[0] = tableID
	section[5] = 0xc1 | ((version & 0x1f) << 1)
	section[6] = number
	section[7] = last
	writeCRC(section)
	return section
}

func observeTable(t *testing.T, state *serviceScanState, pid uint16, section Section) {
	t.Helper()
	for _, packet := range readAllPackets(t, sectionPackets(pid, section, 0)) {
		if err := state.observe(packet); err != nil {
			t.Fatal(err)
		}
	}
}

func serviceDescriptor(serviceType uint8, providerName, serviceName []byte) []byte {
	data := []byte{serviceType, byte(len(providerName))}
	data = append(data, providerName...)
	data = append(data, byte(len(serviceName)))
	data = append(data, serviceName...)
	return append([]byte{DescriptorTagService, byte(len(data))}, data...)
}
