package ts

import (
	"errors"
	"testing"
	"time"
)

func TestParseTOTParsesJSTTimeAndDescriptors(t *testing.T) {
	jstTime := time.Date(2026, 6, 29, 12, 34, 56, 0, jst)
	desc := descriptor(DescriptorTagServiceList, []byte{0x01, 0x00, 0x01})
	tot, err := ParseTOT(buildTOT(t, jstTime, []Descriptor{desc}))
	if err != nil {
		t.Fatal(err)
	}
	if !tot.JSTTime.Equal(jstTime) {
		t.Fatalf("JSTTime = %s, want %s", tot.JSTTime, jstTime)
	}
	if len(tot.Descriptors) != 1 || tot.Descriptors[0].Tag() != DescriptorTagServiceList {
		t.Fatalf("descriptors = %#v", tot.Descriptors)
	}
}

func TestParseTOTRejectsInvalidSections(t *testing.T) {
	valid := buildTOT(t, time.Date(2026, 6, 29, 12, 34, 56, 0, jst), nil)

	wrongTableID := append(Section(nil), valid...)
	wrongTableID[0] = TableIDEITPF0
	writeCRC(wrongTableID)
	if _, err := ParseTOT(wrongTableID); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("wrong table id error = %v, want ErrInvalidSection", err)
	}

	brokenSyntaxIndicator := append(Section(nil), valid...)
	brokenSyntaxIndicator[1] |= 0x80
	writeCRC(brokenSyntaxIndicator)
	if _, err := ParseTOT(brokenSyntaxIndicator); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken syntax indicator error = %v, want ErrInvalidSection", err)
	}

	brokenCRC := append(Section(nil), valid...)
	brokenCRC[len(brokenCRC)-1] ^= 0xff
	if _, err := ParseTOT(brokenCRC); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken CRC error = %v, want ErrInvalidSection", err)
	}

	brokenBCD := buildTOT(t, time.Date(2026, 6, 29, 12, 34, 56, 0, jst), nil)
	brokenBCD[5] = 0x2a
	writeCRC(brokenBCD)
	if _, err := ParseTOT(brokenBCD); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken BCD error = %v, want ErrInvalidSection", err)
	}

	brokenLoop := buildTOT(t, time.Date(2026, 6, 29, 12, 34, 56, 0, jst), nil)
	brokenLoop[9] = 1
	writeCRC(brokenLoop)
	if _, err := ParseTOT(brokenLoop); !errors.Is(err, ErrInvalidSection) {
		t.Fatalf("broken descriptor loop error = %v, want ErrInvalidSection", err)
	}
}

func buildTOT(t *testing.T, jstTime time.Time, descriptors []Descriptor) Section {
	t.Helper()
	var descBytes []byte
	for _, desc := range descriptors {
		descBytes = append(descBytes, desc...)
	}
	length := 5 + 2 + len(descBytes) + 4
	s := make([]byte, 3+length)
	s[0] = TableIDTOT
	s[1] = 0x70 | byte(length>>8)
	s[2] = byte(length)
	copy(s[3:8], encodeMJDTime(jstTime))
	s[8] = 0xf0 | byte(len(descBytes)>>8)
	s[9] = byte(len(descBytes))
	copy(s[10:], descBytes)
	writeCRC(s)
	return Section(s)
}
