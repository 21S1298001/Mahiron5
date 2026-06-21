package filter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/21S1298001/Mahiron5/ts"
)

func TestServiceFilterUsesNativeTSFilter(t *testing.T) {
	input := sectionPackets(0, buildPAT(map[uint16]uint16{200: 0x0200}), 0)
	err := NewServiceFilter().FilterService(context.Background(), 100, bytes.NewReader(input), io.Discard)
	if !errors.Is(err, ts.ErrServiceNotFound) {
		t.Fatalf("FilterService error = %v, want ErrServiceNotFound", err)
	}
}

func buildPAT(programs map[uint16]uint16) []byte {
	sectionLength := 5 + len(programs)*4 + 4
	s := make([]byte, 3+sectionLength)
	s[0] = ts.TableIDPAT
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
	crc := crc32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
	return s
}

func sectionPackets(pid uint16, section []byte, counter byte) []byte {
	packet := make([]byte, ts.PacketSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0] = ts.SyncByte
	packet[1] = 0x40 | byte(pid>>8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | (counter & 0x0f)
	packet[4] = 0
	copy(packet[5:], section)
	return packet
}

func crc32MPEG2(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
