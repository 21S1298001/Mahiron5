package ts

import (
	"errors"
	"io"
)

// Section represents a reassembled PSI/SI section.
type Section []byte

// TableID returns the table identifier.
func (s Section) TableID() byte { return s[0] }

// SectionSyntaxIndicator reports whether the section uses the long syntax.
func (s Section) SectionSyntaxIndicator() bool { return s[1]&0x80 != 0 }

// SectionLength returns the 12-bit section_length value.
func (s Section) SectionLength() int {
	return int((uint16(s[1]&0x0f) << 8) | uint16(s[2]))
}

// TotalLength returns the total byte length including the 3-byte prefix and CRC.
func (s Section) TotalLength() int { return 3 + s.SectionLength() }

// CRC32 returns the CRC32 stored at the end of the section.
func (s Section) CRC32() uint32 {
	end := s.TotalLength() - 4
	return uint32(s[end])<<24 | uint32(s[end+1])<<16 | uint32(s[end+2])<<8 | uint32(s[end+3])
}

// ValidateCRC checks the section CRC32-MPEG-2.
func (s Section) ValidateCRC() bool {
	// TODO: implement CRC32-MPEG-2 validation.
	return false
}

// SectionAssembler reassembles sections from packets of a single PID.
type SectionAssembler struct {
	pid          uint16
	buffer       []byte
	lastCounter  byte
	expecting    bool
}

// NewSectionAssembler creates an assembler for the given PID.
func NewSectionAssembler(pid uint16) *SectionAssembler {
	return &SectionAssembler{pid: pid}
}

// Feed feeds a packet to the assembler. If a complete section is assembled, it is returned.
func (sa *SectionAssembler) Feed(p Packet) (Section, error) {
	// TODO: implement section reassembly across packets.
	return nil, errors.New("ts: Feed not implemented")
}

// SectionScanner reads complete sections from a PacketReader for specific PIDs.
type SectionScanner struct {
	reader      *PacketReader
	assemblers  map[uint16]*SectionAssembler
	pidFilter   map[uint16]bool
}

// NewSectionScanner creates a scanner that collects sections for the given PIDs.
func NewSectionScanner(r *PacketReader, pids ...uint16) *SectionScanner {
	filter := make(map[uint16]bool, len(pids))
	for _, pid := range pids {
		filter[pid] = true
	}
	return &SectionScanner{
		reader:     r,
		assemblers: make(map[uint16]*SectionAssembler),
		pidFilter:  filter,
	}
}

// Next returns the next complete section for the configured PIDs.
func (ss *SectionScanner) Next() (Section, error) {
	// TODO: implement section scanning.
	return nil, io.EOF
}
