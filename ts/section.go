package ts

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
	if len(s) < 4 || s.TotalLength() > len(s) {
		return false
	}
	return crc32MPEG2(s[:s.TotalLength()]) == 0
}

// SectionAssembler reassembles sections from packets of a single PID.
type SectionAssembler struct {
	pid         uint16
	buffer      []byte
	lastCounter byte
	expecting   bool
	pending     []Section
}

// NewSectionAssembler creates an assembler for the given PID.
func NewSectionAssembler(pid uint16) *SectionAssembler {
	return &SectionAssembler{pid: pid}
}

// Feed feeds a packet to the assembler. If a complete section is assembled, it is returned.
func (sa *SectionAssembler) Feed(p Packet) (Section, error) {
	if len(sa.pending) > 0 {
		section := sa.pending[0]
		sa.pending = sa.pending[1:]
		return section, nil
	}
	sections, err := sa.FeedAll(p)
	if err != nil || len(sections) == 0 {
		return nil, err
	}
	if len(sections) > 1 {
		sa.pending = append(sa.pending, sections[1:]...)
	}
	return sections[0], nil
}

// FeedAll feeds a packet to the assembler and returns all complete sections found.
func (sa *SectionAssembler) FeedAll(p Packet) ([]Section, error) {
	if p.PID() != sa.pid || p.TransportErrorIndicator() || !p.HasPayload() {
		return nil, nil
	}
	payload := p.Payload()
	if len(payload) == 0 {
		return nil, nil
	}
	counter := p.ContinuityCounter()
	if sa.expecting && counter != ((sa.lastCounter+1)&0x0f) {
		sa.buffer = nil
		sa.expecting = false
	}
	sa.lastCounter = counter

	if p.PayloadUnitStartIndicator() {
		pointer := int(payload[0])
		payload = payload[1:]
		if pointer > len(payload) {
			sa.buffer = nil
			sa.expecting = false
			return nil, nil
		}
		var sections []Section
		if len(sa.buffer) > 0 && pointer > 0 {
			sa.buffer = append(sa.buffer, payload[:pointer]...)
			sections = append(sections, sa.completeSections(false)...)
		}
		sa.buffer = nil
		sa.expecting = false
		more := sa.consumePayload(payload[pointer:])
		sections = append(sections, more...)
		return sections, nil
	}
	if !sa.expecting {
		return nil, nil
	}
	sa.buffer = append(sa.buffer, payload...)
	return sa.completeSections(false), nil
}

func (sa *SectionAssembler) consumePayload(payload []byte) []Section {
	var sections []Section
	for len(payload) >= 3 && payload[0] != 0xff {
		total := 3 + int(uint16(payload[1]&0x0f)<<8|uint16(payload[2]))
		if total < 3 {
			return sections
		}
		if len(payload) < total {
			sa.buffer = append(sa.buffer[:0], payload...)
			sa.expecting = true
			return sections
		}
		section := append(Section(nil), payload[:total]...)
		if section.ValidateCRC() {
			sections = append(sections, section)
		}
		payload = payload[total:]
	}
	return sections
}

func (sa *SectionAssembler) completeSections(keepRemainder bool) []Section {
	var sections []Section
	for len(sa.buffer) >= 3 {
		total := 3 + int(uint16(sa.buffer[1]&0x0f)<<8|uint16(sa.buffer[2]))
		if len(sa.buffer) < total {
			sa.expecting = true
			return sections
		}
		section := append(Section(nil), sa.buffer[:total]...)
		if section.ValidateCRC() {
			sections = append(sections, section)
		}
		sa.buffer = sa.buffer[total:]
		if !keepRemainder && len(sa.buffer) == 0 {
			break
		}
	}
	if len(sa.buffer) == 0 {
		sa.expecting = false
	}
	return sections
}

// SectionScanner reads complete sections from a PacketReader for specific PIDs.
type SectionScanner struct {
	reader     *PacketReader
	assemblers map[uint16]*SectionAssembler
	pidFilter  map[uint16]bool
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
	for {
		for _, assembler := range ss.assemblers {
			if len(assembler.pending) > 0 {
				section := assembler.pending[0]
				assembler.pending = assembler.pending[1:]
				return section, nil
			}
		}
		p, err := ss.reader.Next()
		if err != nil {
			return nil, err
		}
		if !ss.pidFilter[p.PID()] {
			continue
		}
		assembler := ss.assemblers[p.PID()]
		if assembler == nil {
			assembler = NewSectionAssembler(p.PID())
			ss.assemblers[p.PID()] = assembler
		}
		section, err := assembler.Feed(p)
		if err != nil {
			return nil, err
		}
		if section != nil {
			return section, nil
		}
	}
}

func crc32MPEG2(data []byte) uint32 {
	var crc uint32 = 0xffffffff
	for _, b := range data {
		crc = (crc << 8) ^ crc32MPEG2Table[byte(crc>>24)^b]
	}
	return crc
}

var crc32MPEG2Table = makeCRC32MPEG2Table()

func makeCRC32MPEG2Table() [256]uint32 {
	var table [256]uint32
	for i := range table {
		crc := uint32(i) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}
