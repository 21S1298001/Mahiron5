package ts

import (
	"context"
	"errors"
	"fmt"
	"io"
)

const (
	PIDPAT  = 0x0000
	PIDCAT  = 0x0001
	PIDNIT  = 0x0010
	PIDSDT  = 0x0011
	PIDEIT  = 0x0012
	PIDRST  = 0x0013
	PIDTOT  = 0x0014
	PIDDIT  = 0x001e
	PIDSIT  = 0x001f
	PIDBIT  = 0x0024
	PIDCDT  = 0x0029
	PIDNull = 0x1fff
)

var (
	ErrServiceNotFound = errors.New("ts: service not found")
)

// ServiceFilter reads a raw TS stream and writes only packets belonging to the given service.
type ServiceFilter struct {
	serviceID uint16
}

// NewServiceFilter creates a filter for the given service_id.
func NewServiceFilter(serviceID uint16) *ServiceFilter {
	return &ServiceFilter{serviceID: serviceID}
}

// Filter copies only packets required for the service from src to dst.
// Required PIDs are PAT, PMT, PCR, and elementary stream PIDs referenced by the service.
func (f *ServiceFilter) Filter(ctx context.Context, src io.Reader, dst io.Writer) error {
	state := newServiceFilterState(f.serviceID)
	reader := NewPacketReader(src)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		packet, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
			continue
		}
		if err := state.observe(packet); err != nil {
			return err
		}
		out := state.outputPacket(packet)
		if out == nil {
			continue
		}
		if _, err := dst.Write(out); err != nil {
			return err
		}
	}
}

type serviceFilterState struct {
	serviceID         uint16
	transportStreamID uint16
	pmtPID            uint16
	patPackets        []Packet
	patIndex          int
	patCounter        byte
	assemblers        map[uint16]*SectionAssembler
	psiPIDs           map[uint16]bool
	contentPIDs       map[uint16]bool
	emmPIDs           map[uint16]bool
}

func newServiceFilterState(serviceID uint16) *serviceFilterState {
	psi := map[uint16]bool{
		PIDPAT: true,
		PIDCAT: true,
		PIDNIT: true,
		PIDSDT: true,
		PIDEIT: true,
		PIDRST: true,
		PIDTOT: true,
		PIDDIT: true,
		PIDSIT: true,
		PIDBIT: true,
		PIDCDT: true,
	}
	return &serviceFilterState{
		serviceID:   serviceID,
		assemblers:  map[uint16]*SectionAssembler{},
		psiPIDs:     psi,
		contentPIDs: map[uint16]bool{},
		emmPIDs:     map[uint16]bool{},
	}
}

func (s *serviceFilterState) observe(packet Packet) error {
	pid := packet.PID()
	if pid != PIDPAT && pid != PIDCAT && pid != s.pmtPID {
		return nil
	}
	assembler := s.assemblers[pid]
	if assembler == nil {
		assembler = NewSectionAssembler(pid)
		s.assemblers[pid] = assembler
	}
	sections, err := assembler.FeedAll(packet)
	if err != nil {
		return err
	}
	for _, section := range sections {
		switch section.TableID() {
		case TableIDPAT:
			if pid != PIDPAT {
				continue
			}
			if err := s.handlePAT(section); err != nil {
				return err
			}
		case TableIDCAT:
			if pid == PIDCAT {
				s.handleCAT(section)
			}
		case TableIDPMT:
			if pid == s.pmtPID {
				if err := s.handlePMT(section); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *serviceFilterState) handlePAT(section Section) error {
	pat, err := ParsePAT(section)
	if err != nil {
		return nil
	}
	pmtPID, ok := pat.Programs[s.serviceID]
	if !ok {
		return fmt.Errorf("%w: service_id %d", ErrServiceNotFound, s.serviceID)
	}
	if s.pmtPID != pmtPID {
		delete(s.psiPIDs, s.pmtPID)
		s.pmtPID = pmtPID
		s.psiPIDs[pmtPID] = true
	}
	s.transportStreamID = pat.TransportStreamID
	rewritten, err := BuildPATSection(pat.TransportStreamID, s.serviceID, pmtPID, pat.VersionNumber)
	if err != nil {
		return err
	}
	s.patPackets = packetizeSection(PIDPAT, rewritten, &s.patCounter)
	s.patIndex = 0
	return nil
}

func (s *serviceFilterState) handleCAT(section Section) {
	if len(section) < 12 || section.TotalLength() > len(section) || !section.ValidateCRC() {
		return
	}
	emm := map[uint16]bool{}
	end := section.TotalLength() - 4
	for _, desc := range ParseDescriptors(section[8:end]) {
		if desc.Tag() == DescriptorTagCA {
			if pid, ok := caPID(desc); ok {
				emm[pid] = true
			}
		}
	}
	s.emmPIDs = emm
}

func (s *serviceFilterState) handlePMT(section Section) error {
	pmt, err := ParsePMT(section)
	if err != nil {
		return nil
	}
	if pmt.ProgramNumber != s.serviceID {
		return nil
	}
	content := map[uint16]bool{}
	if pmt.PCRPID != PIDNull {
		content[pmt.PCRPID] = true
	}
	for _, desc := range pmt.Descriptors {
		if desc.Tag() == DescriptorTagCA {
			if pid, ok := caPID(desc); ok {
				content[pid] = true
			}
		}
	}
	for _, elem := range pmt.Elements {
		content[elem.ElementaryPID] = true
		for _, desc := range elem.Descriptors {
			if desc.Tag() == DescriptorTagCA {
				if pid, ok := caPID(desc); ok {
					content[pid] = true
				}
			}
		}
	}
	s.contentPIDs = content
	return nil
}

func (s *serviceFilterState) outputPacket(packet Packet) Packet {
	pid := packet.PID()
	if pid == PIDPAT {
		if len(s.patPackets) == 0 {
			return nil
		}
		out := s.patPackets[s.patIndex]
		s.patIndex = (s.patIndex + 1) % len(s.patPackets)
		return out
	}
	if s.psiPIDs[pid] || s.contentPIDs[pid] || s.emmPIDs[pid] {
		return packet
	}
	return nil
}

func caPID(desc Descriptor) (uint16, bool) {
	data := desc.Data()
	if len(data) < 4 {
		return 0, false
	}
	return uint16(data[2]&0x1f)<<8 | uint16(data[3]), true
}

func packetizeSection(pid uint16, section Section, counter *byte) []Packet {
	var packets []Packet
	remaining := []byte(section)
	first := true
	for first || len(remaining) > 0 {
		packet := make([]byte, PacketSize)
		for i := range packet {
			packet[i] = 0xff
		}
		packet[0] = SyncByte
		packet[1] = byte(pid >> 8)
		if first {
			packet[1] |= 0x40
		}
		packet[2] = byte(pid)
		packet[3] = 0x10 | (*counter & 0x0f)
		*counter = (*counter + 1) & 0x0f

		offset := 4
		if first {
			packet[offset] = 0
			offset++
		}
		n := copy(packet[offset:], remaining)
		remaining = remaining[n:]
		packets = append(packets, Packet(packet))
		first = false
	}
	return packets
}
