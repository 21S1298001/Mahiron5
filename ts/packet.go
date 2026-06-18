package ts

import (
	"errors"
	"io"
)

const (
	// PacketSize is the size of an ARIB MPEG-2 TS packet in bytes.
	PacketSize = 188
	// SyncByte is the first byte of every TS packet.
	SyncByte = 0x47
)

var (
	// ErrNoSync is returned when the reader cannot find a sync byte within a reasonable window.
	ErrNoSync = errors.New("ts: sync byte not found")
)

// Packet represents a single 188-byte MPEG-2 Transport Stream packet.
type Packet []byte

// Sync returns the sync byte (must be 0x47 for a valid packet).
func (p Packet) Sync() byte { return p[0] }

// TransportErrorIndicator returns true if the packet has a transport error.
func (p Packet) TransportErrorIndicator() bool { return p[1]&0x80 != 0 }

// PayloadUnitStartIndicator returns true if this packet starts a PES packet or section.
func (p Packet) PayloadUnitStartIndicator() bool { return p[1]&0x40 != 0 }

// Priority returns the transport priority bit.
func (p Packet) Priority() bool { return p[1]&0x20 != 0 }

// PID returns the 13-bit packet identifier.
func (p Packet) PID() uint16 { return (uint16(p[1]&0x1f) << 8) | uint16(p[2]) }

// ScramblingControl returns the 2-bit transport scrambling control value.
func (p Packet) ScramblingControl() byte { return (p[3] >> 6) & 0x03 }

// HasAdaptationField reports whether the packet contains an adaptation field.
func (p Packet) HasAdaptationField() bool { return (p[3]>>4)&0x03 >= 2 }

// HasPayload reports whether the packet contains a payload.
func (p Packet) HasPayload() bool { return (p[3]>>4)&0x03 == 1 || (p[3]>>4)&0x03 == 3 }

// ContinuityCounter returns the 4-bit continuity counter.
func (p Packet) ContinuityCounter() byte { return p[3] & 0x0f }

// AdaptationFieldLength returns the length of the adaptation field, or 0 if none.
func (p Packet) AdaptationFieldLength() int {
	if !p.HasAdaptationField() {
		return 0
	}
	return int(p[4])
}

// PayloadOffset returns the byte offset where the payload begins.
func (p Packet) PayloadOffset() int {
	if !p.HasAdaptationField() {
		return 4
	}
	return 5 + p.AdaptationFieldLength()
}

// Payload returns the packet payload bytes.
func (p Packet) Payload() []byte {
	if !p.HasPayload() {
		return nil
	}
	return p[p.PayloadOffset():]
}

// IsNull reports whether this is a null packet (PID 0x1fff).
func (p Packet) IsNull() bool { return p.PID() == 0x1fff }

// PacketReader reads TS packets from an io.Reader, recovering sync if necessary.
type PacketReader struct {
	r   io.Reader
	buf []byte
}

// NewPacketReader creates a new PacketReader.
func NewPacketReader(r io.Reader) *PacketReader {
	return &PacketReader{r: r, buf: make([]byte, PacketSize)}
}

// Next reads the next valid TS packet. It skips garbage until a sync byte is found.
func (pr *PacketReader) Next() (Packet, error) {
	// TODO: implement sync recovery and packet reading.
	return nil, errors.New("ts: Next not implemented")
}
