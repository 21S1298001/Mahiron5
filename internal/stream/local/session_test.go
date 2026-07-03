package local

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/stream/internal/streamtest"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/ts"
)

func TestSessionSectionUpdaterIgnoresScheduleEIT(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	for range sectionQueueSize + 1 {
		session.observeSection(ts.Section{ts.TableIDEITSStart})
	}
	if got := len(session.sectionQueue); got != 0 {
		t.Fatalf("section updater queue length = %d, want 0 for schedule EIT", got)
	}

	session.observeSection(ts.Section{ts.TableIDEITPF0})
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want EIT p/f to be queued", got)
	}
}

func TestSessionSectionUpdaterRoutesCommonLogoSections(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	session.observeSection(ts.Section{ts.TableIDCDT})
	session.observeSection(ts.Section{ts.TableIDSDTT})
	if got := len(session.sectionQueue); got != 2 {
		t.Fatalf("section updater queue length = %d, want CDT and SDTT to be queued", got)
	}
	if got := len(session.carouselQueue); got != 0 {
		t.Fatalf("carousel updater queue length = %d, want 0 before carousel sections", got)
	}

	session.observeSection(ts.Section{ts.TableIDDSMCCDII})
	session.observeSection(ts.Section{ts.TableIDDSMCCDDB})
	if got := len(session.carouselQueue); got != 2 {
		t.Fatalf("carousel updater queue length = %d, want DII and DDB to be queued", got)
	}
	if got := len(session.sectionQueue); got != 2 {
		t.Fatalf("section updater queue length = %d, want unchanged after carousel sections", got)
	}
}

func TestSessionSectionUpdaterCarouselOverflowDoesNotBlockSections(t *testing.T) {
	session := &Session{
		channel:       "BS01_0",
		typ:           "BS",
		sectionQueue:  make(chan ts.Section, sectionQueueSize),
		carouselQueue: make(chan ts.Section, carouselQueueSize),
	}

	for range carouselQueueSize + 1 {
		session.observeSection(ts.Section{ts.TableIDDSMCCDDB})
	}
	if got := len(session.carouselQueue); got != carouselQueueSize {
		t.Fatalf("carousel updater queue length = %d, want capped at %d", got, carouselQueueSize)
	}

	session.observeSection(ts.Section{ts.TableIDEITPF0})
	if got := len(session.sectionQueue); got != 1 {
		t.Fatalf("section updater queue length = %d, want EIT p/f unaffected by carousel overflow", got)
	}
}

func TestChannelSessionCollectEITWithClockUsesLatestTOT(t *testing.T) {
	clock := time.Date(2026, 6, 29, 12, 34, 56, 0, time.FixedZone("JST", 9*60*60))
	key := epgClockTestKey{networkID: 4, serviceID: 101}
	input := append(streamSectionPackets(ts.PIDTOT, streamBuildTOT(clock), 0), streamSectionPackets(ts.PIDEIT, streamBuildEIT(ts.TableIDEITSStart, key, 10), 1)...)
	session := NewSession(Config{
		Broadcast: source.NewBroadcast(streamtest.NewFinitePacketSource(input, streamtest.ClosedStart()), nil),
		Channel:   "27",
		Type:      "GR",
	})

	var gotClock time.Time
	var gotEventID uint16
	err := session.CollectEITWithClock(t.Context(), func(eit *ts.EIT, observedClock time.Time) error {
		gotClock = observedClock
		if len(eit.Events) > 0 {
			gotEventID = eit.Events[0].EventID
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gotClock.Equal(clock) {
		t.Fatalf("clock = %s, want %s", gotClock, clock)
	}
	if gotEventID != 10 {
		t.Fatalf("event id = %d, want 10", gotEventID)
	}
}

type epgClockTestKey struct {
	networkID uint16
	serviceID uint16
}

func streamBuildTOT(jstTime time.Time) ts.Section {
	encodedTime := streamEncodeMJDTime(jstTime)
	length := 5 + 2 + 4
	s := make([]byte, 3+length)
	s[0] = ts.TableIDTOT
	s[1] = 0x70 | byte(length>>8)
	s[2] = byte(length)
	copy(s[3:8], encodedTime)
	s[8] = 0xf0
	s[9] = 0
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamBuildEIT(tableID byte, key epgClockTestKey, eventID uint16) ts.Section {
	length := 11 + 12 + 4
	s := make([]byte, 3+length)
	s[0] = tableID
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	s[3] = byte(key.serviceID >> 8)
	s[4] = byte(key.serviceID)
	s[5] = 0xc1
	s[8] = 0
	s[9] = 1
	s[10] = byte(key.networkID >> 8)
	s[11] = byte(key.networkID)
	s[12] = 0
	s[13] = tableID
	off := 14
	s[off] = byte(eventID >> 8)
	s[off+1] = byte(eventID)
	copy(s[off+2:off+7], streamEncodeMJDTime(time.Date(2026, 6, 29, 13, 0, 0, 0, time.FixedZone("JST", 9*60*60))))
	copy(s[off+7:off+10], []byte{0x00, 0x30, 0x00})
	s[off+10] = 0x80
	s[off+11] = 0
	streamWriteCRC(s)
	return ts.Section(s)
}

func streamSectionPackets(pid uint16, section ts.Section, counter byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, ts.PacketSize)
	packet[0] = ts.SyncByte
	packet[1] = 0x40 | byte(pid>>8)
	packet[2] = byte(pid)
	packet[3] = 0x10 | counter&0x0f
	packet[4] = 0
	copy(packet[5:], section)
	return packet
}

func streamEncodeMJDTime(t time.Time) []byte {
	jst := time.FixedZone("JST", 9*60*60)
	t = t.In(jst)
	mjd := streamMJDFromDate(t)
	return []byte{byte(mjd >> 8), byte(mjd), streamEncodeBCD(t.Hour()), streamEncodeBCD(t.Minute()), streamEncodeBCD(t.Second())}
}

func streamMJDFromDate(t time.Time) int {
	y := t.Year() - 1900
	m := int(t.Month())
	d := t.Day()
	l := 0
	if m == 1 || m == 2 {
		l = 1
	}
	return 14956 + d + int(float64(y-l)*365.25) + int(float64(m+1+l*12)*30.6001)
}

func streamEncodeBCD(v int) byte {
	return byte((v/10)<<4 | (v % 10))
}

func streamWriteCRC(s []byte) {
	crc := streamCRC32MPEG2(s[:len(s)-4])
	s[len(s)-4] = byte(crc >> 24)
	s[len(s)-3] = byte(crc >> 16)
	s[len(s)-2] = byte(crc >> 8)
	s[len(s)-1] = byte(crc)
}

func streamCRC32MPEG2(data []byte) uint32 {
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

func TestSharedSessionUsesOneDescramblerForDecodedSubscribers(t *testing.T) {
	packet := streamtest.TestPacket(0x0100, 1)
	start := make(chan struct{})
	packetSource := streamtest.NewFinitePacketSource(bytes.Repeat(packet, 4), start)
	descrambler := &passthroughDescrambler{}
	session := NewSession(Config{
		Broadcast:   source.NewBroadcast(packetSource, nil),
		Channel:     "27",
		Descrambler: descrambler,
		OnStop:      func() {},
		Type:        "GR",
	})

	var first, second bytes.Buffer
	errs := make(chan error, 2)
	go func() { errs <- session.ChannelStream(t.Context(), true, &first) }()
	go func() { errs <- session.ChannelStream(t.Context(), true, &second) }()
	if !streamtest.Eventually(time.Second, func() bool { return session.decodedDemuxer.PacketSubscriberCount() == 2 }) {
		t.Fatal("decoded subscribers did not reach 2")
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if descrambler.starts.Load() != 1 {
		t.Fatalf("descrambler starts = %d, want 1", descrambler.starts.Load())
	}
	if first.Len() != 4*ts.PacketSize || second.Len() != 4*ts.PacketSize {
		t.Fatalf("decoded subscriber bytes = %d/%d", first.Len(), second.Len())
	}
}

type passthroughDescrambler struct {
	starts atomic.Int32
}

func (d *passthroughDescrambler) Descramble(_ context.Context, src io.Reader, dst io.Writer) error {
	d.starts.Add(1)
	_, err := io.Copy(dst, src)
	return err
}
