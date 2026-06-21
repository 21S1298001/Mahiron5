package ts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceFilterLocalFixtures(t *testing.T) {
	paths := localTSFixturePaths()
	if len(paths) == 0 {
		t.Skip("no local TS fixtures found")
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			serviceID := firstServiceIDFromFixture(t, path)

			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			var out bytes.Buffer
			if err := NewServiceFilter(serviceID).Filter(context.Background(), file, &out); err != nil {
				t.Fatal(err)
			}
			if out.Len() == 0 {
				t.Fatal("filtered output is empty")
			}
			if out.Len()%PacketSize != 0 {
				t.Fatalf("filtered output length = %d, want a multiple of %d", out.Len(), PacketSize)
			}

			pat := firstPATFromReader(t, bytes.NewReader(out.Bytes()))
			if len(pat.Programs) != 1 || pat.Programs[serviceID] == 0 {
				t.Fatalf("rewritten PAT programs = %#v, want only service_id %d", pat.Programs, serviceID)
			}
		})
	}
}

func localTSFixturePaths() []string {
	matches, _ := filepath.Glob("testdata/local/test-*-*.ts")
	return matches
}

func firstServiceIDFromFixture(t *testing.T, path string) uint16 {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	pat := firstPATFromReader(t, file)
	var serviceID uint16
	for id := range pat.Programs {
		if serviceID == 0 || id < serviceID {
			serviceID = id
		}
	}
	if serviceID == 0 {
		t.Fatalf("%s has no service in PAT", path)
	}
	return serviceID
}

func firstPATFromReader(t *testing.T, r io.Reader) *PAT {
	t.Helper()
	reader := NewPacketReader(r)
	assembler := NewSectionAssembler(PIDPAT)
	for packetCount := 0; packetCount < 10000; packetCount++ {
		packet, err := reader.Next()
		if errors.Is(err, io.EOF) {
			t.Fatal("PAT not found before EOF")
		}
		if err != nil {
			t.Fatal(err)
		}
		if packet.PID() != PIDPAT || packet.TransportErrorIndicator() {
			continue
		}
		sections, err := assembler.FeedAll(packet)
		if err != nil {
			t.Fatal(err)
		}
		for _, section := range sections {
			if section.TableID() != TableIDPAT {
				continue
			}
			pat, err := ParsePAT(section)
			if err != nil {
				t.Fatal(err)
			}
			return pat
		}
	}
	t.Fatal("PAT not found in first 10000 packets")
	return nil
}
