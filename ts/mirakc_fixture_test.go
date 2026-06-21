package ts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestServiceFilterMatchesMirakcAribPIDSet(t *testing.T) {
	cases := []struct {
		name       string
		inputPath  string
		mirakcPath string
		serviceID  uint16
	}{
		{
			name:       "gr-27-sid-1024",
			inputPath:  "testdata/local/test-gr-27.ts",
			mirakcPath: "testdata/local/mirakc-arib-filter-service-sid-1024-gr-27.ts",
			serviceID:  1024,
		},
		{
			name:       "bs-15-sid-101",
			inputPath:  "testdata/local/test-bs-15.ts",
			mirakcPath: "testdata/local/mirakc-arib-filter-serivce-sid-101-bs-15.ts",
			serviceID:  101,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !fileExists(tc.inputPath) || !fileExists(tc.mirakcPath) {
				t.Skip("local TS fixture or mirakc-arib output fixture not found")
			}

			input, err := os.Open(tc.inputPath)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()

			var filtered bytes.Buffer
			if err := NewServiceFilter(tc.serviceID).Filter(context.Background(), input, &filtered); err != nil {
				t.Fatal(err)
			}

			inputPIDs := packetPIDsFromFile(t, tc.inputPath)
			got := packetPIDsFromReader(t, bytes.NewReader(filtered.Bytes()))
			want := packetPIDsFromFile(t, tc.mirakcPath)
			want = intersectPIDSet(want, inputPIDs)

			if !reflect.DeepEqual(got, want) {
				t.Fatalf("filtered PID set = %#v, want mirakc-arib PID set %#v", got, want)
			}
		})
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func packetPIDsFromFile(t *testing.T, path string) []uint16 {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	return packetPIDsFromReader(t, file)
}

func packetPIDsFromReader(t *testing.T, r io.Reader) []uint16 {
	t.Helper()
	reader := NewPacketReader(r)
	pids := map[uint16]bool{}
	for {
		packet, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return sortedPIDSet(pids)
		}
		if err != nil {
			t.Fatal(err)
		}
		pids[packet.PID()] = true
	}
}

func intersectPIDSet(pids, allowed []uint16) []uint16 {
	allowedSet := map[uint16]bool{}
	for _, pid := range allowed {
		allowedSet[pid] = true
	}
	out := make([]uint16, 0, len(pids))
	for _, pid := range pids {
		if allowedSet[pid] {
			out = append(out, pid)
		}
	}
	return out
}

func sortedPIDSet(pids map[uint16]bool) []uint16 {
	out := make([]uint16, 0, len(pids))
	for pid := range pids {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
