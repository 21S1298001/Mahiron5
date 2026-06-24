package aribstr

import "testing"

func TestTable721Sorted(t *testing.T) {
	assertRuneEntriesSorted(t, "aribTable721BMPKanjiPUA", aribTable721BMPKanjiPUA)
}

func TestJISX0213TablesSorted(t *testing.T) {
	assertStringEntriesSorted(t, "jisX0213DeltaDecode", jisX0213DeltaDecode)

	for i := 1; i < len(jisX0213Plane1Undefined); i++ {
		if jisX0213Plane1Undefined[i-1] >= jisX0213Plane1Undefined[i] {
			t.Fatalf("jisX0213Plane1Undefined is not strictly sorted at %d: %#04x >= %#04x",
				i, jisX0213Plane1Undefined[i-1], jisX0213Plane1Undefined[i])
		}
	}
}

func TestTable719RepresentativeCells(t *testing.T) {
	tests := []struct {
		first  byte
		second byte
		want   string
		ok     bool
	}{
		{0x75, 0x21, "\u3402", true},
		{0x76, 0x21, "\u9fc5", true},
		{0x7a, 0x56, "\U0001f211", true},
		{0x7b, 0x2b, "\u3245", true},
		{0x7c, 0x7b, "\u213b", true},
		{0x7d, 0x79, "\u269f", true},
		{0x7e, 0x7b, "\u24eb", true},
		{0x7a, 0x27, "", false},
		{0x77, 0x21, "", false},
	}

	for _, tt := range tests {
		got, ok := lookupARIBTable719AdditionalSymbol(tt.first, tt.second)
		if ok != tt.ok || got != tt.want {
			t.Fatalf("lookupARIBTable719AdditionalSymbol(%#02x, %#02x) = %q, %v; want %q, %v",
				tt.first, tt.second, got, ok, tt.want, tt.ok)
		}
	}
}

func assertRuneEntriesSorted(t *testing.T, name string, entries []runeEntry) {
	t.Helper()
	for i := 1; i < len(entries); i++ {
		if entries[i-1].key >= entries[i].key {
			t.Fatalf("%s is not strictly sorted at %d: %#06x >= %#06x",
				name, i, entries[i-1].key, entries[i].key)
		}
	}
}

func assertStringEntriesSorted(t *testing.T, name string, entries []stringEntry) {
	t.Helper()
	for i := 1; i < len(entries); i++ {
		if entries[i-1].key >= entries[i].key {
			t.Fatalf("%s is not strictly sorted at %d: %#06x >= %#06x",
				name, i, entries[i-1].key, entries[i].key)
		}
	}
}
