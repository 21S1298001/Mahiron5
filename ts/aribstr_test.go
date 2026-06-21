package ts

import "testing"

func TestDecodeARIBString(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want string
	}{
		{
			name: "alphanumeric",
			in:   []byte{0x0e, 'N', 'H', 'K', ' ', 'B', 'S'},
			want: "ＮＨＫ　ＢＳ",
		},
		{
			name: "jis x 0208",
			in:   []byte{0x41, 0x6d, 0x39, 0x67},
			want: "総合",
		},
		{
			name: "fixture mixed controls",
			in: []byte{
				0x0e, 'N', 'H', 'K',
				0x0f, 0x41, 0x6d, 0x39, 0x67,
				0x0e, '1', 0xfe,
				0x0f, 0x45, 0x6c, 0x35, 0x7e,
			},
			want: "ＮＨＫ総合１・東京",
		},
		{
			name: "escape single shift alphanumeric",
			in:   []byte{0x1b, 0x7e, 'B', 'S'},
			want: "ＢＳ",
		},
		{
			name: "collapse fullwidth spaces",
			in:   []byte{0x0e, 'N', 'H', 'K', ' ', ' ', 'B', 'S'},
			want: "ＮＨＫ　ＢＳ",
		},
		{
			name: "dangling kanji byte",
			in:   []byte{0x41},
			want: "\ufffd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeARIBString(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("DecodeARIBString(% x) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
