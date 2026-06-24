package api

import (
	"testing"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestShouldDecode(t *testing.T) {
	tests := []struct {
		name   string
		decode apigen.OptInt
		want   bool
	}{
		{
			name: "default",
			want: true,
		},
		{
			name:   "decode 0",
			decode: apigen.NewOptInt(0),
			want:   false,
		},
		{
			name:   "decode 1",
			decode: apigen.NewOptInt(1),
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDecode(tt.decode); got != tt.want {
				t.Fatalf("shouldDecode() = %v, want %v", got, tt.want)
			}
		})
	}
}
