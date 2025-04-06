package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseChannelConfig(t *testing.T) {
	yes := true
	no := false

	ch2ServiceId := uint32(25565)

	ch3ServiceId := uint32(12345)
	ch3tsmfRelTs := uint8(15)

	ch5ServiceId := uint32(65534)

	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    ChannelConfig
		wantErr bool
	}{
		{
			name: "Valid config",
			args: args{
				filePath: "testdata/channels-valid.yml",
			},
			want: ChannelConfig{
				{
					Name:        "Channel1",
					Type:        "GR",
					Channel:     "GR01",
					ServiceId:   nil,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{},
				},
				{
					Name:        "Channel2",
					Type:        "SKY",
					Channel:     "SKY02",
					ServiceId:   &ch2ServiceId,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{},
				},
				{
					Name:        "Channel3",
					Type:        "CATV",
					Channel:     "CATV03",
					ServiceId:   &ch3ServiceId,
					TsmfRelTs:   &ch3tsmfRelTs,
					CommandVars: map[string]any{},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{},
				},
				{
					Name:      "Channel4",
					Type:      "BS",
					Channel:   "BS04",
					ServiceId: nil,
					TsmfRelTs: nil,
					CommandVars: map[string]any{
						"extra-args": "--extra-arg",
					},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{},
				},
				{
					Name:      "Channel5",
					Type:      "CS",
					Channel:   "CS05",
					ServiceId: &ch5ServiceId,
					TsmfRelTs: nil,
					CommandVars: map[string]any{
						"satellite": "SOMESAT",
						"space":     uint8(1),
						"freq":      uint32(12345),
						"polarity":  "H",
					},
					IsDisabled:  &no,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{},
				},
				{
					Name:        "Channel6",
					Type:        "GR",
					Channel:     "GR06",
					ServiceId:   nil,
					TsmfRelTs:   nil,
					CommandVars: map[string]any{},
					IsDisabled:  &yes,
					Satelite:    nil,
					Satellite:   nil,
					Space:       nil,
					Freq:        nil,
					Polarity:    nil,
					TunerGroups: []string{"GR_KANAGAWA"},
				},
			},
			wantErr: false,
		},
		{
			name: "Empty config",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty tuner name",
			args: args{
				filePath: "testdata/channels-empty-name.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty channel type",
			args: args{
				filePath: "testdata/channels-empty-type.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty channel symbol",
			args: args{
				filePath: "testdata/channels-empty-symbol.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Only specified tsmfRelTs",
			args: args{
				filePath: "testdata/channels-tsmfrelts.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid tsmfRelTs",
			args: args{
				filePath: "testdata/channels-invalid-tsmfrelts.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Duplicate specify commandVars and other fields",
			args: args{
				filePath: "testdata/channels-duplicate-commandvars.yml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseChannelConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseChannelConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadAndParseChannelConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
