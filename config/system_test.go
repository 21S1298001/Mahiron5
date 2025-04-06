package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadAndParseSystemConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *SystemConfig
		wantErr bool
	}{
		{
			name: "Empty file",
			args: args{
				filePath: "testdata/empty.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Multiple http addresses",
			args: args{
				filePath: "testdata/system-multiple-http.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "test:1",
					},
					{
						Http: "test:2",
					},
					{
						Http: "test:3",
					},
				},
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Multiple unix addresses",
			args: args{
				filePath: "testdata/system-multiple-unix.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Unix: "/test1.sock",
					},
					{
						Unix: "/test2.sock",
					},
					{
						Unix: "/test3.sock",
					},
				},
				LogLevel: "info",
			},
			wantErr: false,
		},
		{
			name: "Both http and unix addresses",
			args: args{
				filePath: "testdata/system-http-and-unix.yml",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Verbose log level",
			args: args{
				filePath: "testdata/system-verbose.yml",
			},
			want: &SystemConfig{
				Addresses: []ServerAddress{
					{
						Http: "localhost:40772",
					},
				},
				LogLevel: "debug",
			},
			wantErr: false,
		},
		{
			name: "Invalid log level",
			args: args{
				filePath: "testdata/system-invalid-log-level.yml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadAndParseSystemConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAndParseSystemConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadAndParseSystemConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
