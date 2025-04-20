package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseCommandLine(t *testing.T) {
	type args struct {
		commandline string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "Single argument",
			args: args{
				commandline: "arg1",
			},
			want: []string{
				"arg1",
			},
			wantErr: false,
		},
		{
			name: "Multiple arguments",
			args: args{
				commandline: "arg1 arg2 arg3",
			},
			want: []string{
				"arg1",
				"arg2",
				"arg3",
			},
			wantErr: false,
		},
		{
			name: "Quoted argument",
			args: args{
				commandline: `"arg1 arg2" 'arg3 arg4'`,
			},
			want: []string{
				"arg1 arg2",
				"arg3 arg4",
			},
			wantErr: false,
		},
		{
			name: "Empty command line",
			args: args{
				commandline: "",
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "Escaped quotes",
			args: args{
				commandline: `\"arg1\" arg2`,
			},
			want: []string{
				"\"arg1\"",
				"arg2",
			},
			wantErr: false,
		},
		{
			name: "Unclosed double quote",
			args: args{
				commandline: `"arg1 arg2`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Unclosed single quote",
			args: args{
				commandline: `'arg1 arg2`,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Escaped backslash",
			args: args{
				commandline: `arg1\\ arg2`,
			},
			want: []string{
				"arg1\\",
				"arg2",
			},
			wantErr: false,
		},
		{
			name: "Escaped space",
			args: args{
				commandline: `arg1\ arg2`,
			},
			want: []string{
				"arg1 arg2",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCommandLine(tt.args.commandline)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCommandLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ParseCommandLine() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
