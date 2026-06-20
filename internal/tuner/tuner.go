package tuner

import "github.com/21S1298001/Mahiron5/internal/config"

type SourceKind string

const (
	SourceKindUnsupported SourceKind = ""
	SourceKindCommand     SourceKind = "command"
	SourceKindDVB         SourceKind = "dvb"
	SourceKindRemote      SourceKind = "remote"
)

type Tuner struct {
	config *config.TunerConfig
}

func NewTuner(config *config.TunerConfig) *Tuner {
	return &Tuner{
		config: config,
	}
}

func (t *Tuner) Name() string {
	return t.config.Name
}

func (t *Tuner) Groups() []string {
	groups := map[string]struct{}{}
	for _, group := range t.config.Types {
		groups[group] = struct{}{}
	}

	groupList := make([]string, 0, len(groups))
	for group := range groups {
		groupList = append(groupList, group)
	}
	return groupList
}

func (t *Tuner) IsDisabled() bool {
	return t.config.IsDisabled
}

func (t *Tuner) Command() string {
	return t.config.Command
}

func (t *Tuner) SourceKind() SourceKind {
	switch {
	case t.config.DvbDevicePath != "":
		return SourceKindDVB
	case t.config.Command != "":
		return SourceKindCommand
	case t.config.Remote != nil:
		return SourceKindRemote
	default:
		return SourceKindUnsupported
	}
}

func (t *Tuner) Usable() bool {
	switch t.SourceKind() {
	case SourceKindCommand, SourceKindDVB:
		return true
	default:
		return false
	}
}

func (t *Tuner) IsRemote() bool {
	return t.config.Remote != nil
}

func (t *Tuner) DecoderCommand() string {
	return t.config.Decoder
}

func (t *Tuner) NewDevice(channel *config.ChannelConfig) Device {
	switch t.SourceKind() {
	case SourceKindDVB:
		return NewDVBDevice(channel, t.config.Command, t.config.DvbDevicePath)
	case SourceKindCommand:
		return NewCommandDevice(channel, t.config.Command)
	default:
		return nil
	}
}
