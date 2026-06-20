package tuner

import (
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
)

func TestReplaceCommandTemplateMirakurunCompatibilityAliases(t *testing.T) {
	channel := &config.ChannelConfig{
		Type:        "BS",
		Channel:     "101",
		CommandVars: map[string]any{"satellite": "JCSAT3A", "space": uint8(1)},
	}

	got := replaceCommandTemplate("tuner <satellite> <satelite> <space> <missing>", channel)
	if want := "tuner JCSAT3A JCSAT3A 1 "; got != want {
		t.Fatalf("replaceCommandTemplate() = %q, want %q", got, want)
	}

	got = replaceCommandTemplate("tuner <space> <satelite>", &config.ChannelConfig{CommandVars: map[string]any{}})
	if want := "tuner 0 "; got != want {
		t.Fatalf("replaceCommandTemplate() default = %q, want %q", got, want)
	}
}
