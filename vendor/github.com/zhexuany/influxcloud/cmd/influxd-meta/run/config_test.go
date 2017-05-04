package run_test

import (
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/cmd/influxd/run"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if err := c.FromToml(`
[meta]
dir = "/tmp/meta"

`); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Parse_EnvOverride(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
[meta]
dir = "/tmp/meta"
`, &c); err != nil {
		t.Fatal(err)
	}

}

func TestConfig_ValidateNoServiceConfigured(t *testing.T) {
	var c run.Config
	if _, err := toml.Decode(`
[meta]
enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}

	if e := c.Validate(); e == nil {
		t.Fatalf("got nil, expected error")
	}
}
