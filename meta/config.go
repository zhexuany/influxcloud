package meta

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultEnabled is the default state for the meta service to run
	DefaultEnabled = true

	// DefaultHostname is the default hostname if one is not provided.
	DefaultHostname = "localhost"

	// DefaultRaftBindAddress is the default address to bind to.
	DefaultRaftBindAddress = ":8088"

	// DefaultHTTPBindAddress is the default address to bind the API to.
	DefaultHTTPBindAddress = ":8091"

	// DefaultHeartbeatTimeout is the default heartbeat timeout for the store.
	DefaultHeartbeatTimeout = 1000 * time.Millisecond

	// DefaultElectionTimeout is the default election timeout for the store.
	DefaultElectionTimeout = 1000 * time.Millisecond

	DefaultGossipFrequency = 5 * 1000 * time.Millisecond

	DefaultAnnouncementExpiration = 30 * 1000 * time.Millisecond

	// DefaultLeaderLeaseTimeout is the default leader lease for the store.
	DefaultLeaderLeaseTimeout = 500 * time.Millisecond

	// DefaultCommitTimeout is the default commit timeout for the store.
	DefaultCommitTimeout = 50 * time.Millisecond

	// DefaultRaftPromotionEnabled is the default for auto promoting a node to a raft node when needed
	DefaultRaftPromotionEnabled = true

	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second

	// DefaultLoggingEnabled determines if log messages are printed for the meta service
	DefaultLoggingEnabled = true
)

// Config represents the meta configuration.
type Config struct {
	Meta *MetaConfig `toml:"meta"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	meta := &MetaConfig{
		Enabled:                true, // enabled by default
		BindAddress:            DefaultRaftBindAddress,
		HTTPBindAddress:        DefaultHTTPBindAddress,
		RetentionAutoCreate:    true,
		Gossipfrequency:        toml.Duration(DefaultGossipFrequency),
		Announcementexpiration: toml.Duration(DefaultAnnouncementExpiration),
		ElectionTimeout:        toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:       toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout:     toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:          toml.Duration(DefaultCommitTimeout),
		RaftPromotionEnabled:   DefaultRaftPromotionEnabled,
		LeaseDuration:          toml.Duration(DefaultLeaseDuration),
		LoggingEnabled:         DefaultLoggingEnabled,
	}
	return &Config{
		Meta: meta,
	}
}

func NewDemoConfig() *Config {
	c := NewConfig()
	// c.InitTableAttrs()

	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	c.Meta.Dir = filepath.Join(homeDir, ".influxdb/meta")
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		return nil
	}

	// The following lines should add to data node instead of meta node TODO
	// c.Data.Dir = filepath.Join(homeDir, ".influxdb/data")
	// c.HintedHandoff.Dir = filepath.Join(homeDir, ".influxdb/hh")
	// c.Data.WALDir = filepath.Join(homeDir, ".influxdb/wal")
	// c.HintedHandoff.Enabled = true
	// c.Meta.
	// c.Admin.Enabled = true

	return c

}

func (c *Config) Validate() error {
	return c.Meta.Validate()
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *Config) ApplyEnvOverrides() error {
	return c.applyEnvOverrides("INFLUXDB", reflect.ValueOf(c))
}

func (c *Config) applyEnvOverrides(prefix string, spec reflect.Value) error {
	// If we have a pointer, dereference it
	s := spec
	if spec.Kind() == reflect.Ptr {
		s = spec.Elem()
	}

	// Make sure we have struct
	if s.Kind() != reflect.Struct {
		return nil
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		// Get the toml tag to determine what env var name to use
		configName := typeOfSpec.Field(i).Tag.Get("toml")
		// Replace hyphens with underscores to avoid issues with shells
		configName = strings.Replace(configName, "-", "_", -1)
		fieldKey := typeOfSpec.Field(i).Name

		// Skip any fields that we cannot set
		if f.CanSet() || f.Kind() == reflect.Slice {

			// Use the upper-case prefix and toml name for the env var
			key := strings.ToUpper(configName)
			if prefix != "" {
				key = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
			}
			value := os.Getenv(key)

			// If the type is s slice, apply to each using the index as a suffix
			// e.g. GRAPHITE_0
			if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
				for i := 0; i < f.Len(); i++ {
					if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), f.Index(i)); err != nil {
						return err
					}
				}
				continue
			}

			// If it's a sub-config, recursively apply
			if f.Kind() == reflect.Struct || f.Kind() == reflect.Ptr {
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
				continue
			}

			// Skip any fields we don't have a value to set
			if value == "" {
				continue
			}

			switch f.Kind() {
			case reflect.String:
				f.SetString(value)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

				var intValue int64

				// Handle toml.Duration
				if f.Type().Name() == "Duration" {
					dur, err := time.ParseDuration(value)
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
					}
					intValue = dur.Nanoseconds()
				} else {
					var err error
					intValue, err = strconv.ParseInt(value, 0, f.Type().Bits())
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
					}
				}

				f.SetInt(intValue)
			case reflect.Bool:
				boolValue, err := strconv.ParseBool(value)
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

				}
				f.SetBool(boolValue)
			case reflect.Float32, reflect.Float64:
				floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

				}
				f.SetFloat(floatValue)
			default:
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *Config) defaultHost(addr string) string {
	address, err := DefaultHost(DefaultHostname, addr)
	if nil != err {
		return addr
	}
	return address
}

func DefaultHost(hostname, addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort(hostname, port), nil
	}
	return addr, nil
}

type MetaConfig struct {
	Enabled bool   `toml:"enabled"`
	Dir     string `toml:"dir"`
	// RemoteHostname is the hostname portion to use when registering meta node
	// addresses.  This hostname must be resolvable from other nodes.
	RemoteHostname string `toml:"-"`

	// this is deprecated. Should use the address from run/config.go
	BindAddress string `toml:"bind-address"`

	// HTTPBindAddress is the bind address for the metaservice HTTP API
	HTTPBindAddress  string `toml:"http-bind-address"`
	HTTPSEnabled     bool   `toml:"https-enabled"`
	HTTPSCertificate string `toml:"https-certificate"`

	RetentionAutoCreate    bool          `toml:"retention-autocreate"`
	Gossipfrequency        toml.Duration `toml:"gossip-frequency"`
	Announcementexpiration toml.Duration `toml:"announcement-expiration"`
	ElectionTimeout        toml.Duration `toml:"election-timeout"`
	HeartbeatTimeout       toml.Duration `toml:"heartbeat-timeout"`
	LeaderLeaseTimeout     toml.Duration `toml:"leader-lease-timeout"`
	CommitTimeout          toml.Duration `toml:"commit-timeout"`
	ClusterTracing         bool          `toml:"cluster-tracing"`
	RaftPromotionEnabled   bool          `toml:"raft-promotion-enabled"`
	LoggingEnabled         bool          `toml:"logging-enabled"`
	PprofEnabled           bool          `toml:"pprof-enabled"`

	LeaseDuration toml.Duration `toml:"lease-duration"`
}

//TODO add more criterias
func (m *MetaConfig) Validate() error {
	if m.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
