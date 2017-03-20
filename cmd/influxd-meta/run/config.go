package run

import (
	"log"

	"github.com/BurntSushi/toml"
	"github.com/zhexuany/influxdb-cluster/meta"
)

const (
	// DefaultBindAddress is the default address for various RPC services.
	DefaultBindAddress = ":8088"
)

func ParseConfig(path string) (*meta.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		log.Println("no configuration provided, using default settings")
		return meta.NewDemoConfig(), nil
	}

	log.Printf("Using configuration at: %s\n", path)

	config := meta.NewConfig()
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil
}
