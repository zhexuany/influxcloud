package cluster

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultDialTimeout is the default timeout for a complete dial to succeed.
	DefaultDialTimeout = 1 * time.Second

	// DefaultShardWriterTimeout is the default timeout set on shard writers.
	DefaultShardWriterTimeout = 5 * time.Second

	// DefaultShardReaderTimeout is the default timeout set on shard writers.
	DefaultShardReaderTimeout = 5 * time.Second

	// DefaultMaxRemoteWriteConnections is the maximum number of open connections
	// that will be available for remote writes to another host.
	DefaultMaxRemoteWriteConnections = 3

	// DefaultClusterTracing enables traceing cluster info if it is true
	DefaultClusterTracing = false

	// DefaultWriteTimeout is the default timeout for a complete write to succeed.
	DefaultWriteTimeout = 5 * time.Second

	// DefaultMaxConcurrentQueries is the maximum number of running queries.
	// A value of zero will make the maximum query limit unlimited.
	DefaultMaxConcurrentQueries = 0

	// DefaultMaxSelectPointN is the maximum number of points a SELECT can process.
	// A value of zero will make the maximum point count unlimited.
	DefaultMaxSelectPointN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectSeriesN = 0

	// DefaultMaxSelectSeriesN is the maximum number of series a SELECT can run.
	// A value of zero will make the maximum series count unlimited.
	DefaultMaxSelectBucketsN = 0
)

// Config represents the configuration for the clustering service.
type Config struct {
	DialTimeout               toml.Duration `toml:"dial-timeout"`
	ShardWriterTimeout        toml.Duration `toml:"shard-writer-timeout"`
	ShardReaderTimeout        toml.Duration `toml:"shard-reader-timeout"`
	MaxRemoteWriteConnections int           `toml:"max-remote-write-connections"`
	ClusterTracing            bool          `toml:"cluster-tracing`
	WriteTimeout              toml.Duration `toml:"write-timeout"`
	MaxConcurrentQueries      int           `toml:"max-concurrent-queries"`
	QueryTimeout              toml.Duration `toml:"query-timeout"`
	LogQueriesAfter           toml.Duration `toml:"log-queries-after"`
	MaxSelectPointN           int           `toml:"max-select-point"`
	MaxSelectSeriesN          int           `toml:"max-select-series"`
	MaxSelectBucketsN         int           `toml:"max-select-buckets"`
}

// NewConfig returns an instance of Config with defaults.
func NewConfig() Config {
	return Config{
		DialTimeout:               toml.Duration(DefaultDialTimeout),
		ShardWriterTimeout:        toml.Duration(DefaultShardWriterTimeout),
		ShardReaderTimeout:        toml.Duration(DefaultShardreaderTimeout),
		MaxRemoteWriteConnections: DefaultMaxRemoteWriteConnections,
		ClusterTracing:            DefaultClusterTracing,
		WriteTimeout:              toml.Duration(DefaultWriteTimeout),
		MaxConcurrentQueries:      DefaultMaxConcurrentQueries,
		QueryTimeout:              toml.Duration(influxql.DefaultQueryTimeout),
		MaxSelectPointN:           DefaultMaxSelectPointN,
		MaxSelectSeriesN:          DefaultMaxSelectSeriesN,
		MaxSelectBucketsN:         DefaultMaxSelectBucketsN,
	}
}
