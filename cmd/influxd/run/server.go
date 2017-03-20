package run

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/admin"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	"go.uber.org/zap"
	// Initialize the engine packages
	_ "github.com/influxdata/influxdb/tsdb/engine"
	"github.com/zhexuany/influxdb-cluster/cluster"
	"github.com/zhexuany/influxdb-cluster/meta"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string
	Listener    net.Listener

	Logger zap.Logger

	MetaClient *meta.Client

	TSDBStore     *tsdb.Store
	QueryExecutor *influxql.QueryExecutor
	PointsWriter  *cluster.PointsWriter
	Subscriber    *subscriber.Service

	Services []Service

	// These references are required for the tcp muxer.
	ClusterService *cluster.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// We need to ensure that a data directory always exists even if
	// we don't start the data store.
	if err := os.MkdirAll(c.Data.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}
	bind := c.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Logger: zap.New(
			zap.NewTextEncoder(),
			zap.Output(os.Stderr),
		),

		MetaClient: meta.NewClient(),

		reportingDisabled: c.ReportingDisabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config: c,
	}
	s.MetaClient.SetTLS(c.Meta.MetaTLSEnabled)
	s.MetaClient.SetPath(c.Meta.Dir)

	s.Monitor = monitor.New(s, c.Monitor)

	s.TSDBStore = tsdb.NewStore(c.Data.Dir)
	s.TSDBStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine

	// Create the Subscriber service
	s.Subscriber = subscriber.NewService(c.Subscriber)

	// Initialize points writer.
	s.PointsWriter = cluster.NewPointsWriter()
	s.PointsWriter.WriteTimeout = time.Duration(c.Coordinator.WriteTimeout)
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.Subscriber = s.Subscriber

	// Initialize query executor.
	s.QueryExecutor = influxql.NewQueryExecutor()
	s.QueryExecutor.StatementExecutor = &cluster.StatementExecutor{
		MetaClient:        s.MetaClient,
		TaskManager:       s.QueryExecutor.TaskManager,
		TSDBStore:         coordinator.LocalTSDBStore{Store: s.TSDBStore},
		Monitor:           s.Monitor,
		PointsWriter:      s.PointsWriter,
		MaxSelectPointN:   c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:  c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN: c.Coordinator.MaxSelectBucketsN,
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries

	// Initialize the monitor
	s.Monitor.Version = s.buildInfo.Version
	s.Monitor.Commit = s.buildInfo.Commit
	s.Monitor.Branch = s.buildInfo.Branch
	s.Monitor.BuildTime = s.buildInfo.Time
	s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s, nil
}

func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.Subscriber.Statistics(tags)...)
	for _, srv := range s.Services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	return statistics
}

// clusterWriter is a wrapper around `cluster.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type clusterWriter cluster.PointsWriter

func (w *clusterWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	return w.WritePoints(database, retentionPolicy, points)
}

func (s *Server) appendClusterService(c cluster.Config) {
	srv := cluster.NewService(c)
	s.Services = append(s.Services, srv)
	s.ClusterService = srv
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) {
	s.Logger = zap.New(zap.NewTextEncoder(), zap.Output(zap.AddSync(w)))
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
func (s *Server) Open() error {
	// Start profiling, if set.
	startProfile(s.CPUProfile, s.MemProfile)

	// Open metaclient
	if err := s.MetaClient.Open(); err != nil {
		return fmt.Errorf("failed to open metaclient %v", err)
	}

	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.

	// Append services.
	s.appendClusterService(s.config.Cluster)
	s.appendHTTPDService(s.config.HTTPD)
	for _, i := range s.config.GraphiteInputs {
		if err := s.appendGraphiteService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.CollectdInputs {
		s.appendCollectdService(i)
	}
	for _, i := range s.config.OpenTSDBInputs {
		if err := s.appendOpenTSDBService(i); err != nil {
			return err
		}
	}
	for _, i := range s.config.UDPInputs {
		s.appendUDPService(i)
	}

	ni, err := s.MetaClient.DataNodeByHTTPHost(s.remoteAddr(addr))
	if err != nil {
		return err
	}

	mux := tcp.NewMux()
	// mux.Listen(header byte)
	// go mux.Serve(ln)
	// s.appendMonitorService()
	// s.appendPrecreatorService(s.config.Precreator)
	// s.appendSnapshotterService()
	// s.appendAdminService(s.config.Admin)
	// s.appendContinuousQueryService(s.config.ContinuousQuery)
	// s.appendRetentionPolicyService(s.config.Retention)
	s.Subscriber.MetaClient = s.MetaClient
	s.Subscriber.MetaClient = s.MetaClient
	s.PointsWriter.MetaClient = s.MetaClient
	s.Monitor.MetaClient = s.MetaClient

	s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)

	// Configure logging for all services and clients.
	if s.config.Meta.LoggingEnabled {
		s.MetaClient.WithLogger(s.Logger)
	}
	s.TSDBStore.WithLogger(s.Logger)
	if s.config.Data.QueryLogEnabled {
		s.QueryExecutor.WithLogger(s.Logger)
	}
	s.PointsWriter.WithLogger(s.Logger)
	s.Subscriber.WithLogger(s.Logger)
	for _, svc := range s.Services {
		svc.WithLogger(s.Logger)
	}
	s.SnapshotterService.WithLogger(s.Logger)
	s.Monitor.WithLogger(s.Logger)

	// Open TSDB store.
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the subcriber service
	if err := s.Subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	// Open the points writer service
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
		}
	}

	//TODO open hinted handoff
	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}

	return nil
}

func (s *Server) setCoreEntitleMents() {

}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	if s.Monitor != nil {
		s.Monitor.Close()
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.QueryExecutor != nil {
		s.QueryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	if s.MetaClient != nil {
		s.MetaClient.Close()
	}

	//TODO handler setNodeStatus
	close(s.closing)
	return nil
}

func (s *Server) reset() {

}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

// reportServer reports usage statistics about the system.
func (s *Server) reportServer() {
	dis := s.MetaClient.Databases()
	numDatabases := len(dis)

	numMeasurements := 0
	numSeries := 0

	// Only needed in the case of a data node
	if s.TSDBStore != nil {
		for _, di := range dis {
			d := s.TSDBStore.DatabaseIndex(di.Name)
			if d == nil {
				// No data in this store for this database.
				continue
			}
			m, s := d.MeasurementSeriesCounts()
			numMeasurements += m
			numSeries += s
		}
	}

	clusterID := s.MetaClient.ClusterID()
	cl := client.New("")
	usage := client.Usage{
		Product: "influxdb",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	s.Logger.Info("Sending usage statistics to usage.influxdata.com")

	go cl.Save(usage)
}

// HTTPAddr returns the HTTP address used by other nodes for HTTP queries and writes.
func (s *Server) HTTPAddr() string {
	return s.remoteAddr(s.httpAPIAddr)
}

// TCPAddr returns the TCP address used by other nodes for cluster communication.
func (s *Server) TCPAddr() string {
	return s.remoteAddr(s.tcpAddr)
}

// MetaServers returns the meta node HTTP addresses used by this server.
func (s *Server) MetaServers() []string {
	return []string{s.HTTPAddr()}
}

// Service represents a service attached to the server.
type Service interface {
	WithLogger(log zap.Logger)
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

type tcpaddr struct{ host string }

func (a *tcpaddr) Network() string { return "tcp" }
func (a *tcpaddr) String() string  { return a.host }

func (s *Server) remoteAddr(addr string) string {
	hostname := s.config.Hostname
	remote, err := DefaultHost(hostname, addr)
	if err != nil {
		return addr
	}
	return remote
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
