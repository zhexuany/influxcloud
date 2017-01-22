package run

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tcp"
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
	Tags    string
}

func (bi *BuildInfo) String() string {
	return fmt.Sprintf("Version %s, Commit %s, Branch %s, Tags %s",
		bi.Version, bi.Commit, bi.Branch, bi.Tags)
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

	Logger *log.Logger

	MetaClient *meta.Client

	Service *meta.Service

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	config *meta.Config

	// logOutput is the writer to which all services should be configured to
	// write logs to after appension.
	logOutput io.Writer
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *meta.Config, buildInfo *BuildInfo) (*Server, error) {
	// We need to ensure that a meta directory always exists even if
	// we don't start the meta store.  node.json is always stored under
	// the meta directory.

	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// 0.10-rc1 and prior would sometimes put the node.json at the root
	// dir which breaks backup/restore and restarting nodes.  This moves
	// the file from the root so it's always under the meta dir.
	oldPath := filepath.Join(filepath.Dir(c.Meta.Dir), "node.json")
	newPath := filepath.Join(c.Meta.Dir, "node.json")

	//check oldpath is existed or not, if yes rename oldpath with newpath
	if _, err := os.Stat(oldPath); err == nil {
		if err := os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
	}

	// load node from node.json and check the error
	node, err := influxdb.LoadNode(c.Meta.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
	}

	//LoadNode will just pasrse node.json file and create a instance
	//node. So, node.json wiill not be changed util we trigger from program
	//Hence, we have to check path in original node.json include the newPath
	//instead of oldPath. If not, we have to save such node instance to
	//node.json file
	if buf, err := ioutil.ReadFile(filepath.Join(newPath)); err != nil {
		if !strings.Contains(string(buf), "path") {
			node.Save()
		}
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.Meta.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Logger: log.New(os.Stderr, "", log.LstdFlags),

		MetaClient: meta.NewClient(),

		Service: meta.NewService(c.Meta),

		httpAPIAddr: c.Meta.HTTPBindAddress,

		config:    c,
		logOutput: os.Stderr,
	}

	return s, nil
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) {
	s.Logger = log.New(os.Stderr, "", log.LstdFlags)
	s.logOutput = w
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta services
func (s *Server) Open() error {
	// Start profiling, if set.
	startProfile(s.CPUProfile, s.MemProfile)

	log.Println("Opening Server for meta service")
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	//initializes metaClient
	s.initializeMetaClient()
	// Multiplex listener.
	mux := tcp.NewMux()
	// go mux.Listen(byte())
	go mux.Serve(ln)
	if err := s.MetaClient.Open(); err != nil {
		return err
	}

	s.HTTPAddr()
	if err := s.Service.Open(); err != nil {
		return err
	}

	s.Service.Err()
	return nil
}

//TODO need revist this
func (s *Server) initializeMetaClient() {
	s.MetaClient.SetMetaServers(nil)
	s.MetaClient.SetTLS(s.config.Meta.HTTPSEnabled)
	if s.MetaClient.HTTPClient != nil {
		s.MetaClient.SetHTTPClient(nil)
	}
	s.MetaClient.SetAuthInfo(s.config.Meta.RemoteHostname)
	s.MetaClient.Open()
	s.MetaClient.WaitForDataChanged()
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	s.MetaClient.Close()

	s.Service.Close()

	close(s.closing)
	return nil
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
	// dis, _ := s.MetaClient.Databases()
	// numDatabases := len(dis)

	// numMeasurements := 0
	// numSeries := 0

	// clusterID := s.MetaClient.ClusterID()
	// usage := client.Usage{
	// 	Product: "influxdb",
	// 	Data: []client.UsageData{
	// 		{
	// 			Values: client.Values{
	// 				"os":               runtime.GOOS,
	// 				"arch":             runtime.GOARCH,
	// 				"version":          s.buildInfo.Version,
	// 				"cluster_id":       fmt.Sprintf("%v", clusterID),
	// 				"num_series":       numSeries,
	// 				"num_measurements": numMeasurements,
	// 				"num_databases":    numDatabases,
	// 				"uptime":           time.Since(startTime).Seconds(),
	// 			},
	// 		},
	// 	},
	// }

	s.Logger.Printf("Sending usage statistics to usage.influxdata.com")

	// go cl.Save(usage)
}

// monitorErrorChan reads an error channel and resends it through the server.
func (s *Server) monitorErrorChan(ch <-chan error) {
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			s.err <- err
		case <-s.closing:
			return
		}
	}
}

func (s *Server) HTTPAddr() string {
	return s.httpAPIAddr
}

// Service represents a service attached to the server.
type Service interface {
	SetLogOutput(w io.Writer)
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
