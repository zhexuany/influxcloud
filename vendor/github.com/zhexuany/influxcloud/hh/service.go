package hh // import "github.com/zhexuany/influxcloud/hh"

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/uber-go/zap"
)

// ErrHintedHandoffDisabled is returned when attempting to use a
// disabled hinted handoff service.
var ErrHintedHandoffDisabled = fmt.Errorf("hinted handoff disabled")

// Statistics for the Subscriber service.
const (
	statNodeProcessorCreated = "nodeProcessorCreated"
	statNodeProcessorOpened  = "nodeProcessorOpened"
)

// Service represents a hinted handoff service.
type Service struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closing chan struct{}

	processors map[uint64]*NodeProcessor

	defaultTags models.StatisticTags
	stats       *HHStatistics

	Logger zap.Logger
	cfg    Config

	shardWriter shardWriter
	MetaClient  metaClient

	Monitor interface {
		RegisterDiagnosticsClient(name string, client diagnostics.Client)
		DeregisterDiagnosticsClient(name string)
	}

	enabled bool
}

type shardWriter interface {
	WriteShard(shardID, ownerID uint64, points []models.Point) error
}

type metaClient interface {
	DataNode(id uint64) (ni *meta.NodeInfo, err error)
}

// NewService returns a new instance of Service.
func NewService(c Config, w shardWriter, m metaClient) *Service {
	//createing defaultTags
	tags := map[string]string{"path": c.Dir}

	s := &Service{
		enabled:     c.Enabled,
		cfg:         c,
		closing:     make(chan struct{}),
		processors:  make(map[uint64]*NodeProcessor),
		stats:       &HHStatistics{},
		Logger:      zap.New(zap.NullEncoder()),
		shardWriter: w,
		MetaClient:  m,
	}

	s.defaultTags = tags

	return s
}

// Open opens the hinted handoff service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		// Allow Open to proceed, but don't do anything.
		return nil
	}
	s.Logger.Info("Starting hinted handoff service")
	s.closing = make(chan struct{})

	// Register diagnostics if a Monitor service is available.
	if s.Monitor != nil {
		s.Monitor.RegisterDiagnosticsClient("hh", s)
	}

	// Create the root directory if it doesn't already exist.
	// s.Logger.Info("Using data dir: %v", s.cfg.Dir)
	if err := os.MkdirAll(s.cfg.Dir, 0700); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Create a node processor for each node directory.
	files, err := ioutil.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		nodeID, err := strconv.ParseUint(file.Name(), 10, 64)
		if err != nil {
			// Not a number? Skip it.
			continue
		}

		n := NewNodeProcessor(nodeID, s.pathforNode(nodeID), s.shardWriter, s.MetaClient, s.cfg)
		//Open newly created NodeProcessor
		if err := n.Open(); err != nil {
			return err
		}
		s.processors[nodeID] = n
	}

	s.wg.Add(1)
	go s.purgeInactiveProcessors()

	return nil
}

// Close closes the hinted handoff service.
func (s *Service) Close() error {
	s.Logger.Info("shutting down hh service")
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range s.processors {
		if err := p.Close(); err != nil {
			return err
		}
	}

	if s.Monitor != nil {
		s.Monitor.DeregisterDiagnosticsClient("hh")
	}

	if s.closing != nil {
		close(s.closing)
	}
	s.wg.Wait()
	s.closing = nil

	return nil
}

// WithLogger sets the internal logger to the logger passed in
func (s *Service) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "cluster"))
}

// HHStatistics keeps all statistcs realted with hinted handoff service
type HHStatistics struct {
	NodeProcessorCreated int64
	NodeProcessorOpened  int64
}

func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{{
		Name: "hh",
		Tags: s.defaultTags.Merge(tags),
		Values: map[string]interface{}{
			statNodeProcessorCreated: atomic.LoadInt64(&s.stats.NodeProcessorCreated),
			statNodeProcessorOpened:  atomic.LoadInt64(&s.stats.NodeProcessorOpened),
		},
	}}

	s.mu.RLock()
	for _, v := range s.processors {
		statistics = append(statistics, v.Statistics(tags)...)
	}
	s.mu.RUnlock()
	return statistics
}

func (s *Service) Empty(id uint64) bool {
	s.mu.RLock()
	processor := s.processors
	s.mu.RUnlock()

	// retrieve node processor by id
	np := processor[id]
	// determine such node processor is empty or not
	return np.Empty()
}

// WriteShard queues the points write for shardID to node ownerID to handoff queue
func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if !s.cfg.Enabled {
		return ErrHintedHandoffDisabled
	}

	s.mu.RLock()
	processor, ok := s.processors[ownerID]
	s.mu.RUnlock()
	if !ok {
		if err := func() error {
			// Check again under write-lock.
			s.mu.Lock()
			defer s.mu.Unlock()

			processor, ok = s.processors[ownerID]
			if !ok {
				processor = NewNodeProcessor(ownerID, s.pathforNode(ownerID), s.shardWriter, s.MetaClient, s.cfg)
				if err := processor.Open(); err != nil {
					return err
				}
				s.processors[ownerID] = processor
				atomic.AddInt64(&s.stats.NodeProcessorCreated, 1)
				atomic.AddInt64(&s.stats.NodeProcessorOpened, 1)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	if err := processor.WriteShard(shardID, points); err != nil {
		return err
	}

	return nil
}

// Diagnostics returns diagnostic information.
func (s *Service) Diagnostics() (*diagnostics.Diagnostics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := &diagnostics.Diagnostics{
		Columns: []string{"node", "active", "last modified", "head", "tail"},
		Rows:    make([][]interface{}, 0, len(s.processors)),
	}

	for k, v := range s.processors {
		lm, err := v.LastModified()
		if err != nil {
			return nil, err
		}

		active, err := v.Active()
		if err != nil {
			return nil, err
		}

		d.Rows = append(d.Rows, []interface{}{k, active, lm, v.Head(), v.Tail()})
	}
	return d, nil
}

// purgeInactiveProcessors will cause the service to remove processors for inactive nodes.
func (s *Service) purgeInactiveProcessors() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Duration(s.cfg.PurgeInterval))
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()

				for k, v := range s.processors {
					if v.Closed() {
						if err := s.remove(v); err != nil {
							s.Logger.Info("error removing node proce: " + err.Error())
							continue
						}
						// remove node processor from map
						delete(s.processors, k)
						// s.Logger.Info("removed node processor %d from map", k)
						continue
					}
					lm, err := v.LastModified()
					if err != nil {
						// s.Logger.Info("failed to determine LastModified for processor %d: %s", k, err.Error())
						continue
					}

					active, err := v.Active()
					if err != nil {
						if err := s.remove(v); err != nil {
							// s.Logger.Info("failed to determine if node %d is active: %s", k, err.Error())
							continue
						}
						// remove node processor from map
						delete(s.processors, k)
						// s.Logger.Info("removed node processor %d from map", k)//
						continue
					} else if active {
						// s.Logger.Info("node processor %d is active. Skiping it", k)
						// Node is active.
						continue
					}

					// NodeProcessor is not active, but this does not mean we can remove the data
					// in dir. It is necessary to ensure all data are old enough
					if !lm.Before(time.Now().Add(-time.Duration(s.cfg.MaxAge))) {
						// Node processor contains too-young data.
						continue
					}

					if err := s.remove(v); err != nil {
						// s.Logger.Info("error removing node proce: %v", err)
						continue
					}
					delete(s.processors, k)
				}
			}()
		}
	}

}

func (s *Service) remove(np *NodeProcessor) error {
	if err := np.Close(); err != nil {
		return fmt.Errorf("failed to close node processor: %v", err)
	}
	if err := np.Purge(); err != nil {
		return fmt.Errorf("failed to pruge node processor: %v", err)
	}
	return nil
}

// pathforNode returns the directory for HH data, for the given node.
func (s *Service) pathforNode(nodeID uint64) string {
	return filepath.Join(s.cfg.Dir, fmt.Sprintf("%d", nodeID))
}
