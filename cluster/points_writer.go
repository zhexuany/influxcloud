package cluster

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"strings"
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrPartialWrite is returned when a write partially succeeds but does
	// not meet the requested consistency level.
	ErrPartialWrite = errors.New("partial write")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")
)

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration
	Logger       *log.Logger

	Node *influxdb.Node

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
		ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo)
	}

	TSDBStore interface {
		CreateShard(database, retentionPolicy string, shardID uint64) error
		WriteToShard(shardID uint64, points []models.Point) error
	}

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}

	HintedHandoff interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}
}

// WritePointsRequest represents a request to write point data to the cluster.
type WritePointsRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, models.NewTags(tags), map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter() *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: DefaultWriteTimeout,
		Logger:       log.New(os.Stderr, "[write] ", log.LstdFlags),
	}
}

// ShardMapping contains a mapping of a shards to a points.
type ShardMapping struct {
	Points map[uint64][]models.Point  // The points associated with a shard ID
	Shards map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
}

// NewShardMapping creates an empty ShardMapping
func NewShardMapping() *ShardMapping {
	return &ShardMapping{
		Points: map[uint64][]models.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint maps a point to shard
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p models.Point) {
	points, ok := s.Points[shardInfo.ID]
	if !ok {
		s.Points[shardInfo.ID] = []models.Point{p}
	} else {
		s.Points[shardInfo.ID] = append(points, p)
	}
	s.Shards[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer
func (w *PointsWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closing = make(chan struct{})
	return nil
}

// Close closes the communication channel with the point writer
func (w *PointsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != nil {
		close(w.closing)
	}
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (w *PointsWriter) SetLogOutput(lw io.Writer) {
	w.Logger = log.New(lw, "[write] ", log.LstdFlags)
}

// WriteStatistics keeps statistics related to the PointsWriter.
type WriteStatistics struct {
	WriteReq            int64
	PointWriteReq       int64
	PointWriteReqLocal  int64
	PointWriteReqRemote int64
	PointWriteReqHH     int64
	WriteOK             int64
	WriteDropped        int64
	WriteTimeout        int64
	WritePartial        int64
	WriteErr            int64
	SubWriteOK          int64
	SubWriteDrop        int64
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (w *PointsWriter) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {
	rp, err := w.MetaClient.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	}
	if rp == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(wp.RetentionPolicy)
	}

	// Holds all the shard groups and shards that are required for writes.
	list := new(sgList)
	for _, p := range wp.Points {
		if !list.Covers(p.Time()) {
			// sg, err := w.MetaClient.CreateShardGroup(wp.Database, wp.RetentionPolicy, p.Time())
			// if err != nil {
			// 	return nil, err
			// }
			// list = list.Add(*sg)
		}
	}

	mapping := NewShardMapping()
	for _, p := range wp.Points {
		sg := list.ShardGroupAt(p.Time())
		if sg != nil {
			si := sg.ShardFor(p.HashID())
			mapping.MapPoint(&si, p)
		}
	}
	return mapping, nil
}

// sgList is a wrapper around a meta.ShardGroupInfos where we can also check
// if a given time is covered by any of the shard groups in the list.
// In addition, it also implements sort interface in order to sort after
// each Add operation. This can improve the efficency of searching a given
// ShardGroupInfo.
type sgList meta.ShardGroupInfos

func (l sgList) Len() int      { return len(l) }
func (l sgList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l sgList) Less(i, j int) bool {
	if l[i].EndTime.Equal(l[j].EndTime) {
		return true
	}
	return false
}

func (l sgList) Covers(t time.Time) bool {
	return len(l) != 0 && l.ShardGroupAt(t) != nil
}

func (l sgList) ShardGroupAt(t time.Time) *meta.ShardGroupInfo {
	if len(l) == 0 {
		return nil
	}
	// Attempt to find a shard group that could contain this point.
	// Shard groups are already sorted first according to end time.
	idx := sort.Search(len(l), func(i int) bool { return l[i].EndTime.After(t) })
	// We couldn't find a shard group the point falls into.
	if idx == len(l) || t.Before(l[idx].StartTime) {
		return nil
	}

	return &l[idx]
}

func (l *sgList) Add(sgi meta.ShardGroupInfo) *sgList {
	next := append(*l, sgi)
	sort.Sort(next)
	return &next
}

// WritePointsInto is a copy of WritePoints that uses a tsdb structure instead of
// a cluster structure for information. This is to avoid a circular dependency
func (w *PointsWriter) WritePointsInto(p *coordinator.IntoWriteRequest) error {
	return w.WritePoints(p.Database, p.RetentionPolicy, models.ConsistencyLevelOne, p.Points)
}

// WritePoints writes across multiple local and remote data nodes according the consistency level.
func (w *PointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {

	if retentionPolicy == "" {
		db := w.MetaClient.Database(database)
		if db == nil {
			return influxdb.ErrDatabaseNotFound(database)
		}
		retentionPolicy = db.DefaultRetentionPolicy
	}

	shardMappings, err := w.MapShards(&WritePointsRequest{Database: database, RetentionPolicy: retentionPolicy, Points: points})
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon
	// as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) {
			ch <- w.writeToShard(shard, database, retentionPolicy, consistencyLevel, points)
		}(shardMappings.Shards[shardID], database, retentionPolicy, points)
	}

	for range shardMappings.Points {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// writeToShards writes points to a shard.
func (w *PointsWriter) writeToShard(shard *meta.ShardInfo, database, retentionPolicy string,
	consistency models.ConsistencyLevel, points []models.Point) error {
	required := len(shard.Owners)
	switch consistency {
	case models.ConsistencyLevelAny, models.ConsistencyLevelOne:
		required = 1
	case models.ConsistencyLevelQuorum:
		required = required/2 + 1
	}

	// AsyncWriteResult is a struct that can be used
	// to determine the status of each PointWriteRequest
	type AsyncWriteResult struct {
		Owner meta.ShardOwner
		Err   error
	}

	// response channel for each shard writer go routine
	ch := make(chan *AsyncWriteResult, len(shard.Owners))

	for _, owner := range shard.Owners {
		go func(shardID uint64, owner meta.ShardOwner, points []models.Point) {
			if w.Node.ID != owner.NodeID {
				w.Logger.Printf("Remote Write")
				return
			}
			// not actually created this shard, tell it to create it and retry the write
			err := w.TSDBStore.WriteToShard(shardID, points)
			if err != nil {
				w.Logger.Printf("failed to write point to shard locally: %v", err)
			}
			ch <- &AsyncWriteResult{owner, err}
			return
		}(shard.ID, owner, points)

		// Start to write Shard into remote nodes
		go func(shardID uint64, owner meta.ShardOwner, points []models.Point) {
			if w.Node.ID != owner.NodeID {

				err := w.ShardWriter.WriteShard(shardID, owner.NodeID, points)
				if err != nil && isRetryable(err) {
					// The remote write failed so queue it via hinted handoff
					hherr := w.HintedHandoff.WriteShard(shardID, owner.NodeID, points)
					if hherr != nil {
						ch <- &AsyncWriteResult{owner, hherr}
						return
					}

					// If the write consistency level is ANY, then a successful hinted handoff can
					// be considered a successful write so send nil to the response channel
					// otherwise, let the original error propagate to the response channel
					if hherr == nil && consistency == models.ConsistencyLevelAny {
						ch <- &AsyncWriteResult{owner, nil}
						return
					}
				}
				ch <- &AsyncWriteResult{owner, err}
			}
		}(shard.ID, owner, points)

	}

	var wrote int
	timeout := time.After(w.WriteTimeout)
	var writeError error
	for range shard.Owners {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case result := <-ch:
			// If the write returned an error, continue to the next response
			if result.Err != nil {
				w.Logger.Printf("write failed for shard %d on node %d: %v", shard.ID, result.Owner.NodeID, result.Err)

				// Keep track of the first error we see to return back to the client
				if writeError == nil {
					writeError = result.Err
				}
				continue
			}

			wrote++

			// We wrote the required consistency level
			if wrote >= required {
				return nil
			}
		}
	}

	if wrote > 0 {
		return ErrPartialWrite
	}

	if writeError != nil {
		return fmt.Errorf("write failed: %v", writeError)
	}

	return ErrWriteFailed
}

func isRetryable(err error) bool {
	if err == nil {
		return true
	}

	if strings.Contains(err.Error(), "field type conflict") {
		return false
	}
	return true
}
