package meta

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	// "strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// Raft configuration.
const (
	raftLogCacheSize      = 512
	raftSnapshotsRetained = 2
	raftTransportMaxPool  = 3
	raftTransportTimeout  = 10 * time.Second
)

// raftState is a consensus strategy that uses a local raft implementation for
// consensus operations.
type raftState struct {
	wg        sync.WaitGroup
	config    *MetaConfig
	closing   chan struct{}
	raft      *raft.Raft
	transport *raft.NetworkTransport
	peerStore raft.PeerStore
	raftStore *raftboltdb.BoltStore
	raftLayer *raftLayer
	ln        net.Listener
	addr      string
	logger    *log.Logger
	path      string
}

//TODO finished this func for now this func is just copied from config.applyEnvOverrides
func loadConfigEnvOverrides(prefix string, spec reflect.Value) error {
	// If we have a pointer, dereference it
	// s := spec
	// if spec.Kind() == reflect.Ptr {
	// 	s = spec.Elem()
	// }

	// // Make sure we have struct
	// if s.Kind() != reflect.Struct {
	// 	return nil
	// }

	// typeOfSpec := s.Type()
	// for i := 0; i < s.NumField(); i++ {
	// 	f := s.Field(i)
	// 	// Get the toml tag to determine what env var name to use
	// 	configName := typeOfSpec.Field(i).Tag.Get("toml")
	// 	// Replace hyphens with underscores to avoid issues with shells
	// 	configName = strings.Replace(configName, "-", "_", -1)
	// 	fieldKey := typeOfSpec.Field(i).Name

	// 	// Skip any fields that we cannot set
	// 	if f.CanSet() || f.Kind() == reflect.Slice {

	// 		// Use the upper-case prefix and toml name for the env var
	// 		key := strings.ToUpper(configName)
	// 		if prefix != "" {
	// 			key = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
	// 		}
	// 		value := os.Getenv(key)

	// 		// If the type is s slice, apply to each using the index as a suffix
	// 		// e.g. GRAPHITE_0
	// 		if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
	// 			for i := 0; i < f.Len(); i++ {
	// 				if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), f.Index(i)); err != nil {
	// 					return err
	// 				}
	// 			}
	// 			continue
	// 		}

	// 		// If it's a sub-config, recursively apply
	// 		if f.Kind() == reflect.Struct || f.Kind() == reflect.Ptr {
	// 			if err := c.applyEnvOverrides(key, f); err != nil {
	// 				return err
	// 			}
	// 			continue
	// 		}

	// 		// Skip any fields we don't have a value to set
	// 		if value == "" {
	// 			continue
	// 		}

	// 		switch f.Kind() {
	// 		case reflect.String:
	// 			f.SetString(value)
	// 		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

	// 			var intValue int64

	// 			// Handle toml.Duration
	// 			if f.Type().Name() == "Duration" {
	// 				dur, err := time.ParseDuration(value)
	// 				if err != nil {
	// 					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
	// 				}
	// 				intValue = dur.Nanoseconds()
	// 			} else {
	// 				var err error
	// 				intValue, err = strconv.ParseInt(value, 0, f.Type().Bits())
	// 				if err != nil {
	// 					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
	// 				}
	// 			}

	// 			f.SetInt(intValue)
	// 		case reflect.Bool:
	// 			boolValue, err := strconv.ParseBool(value)
	// 			if err != nil {
	// 				return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

	// 			}
	// 			f.SetBool(boolValue)
	// 		case reflect.Float32, reflect.Float64:
	// 			floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
	// 			if err != nil {
	// 				return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

	// 			}
	// 			f.SetFloat(floatValue)
	// 		default:
	// 			if err := c.applyEnvOverrides(key, f); err != nil {
	// 				return err
	// 			}
	// 		}
	// 	}
	// }
	return nil
}

func newRaftState(c *MetaConfig, addr string) *raftState {
	return &raftState{
		config: c,
		addr:   addr,
	}
}

func (r *raftState) open(s *store, ln net.Listener) error {
	r.ln = ln
	r.closing = make(chan struct{})

	// Setup raft configuration.
	config := raft.DefaultConfig()
	config.LogOutput = ioutil.Discard

	if r.config.ClusterTracing {
		config.Logger = r.logger
	}

	//call loadConfigEnvOverrides
	config.HeartbeatTimeout = time.Duration(r.config.HeartbeatTimeout)
	config.ElectionTimeout = time.Duration(r.config.ElectionTimeout)
	config.LeaderLeaseTimeout = time.Duration(r.config.LeaderLeaseTimeout)
	config.CommitTimeout = time.Duration(r.config.CommitTimeout)
	// Since we actually never call `removePeer` this is safe.
	// If in the future we decide to call remove peer we have to re-evaluate how to handle this
	config.ShutdownOnRemove = false

	// Build raft layer to multiplex listener.
	r.raftLayer = newRaftLayer(r.addr, r.ln)

	// Create a transport layer
	r.transport = raft.NewNetworkTransport(r.raftLayer, 3, 10*time.Second, config.LogOutput)

	// Create peer storage.
	r.peerStore = raft.NewJSONPeers(r.path, r.transport)

	//check wheather node itself is in peerStore or not
	peers, _ := r.peers()
	if ok := raft.PeerContained(peers, r.addr); !ok {
		// remove node itself from peer store
		r.removePeer(r.addr)
	}

	//call PeerContained
	// Create the log store and stable store.
	store, err := raftboltdb.NewBoltStore(filepath.Join(r.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	r.raftStore = store

	// Create the snapshot store.
	snapshots, err := raft.NewFileSnapshotStore(r.path, raftSnapshotsRetained, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create raft log.
	ra, err := raft.NewRaft(config, (*storeFSM)(s), store, store, snapshots, r.peerStore, r.transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	r.raft = ra

	r.wg.Add(1)
	go r.logLeaderChanges()

	return nil
}

func (r *raftState) logLeaderChanges() {
	defer r.wg.Done()
	// Logs our current state (Node at 1.2.3.4:8088 [Follower])
	r.logger.Printf(r.raft.String())
	for {
		select {
		case <-r.closing:
			return
		case <-r.raft.LeaderCh():
			peers, err := r.peers()
			if err != nil {
				r.logger.Printf("failed to lookup peers: %v", err)
			}
			r.logger.Printf("%v. peers=%v", r.raft.String(), peers)
		}
	}
}

func (r *raftState) close() error {
	if r == nil {
		return nil
	}
	if r.closing != nil {
		close(r.closing)
	}
	r.wg.Wait()

	if r.transport != nil {
		r.transport.Close()
		r.transport = nil
	}

	// Shutdown raft.
	if r.raft != nil {
		if err := r.raft.Shutdown().Error(); err != nil {
			return err
		}
		r.raft = nil
	}

	if r.raftStore != nil {
		r.raftStore.Close()
		r.raftStore = nil
	}

	return nil
}

// apply applies a serialized command to the raft log.
func (r *raftState) apply(b []byte) error {
	// Apply to raft log.
	f := r.raft.Apply(b, 0)
	if err := f.Error(); err != nil {
		return err
	}

	// Return response if it's an error.
	// No other non-nil objects should be returned.
	resp := f.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	if resp != nil {
		panic(fmt.Sprintf("unexpected response: %#v", resp))
	}

	return nil
}

// addPeer adds addr to the list of peers in the cluster.
func (r *raftState) addPeer(addr string) error {
	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	for _, p := range peers {
		if addr == p {
			return nil
		}
	}

	if fut := r.raft.AddPeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

// removePeer removes addr from the list of peers in the cluster.
func (r *raftState) removePeer(addr string) error {
	// Only do this on the leader
	if !r.isLeader() {
		return raft.ErrNotLeader
	}

	peers, err := r.peerStore.Peers()
	if err != nil {
		return err
	}

	var exists bool
	for _, p := range peers {
		if addr == p {
			exists = true
			break
		}
	}

	if !exists {
		return nil
	}

	if fut := r.raft.RemovePeer(addr); fut.Error() != nil {
		return fut.Error()
	}
	return nil
}

func (r *raftState) peers() ([]string, error) {
	return r.peerStore.Peers()
}

func (r *raftState) leader() string {
	if r.raft == nil {
		return ""
	}

	return r.raft.Leader()
}

func (r *raftState) isLeader() bool {
	if r.raft == nil {
		return false
	}
	return r.raft.State() == raft.Leader
}

// raftLayer wraps the connection so it can be re-used for forwarding.
type raftLayer struct {
	addr   *raftLayerAddr
	ln     net.Listener
	conn   chan net.Conn
	closed chan struct{}
}

// Addr returns the local address for the layer.
func (l *raftLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new network connection.
func (l *raftLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	// Write a marker byte for raft messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

// Accept waits for the next connection.
func (l *raftLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *raftLayer) Close() error { return l.ln.Close() }

type raftLayerAddr struct {
	addr string
}

func (r *raftLayerAddr) Network() string {
	return "tcp"
}

func (r *raftLayerAddr) String() string {
	return r.addr
}

// newRaftLayer returns a new instance of raftLayer.
func newRaftLayer(addr string, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr:   &raftLayerAddr{addr},
		ln:     ln,
		conn:   make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

//TODO finished this this funcation called by loadConfigEnvOverrides
func split() {

}
