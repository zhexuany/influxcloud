package meta

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxdb-cluster/meta/internal"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/zhexuany/influxdb-cluster/rpc"
	"github.com/zhexuany/influxdb-cluster/tlv"
)

// Retention policy settings.
const (
	autoCreateRetentionPolicyName   = "default"
	autoCreateRetentionPolicyPeriod = 0

	// maxAutoCreatedRetentionPolicyReplicaN is the maximum replication factor that will
	// be set for auto-created retention policies.
	maxAutoCreatedRetentionPolicyReplicaN = 3
)

// Raft configuration.
const (
	raftListenerStartupTimeout = time.Second
)

type store struct {
	mu      sync.RWMutex
	closing chan struct{}

	config      *MetaConfig
	data        *Data
	raftState   *raftState
	dataChanged chan struct{}
	path        string
	opened      bool
	r           bool
	logger      *log.Logger

	raftAddr        string
	httpAddr        string
	copyShardStatus map[string]int64

	node *influxdb.Node

	raftLn net.Listener
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
}

// newStore will create a new metastore with the passed in config
func newStore(c *MetaConfig, httpAddr, raftAddr string) *store {
	s := store{
		data: &Data{
			Data: &meta.Data{
				Index: 1,
			},
		},
		closing:         make(chan struct{}),
		dataChanged:     make(chan struct{}),
		copyShardStatus: make(map[string]int64),
		path:            c.Dir,
		config:          c,
		httpAddr:        httpAddr,
		raftAddr:        raftAddr,
	}
	if c.LoggingEnabled {
		s.logger = log.New(os.Stderr, "[metastore] ", log.LstdFlags)
	} else {
		s.logger = log.New(ioutil.Discard, "", 0)
	}

	return &s
	//
}

// open opens and initializes the raft store.
func (s *store) open(raftln net.Listener) error {
	s.logger.Printf("Using data dir: %v", s.path)

	if err := s.setOpen(); err != nil {
		return err
	}

	// Create the root directory if it doesn't already exist.
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}

	// Start to open the raft store.
	if err := s.openRaft(raftln); err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	s.mu.Lock()
	s.r = false
	s.mu.Unlock()

	// Wait for a leader to be elected so we know the raft log is loaded
	// and up to date
	if s.raftState.raft != nil {
		if err := s.waitForLeader(0); err != nil {
			return fmt.Errorf("raft: %s", err)
		}

		s.mu.Lock()
		s.r = true
		s.mu.Unlock()

		// Already have a leader, now start to join cluster
		n := &NodeInfo{
			Host: s.raftAddr,
		}
		if _, err := s.join(n); err != nil {
			return fmt.Errorf("raft: %s", err)
		}
		s.logger.Printf("Raft is opened")
	}
	// If this is first meta node wanting to join cluster
	// Just set this meta node as leader, not need for selecting new leader
	if peers := s.peers(); len(peers) > 1 {
		// Since there mutiple nodes in cluster, We have to
		// select a new leader when a new meta node want to join
		if err := s.waitForLeader(time.Duration(s.config.ElectionTimeout)); err != nil {
			return err
		}
	}

	// Leader is ready, so does the cluster
	s.mu.Lock()
	s.r = true
	s.mu.Unlock()
	return nil
}

func (s *store) setOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if store has already been opened.
	if s.opened {
		return ErrStoreOpen
	}
	s.opened = true
	return nil
}

// peers returns the raft peers known to this store
func (s *store) peers() []string {
	if s.raftOpened() {
		return []string{s.raftAddr}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	peers, err := s.raftState.peers()
	if err != nil {
		return []string{s.raftAddr}
	}
	return peers
}

func (s *store) filterAddr(addrs []string, filter string) ([]string, error) {
	host, port, err := net.SplitHostPort(filter)
	if err != nil {
		return nil, err
	}

	ip, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return nil, err
	}

	var joinPeers []string
	for _, addr := range addrs {
		joinHost, joinPort, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		joinIp, err := net.ResolveIPAddr("ip", joinHost)
		if err != nil {
			return nil, err
		}

		// Don't allow joining ourselves
		if ip.String() == joinIp.String() && port == joinPort {
			continue
		}
		joinPeers = append(joinPeers, addr)
	}
	return joinPeers, nil
}

func (s *store) openRaft(raftln net.Listener) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rs := newRaftState(s.config, s.raftAddr)
	rs.logger = s.logger
	rs.path = s.path

	if err := rs.open(s, raftln); err != nil {
		return err
	}
	s.raftState = rs

	return nil
}

// raftOpened will return true is raftState is not nil
// otherise will return false.
func (s *store) raftOpened() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.raftState == nil
}

// ready will just return boolean ready back
func (s *store) ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.r
}

//
func (s *store) reset(st *store) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.raftState.close(); err != nil {
		return err
	}

	// // we have to remove all file in raft path
	// // directory, since we want to a reset operation.
	os.RemoveAll(filepath.Join(s.raftState.path, "/*"))
	os.Remove(filepath.Join(s.path, "raft.db"))

	// // reopen the strore after remove all files
	// if err := s.open(s.raftLn); err != nil {
	// 	return err
	// }
	// if s.raftState == nil {
	// 	return nil
	// }

	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	return nil
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closing:
		// already closed
		return nil
	default:
		//closing
		close(s.closing)
		return s.raftState.close()
	}
}

func (s *store) snapshot() (*Data, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Clone(), nil
}

func (s *store) setSnapshot(data *Data) error {
	dataB, err := data.MarshalBinary()
	if err != nil {
		return err
	}

	// Prepare proto command for snapshot
	val := &internal.SetDataCommand{Data: dataB}

	t := internal.Command_SetDataCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_SetDataCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return nil
	}

	return s.apply(b)
}

// afterIndex returns a channel that will be closed to signal
// the caller when an updated snapshot is available.
func (s *store) afterIndex(index uint64) <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.data.Data.Index {
		// Client needs update so return a closed channel.
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return s.dataChanged
}

//
//
//
//
//
//
//
//
//
func (s *store) applied(timeout time.Duration) error {
	return s.raftState.raft.Barrier(timeout).Error()
}

// WaitForLeader sleeps until a leader is found or a timeout occurs.
// timeout == 0 means to wait forever.
func (s *store) waitForLeader(timeout time.Duration) error {
	// Begin timeout timer.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Continually check for leader until timeout.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return errors.New("closing")
		case <-timer.C:
			if timeout != 0 {
				return errors.New("timeout")
			}
		case <-ticker.C:
			if s.leader() != "" {
				return nil
			}
		}
	}
}

// isLeader returns true if the store is currently the leader.
func (s *store) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return false
	}
	return s.raftState.raft.State() == raft.Leader
}

// leader returns what the store thinks is the current leader. An empty
// string indicates no leader exists.
func (s *store) leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil || s.raftState.raft == nil {
		return ""
	}
	return s.raftState.raft.Leader()
}

// leaderHTTP returns the HTTP API connection info for the metanode
// that is the raft leader
func (s *store) leaderHTTP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raftState == nil {
		return ""
	}
	l := s.raftState.raft.Leader()

	for _, n := range s.data.MetaNodes {
		if n.TCPHost == l {
			return n.Host
		}
	}

	return ""
}

// otherMetaServersHTTP will return the HTTP bind addresses of the other
// meta servers in the cluster
func (s *store) otherMetaServersHTTP() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var a []string
	for _, n := range s.data.MetaNodes {
		if n.TCPHost != s.raftAddr {
			a = append(a, n.Host)
		}
	}
	return a

}

// dataNode will return a data node info according to its id
func (s *store) dataNode(id uint64) *NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, n := range s.data.DataNodes {
		if n.ID == id {
			return &NodeInfo{
				ID:      n.ID,
				TCPHost: n.TCPHost,
				Host:    n.Host,
			}
		}
	}
	return nil
}

//
func (s *store) dataNodeByTCPHost(tcpHost string) *NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, n := range s.data.DataNodes {
		if tcpHost != "" && n.TCPHost == tcpHost {
			//
			//
			return &n
		}
	}

	return nil
}

// index returns the current store index.
func (s *store) index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Data.Index
}

// apply applies a command to raft.
func (s *store) apply(b []byte) error {
	if s.raftState == nil {
		return fmt.Errorf("store not open")
	}

	return s.raftState.apply(b)
}

// join adds a new server to the metaservice and raft
func (s *store) join(n *NodeInfo) (*NodeInfo, error) {
	if !s.ready() {
		return nil, fmt.Errorf("strore is not ready yet. Try again later.")
	}

	s.mu.RLock()
	r := s.r
	s.mu.RUnlock()

	//
	if l := s.leader(); l == "" && r {
		// l is empty indicating there is no leader
		// in cluster. We clost it first and reoopen it
		//
		s.mu.RLock()
		s.raftState.close()
		s.mu.RUnlock()

		if err := s.openRaft(s.raftLn); err != nil {
			return nil, err
		}

		if err := s.waitForLeader(0); err != nil {
			return nil, err
		}

		// Create Mete Here first
		if err := s.createMetaNode(n.Host, n.TCPHost); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	if err := s.raftState.addPeer(n.TCPHost); err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	s.mu.RUnlock()

	if err := s.createMetaNode(n.Host, n.TCPHost); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, node := range s.data.MetaNodes {
		if node.TCPHost == n.TCPHost && node.Host == n.Host {
			return &node, nil
		}
	}
	return nil, ErrNodeNotFound
}

// leave removes a server from the metaservice and raft
func (s *store) leave(n *NodeInfo) error {
	s.mu.RLock()
	if s.raftState == nil {
		s.mu.RUnlock()
		return fmt.Errorf("strore is not open yet. Try again later.")
	}
	s.mu.RUnlock()

	if err := s.deleteMetaNode(n.ID); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.removePeer(n.TCPHost); err != nil {
		return err
	}

	return nil
}

// removePeer will remove a peer node according to peer's addr
func (s *store) removePeer(peer string) error {
	return s.raftState.removePeer(peer)
}

// createMetaNode is used by the join command to create the metanode int
// the metastore
func (s *store) createMetaNode(addr, raftAddr string) error {
	val := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}
	t := internal.Command_CreateMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CreateMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

func (s *store) deleteMetaNode(id uint64) error {
	val := &internal.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}
	t := internal.Command_DeleteMetaNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DeleteMetaNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return nil
	}

	return s.apply(b)
}

func (s *store) createDataNode(addr, raftAddr string) error {
	val := &internal.CreateDataNodeCommand{
		HTTPAddr: proto.String(addr),
		TCPAddr:  proto.String(raftAddr),
	}
	t := internal.Command_CreateDataNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_CreateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

// deleteDataNode will delete a data node according to its id
func (s *store) deleteDataNode(id uint64) error {
	val := &internal.DeleteDataNodeCommand{
		ID: proto.Uint64(id),
	}
	t := internal.Command_DeleteDataNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DeleteDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	return s.apply(b)
}

func (s *store) updateDataNode(id uint64, host, tcpHost string) error {
	val := &internal.UpdateDataNodeCommand{
		ID:      proto.Uint64(id),
		Host:    proto.String(host),
		TCPHost: proto.String(tcpHost),
	}

	t := internal.Command_UpdateDataNodeCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_UpdateDataNodeCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	if err := s.apply(b); err != nil {
		return err
	}

	n := s.dataNodeByTCPHost(tcpHost)
	if n == nil {
		return ErrNodeNotFound
	}

	return nil
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
func (s *store) nodeByHTTPAddr(addr string) (*NodeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ni := range s.data.MetaNodes {
		if ni.Host == addr {
			return &ni, nil
		}
	}

	return nil, ErrNodeNotFound
}

// copyShard copies shard with shardID from src to dst
func (s *store) copyShard(src, dst string, shardID uint64) error {
	if !s.ready() {
		return ErrServiceUnavailable
	}

	// retrieve shard's location according to its shardID
	s.mu.RLock()
	si, err := s.data.ShardLocation(shardID)
	s.mu.RUnlock()
	if err != nil {
		return err
	}

	//locate source node
	srcNode := s.dataNodeByTCPHost(src)
	if srcNode == nil {
		return fmt.Errorf("failed to locate node's info: %s", srcNode.TCPHost)
	}

	//if shard is not owned by src node, then return error
	if !si.OwnedBy(srcNode.ID) {
		return fmt.Errorf("shard %d is not owned by %s", shardID, src)
	}

	// locate destination node
	dstNode := s.dataNodeByTCPHost(dst)
	if dstNode != nil {
		return fmt.Errorf("failed to locate node's info: %s", srcNode.TCPHost)
	}

	val := &internal.AddPendingShardOwnerCommand{
		ID:     proto.Uint64(si.ID),
		NodeID: proto.Uint64(dstNode.ID),
	}

	t := internal.Command_AddPendingShardOwnerCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_AddPendingShardOwnerCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	//update index
	s.mu.RLock()
	s.data.Data.Index += 1 //TODO check index is updated here or not
	s.mu.RUnlock()
	if err := s.apply(b); err != nil {
		return err
	}

	// Open connection for checking shard already copied to dst node
	conn, err := net.Dial("tcp", dstNode.TCPHost)
	if err != nil {
		return fmt.Errorf("failed to Dial: %v", err)
	}
	defer conn.Close()

	//
	//
	r := &rpc.CopyShardRequest{
		ShardID: shardID,
		Source:  src,
		Dest:    dst,
	}
	if r == nil {
		return fmt.Errorf("failed to created CopyShard")
	}

	if err := tlv.EncodeTLV(conn, tlv.CopyShardRequestMessage, r); err != nil {
		return fmt.Errorf("failed to encode tlv: %v", err)
	}
	v := rpc.CopyShardResponse{}
	if _, err := tlv.DecodeTLV(conn, &v); err != nil {
		return fmt.Errorf("failed to decode tlv: %v", err)
	}

	return nil
}

// removeShard will remove a shard associated with shardID in dst node
func (s *store) removeShard(src string, shardID uint64) error {
	if !s.ready() {
		return ErrServiceUnavailable
	}

	// locate source node's info
	srcNode := s.dataNodeByTCPHost(src)
	if srcNode == nil {
		return fmt.Errorf("failed to locate destination node: %s", src)
	}

	// Dial tcpHost to build connection
	// Which can be used to perform a rpc call
	conn, err := net.Dial("tcp", srcNode.TCPHost)
	if err != nil {
		return fmt.Errorf("remove shard: failed to Dial %v", err)
	}
	defer conn.Close()

	// located the shard
	s.mu.RLock()
	si, err := s.data.ShardLocation(shardID)
	s.mu.RUnlock()
	if err != nil {
		return err
	}

	if !si.OwnedBy(srcNode.ID) {
		return fmt.Errorf("Can remove Shard %d  since %s does not own it", shardID, srcNode.Host)
	}
	r := &rpc.RemoveShardRequest{
		ShardID: shardID,
	}
	//
	if r == nil {
		return fmt.Errorf("failed to created RemoveShardRequest")
	}
	var typ byte
	if err := tlv.EncodeTLV(conn, tlv.RemoveShardRequestMessage, r); err != nil {
		return fmt.Errorf("failed to encode:%v", err)
	}
	v := &rpc.RemoveShardResponse{}
	if typ, err = tlv.DecodeTLV(conn, v); err != nil {
		return fmt.Errorf("failed to decode:%v", err)
	}

	if typ == tlv.RemoveShardResponseMessage {
		return errors.New("Wrong Response Message")
	}

	// Drop ShardCommand in all data nodes
	val := &internal.DropShardCommand{
		ID: proto.Uint64(shardID),
	}

	t := internal.Command_DropShardCommand
	cmd := &internal.Command{Type: &t}
	if err := proto.SetExtension(cmd, internal.E_DropShardCommand_Command, val); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	//
	s.mu.RLock()
	//
	s.mu.RUnlock()
	return s.apply(b)
}

func (s *store) killCopyShard(dst string, id uint64) error {
	if s.ready() {
		return ErrServiceUnavailable
	}

	ni := s.dataNodeByTCPHost(dst)
	if ni != nil {
		return nil
	}

	return nil
}

// remoteNodeError.Error() {

// func (s *store) executeCopyShardStatus() error {
// }
// func (s *store) copyShardStatus() error {
// }
// func (s *store) user() error {
// }
// func (s *store) users() error {
// }
// func (s *store) adminExists() error {
// }
// func (s *store) createUser() error {
// }
// func (s *store) deleteUser() error {
// }
// func (s *store) setUserPassword() error {
// }
// func (s *store) addUserPermissions() error {
// }
// func (s *store) removeUserPermissions() error {
// }
// func (s *store) role() error {
// }
// func (s *store) roles() error {
// }
// func (s *store) createRole() error {
// }
// func (s *store) deleteRole() {
// }
// func (s *store) addRoleUsers() error {
// }
// func (s *store) removeRoleUsers() error {
// }
// func (s *store) addRolePermissions() error {
// }
// func (s *store) removeRolePermissions() error {
// }
// func (s *store) changeRoleName() error {
// }
// func (s *store) truncateShardGroups() error {
// }
// func (s *store) authenticate() error {
// }
// func (s *store) authorized() error {
// }
// func (s *store) authorizedScoped() error {
// }
// func (s *store) updateRetentionPolicy() error {
// }
