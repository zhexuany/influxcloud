package cluster

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxcloud/rpc"
	"github.com/zhexuany/influxcloud/tlv"
)

// ShardWriter writes a set of points to a shard.
type ShardWriter struct {
	pool           *clientPool
	timeout        time.Duration
	maxConnections int

	MetaClient interface {
		ShardOwner(shardID uint64) (database, policy string, owners meta.ShardInfo)
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
	}
}

// NewShardWriter returns a new instance of ShardWriter.
func NewShardWriter(timeout time.Duration, maxConnections int) *ShardWriter {
	return &ShardWriter{
		pool:           newClientPool(),
		timeout:        timeout,
		maxConnections: maxConnections,
	}
}

// WriteShard writes time series points to a shard
func (w *ShardWriter) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	buf := make([]byte, 0)
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal point: %v", err)
		}
		buf = append(buf, b...)
	}
	return w.WriteShardBinary(shardID, ownerID, buf)

}

// WriteShardBinary writes binary time series points to a shard
func (w *ShardWriter) WriteShardBinary(shardID, ownerID uint64, buf []byte) error {
	c, err := w.dial(ownerID)
	if err != nil {
		return err
	}

	conn, ok := c.(*pooledConn)
	if !ok {
		panic("wrong connection type")
	}

	// Close is not literally closing such connects.
	// Instead, Close will put connects back to pool
	// which can imporve the memory usage
	defer func(conn net.Conn) {
		conn.Close() // return to pool
	}(conn)

	// Determine the location of this shard and whether it still exists
	db, rp, _ := w.MetaClient.ShardOwner(shardID)

	// Build write request.
	var request rpc.WriteShardRequest
	request.SetShardID(shardID)
	request.SetDatabase(db)
	request.SetRetentionPolicy(rp)
	request.SetBinaryPoints(buf)

	// Marshal into protocol buffers.
	reqB, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Write request.
	conn.SetWriteDeadline(time.Now().Add(w.timeout))
	if err := tlv.WriteTLV(conn, tlv.WriteShardRequestMessage, reqB); err != nil {
		conn.MarkUnusable()
		return err
	}

	// Flush all buffered data
	if err := bufio.NewWriter(conn).Flush(); err != nil {
		return err
	}

	// Read the response.
	conn.SetReadDeadline(time.Now().Add(w.timeout))
	_, buf, err = tlv.ReadTLV(conn)
	if err != nil {
		conn.MarkUnusable()
		return err
	}

	// Unmarshal response.
	var response rpc.WriteShardResponse
	if err := response.UnmarshalBinary(buf); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

func (w *ShardWriter) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := w.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: w.pool, timeout: w.timeout}
		factory.metaClient = w.MetaClient

		p, err := NewBoundedPool(1, w.maxConnections, w.timeout, factory.dial)
		if err != nil {
			return nil, err
		}
		w.pool.setPool(nodeID, p)
	}
	return w.pool.conn(nodeID)
}

// Close closes ShardWriter's pool
func (w *ShardWriter) Close() error {
	if w.pool == nil {
		return fmt.Errorf("client already closed")
	}
	w.pool.close()
	w.pool = nil
	return nil
}

const (
	maxConnections = 500
	maxRetries     = 3
)

var errMaxConnectionsExceeded = fmt.Errorf("can not exceed max connections of %d", maxConnections)

type connFactory struct {
	nodeID  uint64
	timeout time.Duration

	clientPool interface {
		size() int
	}

	metaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
	}
}

func (c *connFactory) dial() (net.Conn, error) {
	if c.clientPool.size() > maxConnections {
		return nil, errMaxConnectionsExceeded
	}

	ni, err := c.metaClient.DataNode(c.nodeID)
	if err != nil {
		return nil, err
	}

	if ni == nil {
		return nil, fmt.Errorf("node %d does not exist", c.nodeID)
	}

	conn, err := net.DialTimeout("tcp", ni.TCPHost, c.timeout)
	if err != nil {
		return nil, err
	}

	// Write a marker byte for cluster messages.
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
