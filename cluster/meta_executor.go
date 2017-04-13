package cluster

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/zhexuany/influxdb-cluster/rpc"
	"github.com/zhexuany/influxdb-cluster/tlv"
	"io"
)

const (
	metaExecutorWriteTimeout        = 5 * time.Second
	metaExecutorMaxWriteConnections = 10
)

// MetaExecutor executes meta queries on all data nodes.
type MetaExecutor struct {
	mu             sync.RWMutex
	timeout        time.Duration
	pool           *clientPool
	maxConnections int
	Logger         *log.Logger
	Node           *influxdb.Node

	nodeExecutor interface {
		executeOnNode(stmt influxql.Statement, database string, node *meta.NodeInfo) error
	}

	MetaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		DataNodes() ([]meta.NodeInfo, error)
	}

	TSDBStore interface {
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		BackupShard(id uint64, since time.Time, w io.Writer) error
		RestoreShard(id uint64, r io.Reader) error
		Measurements(database string, cond influxql.Expr) ([]string, error)
		TagValues(database string, cond influxql.Expr) ([]tsdb.TagValues, error)
	}

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}
}

// NewMetaExecutor returns a new initialized *MetaExecutor.
func NewMetaExecutor() *MetaExecutor {
	m := &MetaExecutor{
		timeout:        metaExecutorWriteTimeout,
		pool:           newClientPool(),
		maxConnections: metaExecutorMaxWriteConnections,
		Logger:         log.New(os.Stderr, "[meta-executor] ", log.LstdFlags),
	}
	m.nodeExecutor = m

	return m
}

//
//

// remoteNodeError wraps an error with context about a node that
// returned the error.
type remoteNodeError struct {
	id  uint64
	err error
}

func (e remoteNodeError) Error() string {
	return fmt.Sprintf("partial success, node %d may be down (%s)", e.id, e.err)
}

// ExecuteStatement executes a single InfluxQL statement on all nodes in the cluster concurrently.
func (m *MetaExecutor) ExecuteStatement(stmt influxql.Statement, database string) error {
	// Get a list of all nodes the query needs to be executed on.
	nodes, err := m.MetaClient.DataNodes()
	if err != nil {
		return err
	} else if len(nodes) < 1 {
		return nil
	}

	// Start a goroutine to execute the statement on each of the remote nodes.
	var wg sync.WaitGroup
	errs := make(chan error, len(nodes)-1)
	for _, node := range nodes {
		wg.Add(1)
		go func(node meta.NodeInfo) {
			defer wg.Done()
			if err := m.nodeExecutor.executeOnNode(stmt, database, &node); err != nil {
				errs <- remoteNodeError{id: node.ID, err: err}
			}
		}(node)
	}

	// Wait on n-1 nodes to execute the statement and respond.
	wg.Wait()

	select {
	case err = <-errs:
		return err
	default:
		return nil
	}
}

// executeOnNode executes a single InfluxQL statement on a single node.
func (m *MetaExecutor) executeOnNode(stmt influxql.Statement, database string, node *meta.NodeInfo) error {
	// We're executing on a remote node so establish a connection.
	c, err := m.dial(node.ID)
	if err != nil {
		return err
	}

	conn, ok := c.(*pooledConn)
	if !ok {
		panic("wrong connection type in MetaExecutor")
	}
	// Return connection to pool by "closing" it.
	defer conn.Close()

	// Build RPC request.
	var request rpc.ExecuteStatementRequest
	request.SetStatement(stmt.String())
	request.SetDatabase(database)

	// Marshal into protocol buffer.
	buf, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Send request.
	conn.SetWriteDeadline(time.Now().Add(m.timeout))
	if err := tlv.WriteTLV(conn, tlv.ExecuteStatementRequestMessage, buf); err != nil {
		conn.MarkUnusable()
		return err
	}

	// Read the response.
	conn.SetReadDeadline(time.Now().Add(m.timeout))
	// TODO need migrte readTLV from cluster to tlv
	// it is just temporary solution
	_, buf, err = ReadTLV(conn)
	if err != nil {
		conn.MarkUnusable()
		return err
	}

	// Unmarshal response.
	var response rpc.ExecuteStatementResponse
	if err := response.UnmarshalBinary(buf); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

// dial returns a connection to a single node in the cluster.
func (m *MetaExecutor) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := m.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: m.pool, timeout: m.timeout}
		factory.metaClient = m.MetaClient

		p, err := NewBoundedPool(1, m.maxConnections, m.timeout, factory.dial)
		if err != nil {
			return nil, err
		}
		m.pool.setPool(nodeID, p)
	}
	return m.pool.conn(nodeID)
}

func (m *MetaExecutor) CreateShard(db, policy string, shardID uint64, enabled bool) error {
	return m.TSDBStore.CreateShard(db, policy, shardID, enabled)
}

func (m *MetaExecutor) WriteToShard(shardID, ownerID uint64, points []models.Point) error {
	return m.ShardWriter.WriteShard(shardID, ownerID, points)
}

func (m *MetaExecutor) DeleteDatabase(stmt influxql.Statement) error {
	db := ""
	return m.ExecuteStatement(stmt, db)
}

func (m *MetaExecutor) DeleteMeasurement(stmt influxql.Statement) error {
	db := ""
	return m.ExecuteStatement(stmt, db)
}

func (m *MetaExecutor) DeleteRetentionPolicy(stmt influxql.Statement) error {
	db := ""
	if st, ok := stmt.(*influxql.DropRetentionPolicyStatement); ok {
		db = st.Database
	}
	return m.ExecuteStatement(stmt, db)
}

func (m *MetaExecutor) DeleteSeries(stmt influxql.Statement) error {
	db := ""
	// if st, ok := stmt.(*influxql.DropSeriesStatement); ok {

	// }
	return m.ExecuteStatement(stmt, db)
}

func (m *MetaExecutor) DeleteShard(stmt influxql.Statement) error {
	db := ""
	return m.ExecuteStatement(stmt, db)
}

func (m *MetaExecutor) BackupShard(id uint64, since time.Time, w io.Writer) error {
	return m.TSDBStore.BackupShard(id, since, w)
}

func (m *MetaExecutor) RestoreShard(id uint64, r io.Reader) error {
	return m.TSDBStore.RestoreShard(id, r)
}

func (m *MetaExecutor) IteratorCreator(opt influxql.IteratorOptions) influxql.IteratorCreator {
	return nil
}

func (m *MetaExecutor) Measurements() []string {
	return nil
}

func (m *MetaExecutor) TagValues() {

}

func (m *MetaExecutor) MetaIteratorCreator() {
}
