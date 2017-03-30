package meta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxdb-cluster/meta/internal"

	"github.com/gogo/protobuf/proto"
	"path/filepath"
)

const (
	// errSleep is the time to sleep after we've failed on every metaserver
	// before making another pass
	errSleep = time.Second

	// maxRetries is the maximum number of attemps to make before returning
	// a failure to the caller
	maxRetries = 10

	metaFile = "meta.db"
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")
)

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *log.Logger

	mu        sync.RWMutex
	changed   chan struct{}
	closing   chan struct{}
	cacheData *Data

	HTTPClient  *http.Client
	metaServers []string

	path string

	retentionAutoCreate bool

	tls bool

	nodeID uint64

	config *Config
}

// NewClient returns a new *Client.
func NewClient(config *Config) *Client {
	return &Client{
		changed:             make(chan struct{}),
		closing:             make(chan struct{}),
		cacheData:           &Data{},
		logger:              log.New(os.Stderr, "[metaclient] ", log.LstdFlags),
		path:                config.Dir,
		retentionAutoCreate: config.RetentionAutoCreate,
		config:              config,
	}
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {
	if c.closed() {
		return ErrServiceUnavailable
	}

	// first load the meta data from disk
	// path := c.Path()
	// if path != "" {
	// 	c.Logger().Printf("using client state dir:%s", path)
	// 	if err := c.loadMetaServers(path); err != nil {
	// 		c.Logger().Fatalf("failed to load meta sever data from %s", path)
	// 	}
	// }

	// if metas := c.MetaServers(); len(metas) == 0 {
	// 	return fmt.Errorf("MetaServers is empty. It shold at least contain itslef")
	// }

	// c.changed = make(chan struct{})
	// c.closing = make(chan struct{})

	// c.cacheData = c.retryUntilSnapshot(0)
	// if c.cacheData == nil {
	// 	return fmt.Errorf("failed to snapshot %v", c.cacheData)
	// }

	// if err := c.updateMetaServers(); err != nil {
	// 	c.Logger().Println("failed to updated meta servers")
	// }

	c.changed = make(chan struct{})
	c.closing = make(chan struct{})
	c.cacheData = c.retryUntilSnapshot(0)

	go c.pollForUpdates()

	return nil
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	if !c.closed() {
		return ErrService
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

func (c *Client) closed() bool {
	select {
	case <-c.closing:
		return true
	default:
		return false
	}
}

//TODO zhexuany revisit this later after the realse of our prototype
func (c *Client) doHTTP(path string, method string, reader io.Reader) (resp *http.Response, err error) {
	return nil, nil
}

func (c *Client) get(path string) (*http.Response, error) {
	return c.doHTTP(path, "GET", nil)
}

func (c *Client) post(path string, cnt string, bufer *bytes.Buffer) (*http.Response, error) {
	return c.doHTTP(path, "POST", nil)
}

// Leave send leave command into cluster
func (c *Client) Leave() error {
	return c.resetMetaServers()
}

// GetNodeID returns the client's node ID.
func (c *Client) NodeID() uint64 { return c.nodeID }

// SetMetaServers updates the meta servers on the client.
func (c *Client) SetMetaServers(a []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metaServers = a
}

func (c *Client) CheckMetaServers() error {
	resp, err := c.get("")
	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
	}
	res := make([]string, 0)
	if err := json.Unmarshal(buf, res); err != nil {
		return err
	}

	for i, meta := range c.MetaServers() {
		if res[i] == meta {
			return ErrService
		}
	}

	return nil
}

func (c *Client) Path() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.path
}

// SetPath will set path as new path in this meta node
func (c *Client) SetPath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.path = path
}

func (c *Client) TLS() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tls
}

// SetTLS sets whether the client should use TLS when connecting.
// This function is not safe for concurrent use.
func (c *Client) SetTLS(v bool) {
	c.mu.RLock()
	c.mu.RUnlock()
	c.tls = v
}

func (c *Client) SetHTTPClient(httpClient *http.Client) {
	c.mu.Lock()
	c.HTTPClient = httpClient
	c.mu.Unlock()
}

// Ping will hit the ping endpoint for the metaservice and return nil if
// it returns 200. If checkAllMetaServers is set to true, it will hit the
// ping endpoint and tell it to verify the health of all metaservers in the
// cluster
func (c *Client) Ping(checkAllMetaServers bool) error {
	c.mu.RLock()
	server := c.metaServers[0]
	c.mu.RUnlock()
	url := c.url(server) + "/ping"
	if checkAllMetaServers {
		url = url + "?all=true"
	}

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf(string(b))
}

// AcquireLease attempts to acquire the specified lease.
// A lease is a logical concept that can be used by anything that needs to limit
// execution to a single node.  E.g., the CQ service on all nodes may ask for
// the "ContinuousQuery" lease. Only the node that acquires it will run CQs.
// NOTE: Leases are not managed through the CP system and are not fully
// consistent.  Any actions taken after acquiring a lease must be idempotent.
func (c *Client) AcquireLease(name string) (l *meta.Lease, err error) {
	for n := 1; n < 11; n++ {
		if l, err = c.acquireLease(name); err == ErrServiceUnavailable || err == ErrService {
			// exponential backoff
			d := time.Duration(math.Pow(10, float64(n))) * time.Millisecond
			time.Sleep(d)
			continue
		}
		break
	}
	return
}

func (c *Client) acquireLease(name string) (*meta.Lease, error) {
	c.mu.RLock()
	servers := c.MetaServers()
	c.mu.RUnlock()

	for _, server := range servers {
		url := fmt.Sprintf("%s/lease?name=%s&nodeid=%d", server, name, c.nodeID)

		resp, err := c.get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			// Read lease JSON from response body.
			b, e := ioutil.ReadAll(resp.Body)
			if e != nil {
				return nil, e
			}
			// Unmarshal JSON into a Lease.
			l := &meta.Lease{}
			if e = json.Unmarshal(b, l); e != nil {
				return nil, e
			}

			return l, err
		case http.StatusConflict:
			err = errors.New("another node owns the lease")
		case http.StatusServiceUnavailable:
			return nil, ErrServiceUnavailable
		case http.StatusBadRequest:
			b, e := ioutil.ReadAll(resp.Body)
			if e != nil {
				return nil, e
			}
			return nil, fmt.Errorf("meta service: %s", string(b))
		case http.StatusInternalServerError:
			return nil, errors.New("meta service internal error")
		default:
			return nil, errors.New("unrecognized meta service error")
		}
	}
	return nil, errors.New("unrecognized meta service error")
}

func (c *Client) setData(data *Data) {
	c.mu.Lock()
	c.cacheData = data
	c.mu.Unlock()
}

func (c *Client) data() *Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	return c.data().Data.ClusterID
}

// Node returns a node by id.
func (c *Client) DataNode(id uint64) (*NodeInfo, error) {
	for _, n := range c.data().DataNodes {
		if n.ID == id {
			return &n, nil
		}
	}
	return nil, ErrNodeNotFound
}

// DataNodes returns the data nodes' info.
func (c *Client) DataNodes() (NodeInfos, error) {
	return c.data().DataNodes, nil
}

// CreateDataNode will create a new data node in the metastore
func (c *Client) CreateDataNode(httpAddr, tcpAddr string) (*NodeInfo, error) {
	cmd := &internal.CreateDataNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
	}

	if err := c.retryUntilExec(internal.Command_CreateDataNodeCommand, internal.E_CreateDataNodeCommand_Command, cmd); err != nil {
		return nil, err
	}

	n, err := c.DataNodeByTCPHost(tcpAddr)
	if err != nil {
		return nil, err
	}

	c.nodeID = n.ID

	return n, nil
}

func (c *Client) ShardPendingOwners() uint64arr {
	mns := c.data().MetaNodes
	for _, meta := range mns {
		if meta.ID == c.nodeID {
			return meta.PendingShardOwners
		}
	}

	return uint64arr{}
}

func (c *Client) RemovePendingShardOwner(id, nodeid uint64) error {
	cmd := &internal.RemovePendingShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeid),
	}

	return c.retryUntilExec(internal.Command_RemovePendingShardOwnerCommand, internal.E_RemovePendingShardOwnerCommand_Command, cmd)
}

func (c *Client) CommitPendingShardOwner(id, nodeid uint64) error {
	cmd := &internal.CommitPendingShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeid),
	}

	return c.retryUntilExec(internal.Command_CommitPendingShardOwnerCommand, internal.E_CommitPendingShardOwnerCommand_Command, cmd)
}

func (c *Client) AddShardOwner(id, nodeid uint64) error {
	cmd := &internal.AddShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeid),
	}

	return c.retryUntilExec(internal.Command_AddShardOwnerCommand, internal.E_AddShardOwnerCommand_Command, cmd)
}

func (c *Client) RemoveShardOwner(id, nodeid uint64) error {
	cmd := &internal.RemoveShardOwnerCommand{
		ID:     proto.Uint64(id),
		NodeID: proto.Uint64(nodeid),
	}

	return c.retryUntilExec(internal.Command_RemoveShardOwnerCommand, internal.E_AddShardOwnerCommand_Command, cmd)
}

func (c *Client) UpdateDataNode(id uint64, host, tcpHost string) error {
	cmd := &internal.UpdateDataNodeCommand{
		ID:      proto.Uint64(id),
		Host:    proto.String(host),
		TCPHost: proto.String(tcpHost),
	}

	if err := c.retryUntilExec(internal.Command_UpdateDataNodeCommand, internal.E_UpdateDataNodeCommand_Command, cmd); err != nil {
		return err
	}

	n, err := c.DataNodeByTCPHost(tcpHost)
	if err != nil {
		return err
	}

	c.nodeID = n.ID

	return nil
}

// DataNodeByHTTPHost returns the data node with the give http bind address
func (c *Client) DataNodeByHTTPHost(httpAddr string) (*NodeInfo, error) {
	nodes, _ := c.DataNodes()
	for _, n := range nodes {
		if n.Host == httpAddr {
			return &n, nil
		}
	}

	return nil, ErrNodeNotFound
}

// DataNodeByTCPHost returns the data node with the give http bind address
func (c *Client) DataNodeByTCPHost(tcpAddr string) (*NodeInfo, error) {
	nodes, _ := c.DataNodes()
	for _, n := range nodes {
		if n.TCPHost == tcpAddr {
			return &n, nil
		}
	}

	return nil, ErrNodeNotFound
}

// DeleteDataNode deletes a data node from the cluster.
func (c *Client) DeleteDataNode(id uint64) error {
	cmd := &internal.DeleteDataNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteDataNodeCommand, internal.E_DeleteDataNodeCommand_Command, cmd)
}

// MetaNodes returns the meta nodes' info.
func (c *Client) MetaNodes() (NodeInfos, error) {
	return c.data().MetaNodes, nil
}

// MetaNodeByAddr returns the meta node's info.
func (c *Client) MetaNodeByAddr(addr string) *NodeInfo {
	for _, n := range c.data().MetaNodes {
		if n.Host == addr {
			return &n
		}
	}
	return nil
}

// Database returns info for the requested database.
func (c *Client) Database(name string) (*meta.DatabaseInfo, error) {
	for _, d := range c.data().Data.Databases {
		if d.Name == name {
			return &d, nil
		}
	}

	// Can't throw ErrDatabaseNotExists here since it would require some major
	// work around catching the error when needed. Should be revisited.
	return nil, nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() ([]meta.DatabaseInfo, error) {
	dbs := c.data().Data.Databases
	if dbs == nil {
		return []meta.DatabaseInfo{}, nil
	}
	return dbs, nil
}

func (c *Client) defaultRetentionPolicyInfo(name string, duration time.Duration) *meta.RetentionPolicyInfo {
	// Retrieve all data nodes from cluster
	nis, err := c.DataNodes()
	if err != nil {
		return nil
	}

	replicaN := 0
	if len(nis) < 3 {
		replicaN = 3
	} else if len(nis) > 1 {
		replicaN = len(nis)
	} else if len(nis) < 1 {
		replicaN = 1
	}
	return &meta.RetentionPolicyInfo{
		Name:     name,
		ReplicaN: replicaN,
		Duration: duration,
	}
}

// CreateDatabase creates a database or returns it if it already exists
func (c *Client) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	if db, _ := c.Database(name); db != nil {
		return db, nil
	}

	cmd := &internal.CreateDatabaseCommand{
		Name: proto.String(name),
	}

	err := c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	return c.Database(name)
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified retention policy.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	if db, _ := c.Database(name); db != nil {
		// Check if the retention policy already exists. If it does and matches
		// the desired retention policy, exit with no error.
		if rp := db.RetentionPolicy(spec.Name); rp != nil {
			if spec.Matches(rp) {
				return nil, ErrRetentionPolicyConflict
			}
			return db, nil
		}
	}

	rpiB, err := spec.NewRetentionPolicyInfo().MarshalBinary()
	if err != nil {
		return nil, err
	}

	cmd := &internal.CreateDatabaseCommand{
		Name:            proto.String(name),
		RetentionPolicy: rpiB,
	}

	err = c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	return c.Database(name)
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	cmd := &internal.DropDatabaseCommand{
		Name: proto.String(name),
	}

	return c.retryUntilExec(internal.Command_DropDatabaseCommand, internal.E_DropDatabaseCommand_Command, cmd)
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec) (*meta.RetentionPolicyInfo, error) {
	if rp, _ := c.RetentionPolicy(database, spec.Name); rp != nil {
		return rp, nil
	}

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	_, err := spec.MarshalBinary()
	if err != nil {
		return nil, err
	}

	//
	rpi := c.defaultRetentionPolicyInfo(spec.Name, *spec.Duration)
	if rpi == nil {
		return nil, ErrRetentionPolicyConflict
	}

	//
	rpiB, err := rpi.MarshalBinary()
	if err != nil {
		return nil, err
	}
	cmd := &internal.CreateRetentionPolicyCommand{
		Database:        proto.String(database),
		RetentionPolicy: rpiB,
	}

	if err := c.retryUntilExec(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return nil, err
	}

	return c.RetentionPolicy(database, rpi.Name)
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	db, err := c.Database(database)
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, influxdb.ErrDatabaseNotFound(database)
	}

	return db.RetentionPolicy(name), nil
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	cmd := &internal.DropRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}

	return c.retryUntilExec(internal.Command_DropRetentionPolicyCommand, internal.E_DropRetentionPolicyCommand_Command, cmd)
}

// SetDefaultRetentionPolicy sets a database's default retention policy.
func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	cmd := &internal.SetDefaultRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}

	return c.retryUntilExec(internal.Command_SetDefaultRetentionPolicyCommand, internal.E_SetDefaultRetentionPolicyCommand_Command, cmd)
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error {
	var newName *string
	if rpu.Name != nil {
		newName = rpu.Name
	}

	var duration *int64
	if rpu.Duration != nil {
		value := int64(*rpu.Duration)
		duration = &value
	}

	var replicaN *uint32
	if rpu.ReplicaN != nil {
		value := uint32(*rpu.ReplicaN)
		replicaN = &value
	}

	cmd := &internal.UpdateRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
		NewName:  newName,
		Duration: duration,
		ReplicaN: replicaN,
	}

	return c.retryUntilExec(internal.Command_UpdateRetentionPolicyCommand, internal.E_UpdateRetentionPolicyCommand_Command, cmd)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	var a []uint64
	for _, dbi := range c.data().Data.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	sort.Sort(uint64Slice(a))
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a meta.ShardGroupInfos, err error) {
	// Find retention policy.
	rpi, err := c.data().Data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]meta.ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	m := make(map[*meta.ShardInfo]struct{})
	for _, src := range sources {
		mm, ok := src.(*influxql.Measurement)
		if !ok {
			return nil, fmt.Errorf("invalid source type: %#v", src)
		}

		groups, err := c.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]meta.ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	if sg, _ := c.data().Data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}

	cmd := &internal.CreateShardGroupCommand{
		Database:  proto.String(database),
		Policy:    proto.String(policy),
		Timestamp: proto.Int64(timestamp.UnixNano()),
	}

	if err := c.retryUntilExec(internal.Command_CreateShardGroupCommand, internal.E_CreateShardGroupCommand_Command, cmd); err != nil {
		return nil, err
	}

	rpi, err := c.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}

	return rpi.ShardGroupByTimestamp(timestamp), nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	cmd := &internal.DeleteShardGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		ShardGroupID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteShardGroupCommand, internal.E_DeleteShardGroupCommand_Command, cmd)
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	for _, di := range c.data().Data.Databases {
		for _, rp := range di.RetentionPolicies {
			if len(rp.ShardGroups) == 0 {
				// No data was ever written to this group, or all groups have been deleted.
				continue
			}
			g := rp.ShardGroups[len(rp.ShardGroups)-1] // Get the last group in time.
			if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
				// Group is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive shard group.
				nextShardGroupTime := g.EndTime.Add(1 * time.Nanosecond)
				if newGroup, err := c.CreateShardGroup(di.Name, rp.Name, nextShardGroupTime); err != nil {
					c.logger.Printf("failed to precreate successive shard group for group %d: %s", g.ID, err.Error())
				} else {
					c.logger.Printf("new shard group %d successfully precreated for database %s, retention policy %s", newGroup.ID, di.Name, rp.Name)
				}
			}
		}
		return nil
	}
	return nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	for _, dbi := range c.data().Data.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, g := range rpi.ShardGroups {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						policy = rpi.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// JoinMetaServer will add the passed in tcpAddr to the raft peers and add a MetaNode to
// the metastore
func (c *Client) JoinMetaServer(httpAddr, tcpAddr string) (*NodeInfo, error) {
	node := &NodeInfo{
		Host:    httpAddr,
		TCPHost: tcpAddr,
	}
	b, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	currentServer := 0
	redirectServer := ""
	//TODO spinning is ok but need a way to log this.
	// or after a specific seconds just return error
	for {
		// get the server to try to join against
		var url string
		if redirectServer != "" {
			url = redirectServer
			redirectServer = ""
		} else {
			c.mu.RLock()

			if currentServer >= len(c.metaServers) {
				// We've tried every server, wait a second before
				// trying again
				time.Sleep(time.Second)
				currentServer = 0
			}
			server := c.metaServers[currentServer]
			c.mu.RUnlock()

			url = c.url(server) + "/join"
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(b))
		if err != nil {
			currentServer++
			continue
		}

		// Successfully joined
		if resp.StatusCode == http.StatusOK {
			defer resp.Body.Close()
			if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
				return nil, err
			}
			break
		}
		resp.Body.Close()

		// We tried to join a meta node that was not the leader, rety at the node
		// they think is the leader.
		if resp.StatusCode == http.StatusTemporaryRedirect {
			redirectServer = resp.Header.Get("Location")
			c.logger.Println("redirect to leader", redirectServer)
			continue
		}

		// Something failed, try the next node
		currentServer++
	}

	return node, nil
}

func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) (*NodeInfo, error) {
	cmd := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
		Rand:     proto.Uint64(uint64(rand.Int63())),
	}

	if err := c.retryUntilExec(internal.Command_CreateMetaNodeCommand, internal.E_CreateMetaNodeCommand_Command, cmd); err != nil {
		return nil, err
	}

	n := c.MetaNodeByAddr(httpAddr)
	if n == nil {
		return nil, errors.New("new meta node not found")
	}

	c.nodeID = n.ID

	return n, nil
}

func (c *Client) DeleteMetaNode(id uint64) error {
	cmd := &internal.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteMetaNodeCommand, internal.E_DeleteMetaNodeCommand_Command, cmd)
}

func (c *Client) CreateContinuousQuery(database, name, query string) error {
	return c.retryUntilExec(internal.Command_CreateContinuousQueryCommand, internal.E_CreateContinuousQueryCommand_Command,
		&internal.CreateContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
			Query:    proto.String(query),
		},
	)
}

func (c *Client) DropContinuousQuery(database, name string) error {
	return c.retryUntilExec(internal.Command_DropContinuousQueryCommand, internal.E_DropContinuousQueryCommand_Command,
		&internal.DropContinuousQueryCommand{
			Database: proto.String(database),
			Name:     proto.String(name),
		},
	)
}

func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return c.retryUntilExec(internal.Command_CreateSubscriptionCommand, internal.E_CreateSubscriptionCommand_Command,
		&internal.CreateSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
			Mode:            proto.String(mode),
			Destinations:    destinations,
		},
	)
}

func (c *Client) DropSubscription(database, rp, name string) error {
	return c.retryUntilExec(internal.Command_DropSubscriptionCommand, internal.E_DropSubscriptionCommand_Command,
		&internal.DropSubscriptionCommand{
			Database:        proto.String(database),
			RetentionPolicy: proto.String(rp),
			Name:            proto.String(name),
		},
	)
}

func (c *Client) Data() *Data {
	return c.data().Clone()
}

func (c *Client) SetData(data *Data) error {
	dataB, _ := data.MarshalBinary()
	return c.retryUntilExec(internal.Command_SetDataCommand, internal.E_SetDataCommand_Command,
		&internal.SetDataCommand{
			Data: dataB,
		},
	)
}

// WaitForDataChanged will return a channel that will get closed when
// the metastore data has changed
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

func (c *Client) Logger() *log.Logger {
	return c.logger
}

func (c *Client) SetLogger(l *log.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = l
}

func (c *Client) index() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data().Data.Index
}

// retryUntilExec will attempt the command on each of the metaservers until it either succeeds or
// hits the max number of tries
func (c *Client) retryUntilExec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	var err error
	var index uint64
	tries := 0
	currentServer := 0
	var redirectServer string

	for {
		c.mu.RLock()
		// exit if we're closed
		select {
		case <-c.closing:
			c.mu.RUnlock()
			return nil
		default:
			// we're still open, continue on
		}
		c.mu.RUnlock()

		// build the url to hit the redirect server or the next metaserver
		var url string
		if redirectServer != "" {
			url = redirectServer
			redirectServer = ""
		} else {
			c.mu.RLock()
			if currentServer >= len(c.metaServers) {
				currentServer = 0
			}
			server := c.metaServers[currentServer]
			c.mu.RUnlock()

			url = fmt.Sprintf("://%s/execute", server)
			if c.tls {
				url = "https" + url
			} else {
				url = "http" + url
			}
		}

		index, err = c.exec(url, typ, desc, value)
		tries++
		currentServer++

		if err == nil {
			c.waitForIndex(index)
			return nil
		}

		if tries > maxRetries {
			return err
		}

		if e, ok := err.(errRedirect); ok {
			redirectServer = e.host
			continue
		}

		if _, ok := err.(errCommand); ok {
			return err
		}

		time.Sleep(errSleep)
	}
}

func (c *Client) exec(url string, typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) (index uint64, err error) {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// read the response
	if resp.StatusCode == http.StatusTemporaryRedirect {
		return 0, errRedirect{host: resp.Header.Get("Location")}
	} else if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("meta service returned %s", resp.Status)
	}

	res := &internal.Response{}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if err := proto.Unmarshal(b, res); err != nil {
		return 0, err
	}
	es := res.GetError()
	if es != "" {
		return 0, errCommand{msg: es}
	}

	return res.GetIndex(), nil
}

func (c *Client) waitForIndex(idx uint64) {
	for {
		c.mu.RLock()
		if c.cacheData.Data.Index >= idx {
			c.mu.RUnlock()
			return
		}
		ch := c.changed
		c.mu.RUnlock()
		<-ch
	}
}

func (c *Client) pollForUpdates() {
	for {
		data := c.retryUntilSnapshot(c.index())
		if data == nil {
			// this will only be nil if the client has been closed,
			// so we can exit out
			return
		}

		// update the data and notify of the change
		c.mu.Lock()
		idx := c.cacheData.Data.Index
		c.cacheData = data
		if idx < data.Data.Index {
			close(c.changed)
			c.changed = make(chan struct{})
		}
		c.mu.Unlock()
	}
}

func (c *Client) getSnapshot(server string, index uint64) (*Data, error) {
	if server == "" {
		return nil, errors.New("server host empty")
	}
	// resp, err := c.get(server + fmt.Sprintf("?index=%d", index))
	resp, err := http.Get(c.url(server) + fmt.Sprintf("?index=%d", index))

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("meta server returned non-200: %s", resp.Status)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return data, nil
}

// peers returns the TCPHost addresses of all the metaservers
func (c *Client) peers() []string {

	var peers Peers
	// query each server and keep track of who their peers are
	for _, server := range c.metaServers {
		url := c.url(server) + "/peers"
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		// This meta-server might not be ready to answer, continue on
		if resp.StatusCode != http.StatusOK {
			continue
		}

		dec := json.NewDecoder(resp.Body)
		var p []string
		if err := dec.Decode(&p); err != nil {
			continue
		}
		peers = peers.Append(p...)
	}

	// Return the unique set of peer addresses
	return []string(peers.Unique())
}

func (c *Client) url(server string) string {
	url := fmt.Sprintf("://%s", server)

	if c.tls {
		url = "https" + url
	} else {
		url = "http" + url
	}

	return url
}

func (c *Client) retryUntilSnapshot(idx uint64) *Data {
	currentServer := 0
	for {
		if c.closed() {
			return nil
		}
		metaServers := c.MetaServers()
		if currentServer >= len(metaServers) {
			currentServer = 0
		}
		server := metaServers[currentServer]

		data, err := c.getSnapshot(server, idx)

		if err == nil {
			return data
		}

		c.Logger().Printf("failure getting snapshot from %s: %s", server, err.Error())
		time.Sleep(errSleep)

		currentServer++
	}
}

func (c *Client) updateMetaServers() error {
	copy := c.data()
	if copy == nil {
		return ErrService
	}
	metaServers := c.MetaServers()
	c.SetMetaServers(metaServers)
	return nil
}

func (c *Client) saveMetaServers() error {
	path := c.Path()
	if path == "" {
		return errors.New("path is not set")
	}

	if err := os.MkdirAll(path, 0777); err != nil {
		return err
	}

	file := filepath.Join(path, metaFile)
	tmpFile := file + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(f).Encode(c.metaServers); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpFile, file)
}

func (c *Client) loadMetaServers(path string) error {
	file := filepath.Join(c.path, metaFile)

	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(c.metaServers); err != nil {
		return err
	}
	return nil
}

func (c *Client) resetMetaServers() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	file := filepath.Join(c.Path(), metaFile)
	tmpFile := file + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	metas := make([]string, len(c.metaServers)-1)
	for i, meta := range c.metaServers {
		if meta != c.config.BindAddress {
			metas[i] = meta
		}
	}

	if err := json.NewEncoder(f).Encode(metas); err != nil {
		f.Close()
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpFile, file)
}

func (c *Client) MetaServers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metaServers
}

type errRedirect struct {
	host string
}

func (e errRedirect) Error() string {
	return fmt.Sprintf("redirect to %s", e.host)
}

type errCommand struct {
	msg string
}

func (e errCommand) Error() string {
	return e.msg
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

func (c *Client) tryHTTP() {
	c.doHTTPWithRedirect()
}

func (c *Client) doHTTPWithRedirect() {}

type Peers []string

func (peers Peers) Append(p ...string) Peers {
	peers = append(peers, p...)

	return peers.Unique()
}

func (peers Peers) Unique() Peers {
	distinct := map[string]struct{}{}
	for _, p := range peers {
		distinct[p] = struct{}{}
	}

	var u Peers
	for k := range distinct {
		u = append(u, k)
	}
	return u
}

func (peers Peers) Contains(peer string) bool {
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}
