package meta_test

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/toml"
	influxdb_cluster "github.com/zhexuany/influxdb-cluster"
	cluster_meta "github.com/zhexuany/influxdb-cluster/meta"
	"sync"
)

func TestMetaService_CreateDatabase(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Make sure a default retention policy was created.
	_, err = c.RetentionPolicy("db0", "default")
	if err != nil {
		t.Fatal(err)
	} else if db.DefaultRetentionPolicy != "default" {
		t.Fatalf("rp name wrong: %s", db.DefaultRetentionPolicy)
	}
}

func TestMetaService_CreateDatabaseIfNotExists(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}
}

func TestMetaService_CreateDatabaseWithRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	hour := time.Duration(time.Hour)
	one := 1
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &meta.RetentionPolicySpec{
		Name:     "rp0",
		Duration: &hour,
		ReplicaN: &one,
	}); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp := db.RetentionPolicy("rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}
}

func TestMetaService_Databases(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create two databases.
	db, err := c.CreateDatabase("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	db, err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db1" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	dbs, err := c.Databases()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases but got %d", len(dbs))
	} else if dbs[0].Name != "db0" {
		t.Fatalf("db name wrong: %s", dbs[0].Name)
	} else if dbs[1].Name != "db1" {
		t.Fatalf("db name wrong: %s", dbs[1].Name)
	}
}

func TestMetaService_DropDatabase(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	if err := c.DropDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	if db, _ = c.Database("db0"); db != nil {
		t.Fatalf("expected database to not return: %v", db)
	}

	// Dropping a database that does not exist is not an error.
	if err := c.DropDatabase("db foo"); err != nil {
		t.Fatalf("got %v error, but expected no error", err)
	}
}

func TestMetaService_CreateRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	hour := time.Hour
	one := 1
	if _, err := c.CreateRetentionPolicy("db0", &meta.RetentionPolicySpec{
		Name:     "rp0",
		Duration: &hour,
		ReplicaN: &one,
	}); err != nil {
		t.Fatal(err)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	// Create the same policy.  Should not error.
	if _, err := c.CreateRetentionPolicy("db0", &meta.RetentionPolicySpec{
		Name:     "rp0",
		Duration: &hour,
		ReplicaN: &one,
	}); err != nil {
		t.Fatal(err)
	}

	rp, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}
}

func TestMetaService_SetDefaultRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	hour := time.Hour
	one := 1
	if _, err := c.CreateDatabaseWithRetentionPolicy("db0", &meta.RetentionPolicySpec{
		Name:     "rp0",
		Duration: &hour,
		ReplicaN: &one,
	}); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	// Make sure default retention policy is now rp0
	if db.DefaultRetentionPolicy != "rp0" {
		t.Fatalf("rp name wrong: %s", db.DefaultRetentionPolicy)
	}
}

func TestMetaService_DropRetentionPolicy(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	hour := time.Hour
	one := 1
	if _, err := c.CreateRetentionPolicy("db0", &meta.RetentionPolicySpec{
		Name:     "rp0",
		Duration: &hour,
		ReplicaN: &one,
	}); err != nil {
		t.Fatal(err)
	}

	rp, err := c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp.Name != "rp0" {
		t.Fatalf("rp name wrong: %s", rp.Name)
	} else if rp.Duration != time.Hour {
		t.Fatalf("rp duration wrong: %s", rp.Duration.String())
	} else if rp.ReplicaN != 1 {
		t.Fatalf("rp replication wrong: %d", rp.ReplicaN)
	}

	if err := c.DropRetentionPolicy("db0", "rp0"); err != nil {
		t.Fatal(err)
	}

	rp, err = c.RetentionPolicy("db0", "rp0")
	if err != nil {
		t.Fatal(err)
	} else if rp != nil {
		t.Fatalf("rp should have been dropped")
	}
}

func TestMetaService_ContinuousQueries(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Create a CQ
	if err := c.CreateContinuousQuery("db0", "cq0", `SELECT count(value) INTO foo_count FROM foo GROUP BY time(10m)`); err != nil {
		t.Fatal(err)
	}

	// Recreate an existing CQ
	if err := c.CreateContinuousQuery("db0", "cq0", `SELECT max(value) INTO foo_max FROM foo GROUP BY time(10m)`); err == nil || err.Error() != `continuous query already exists` {
		t.Fatalf("unexpected error: %s", err)
	}

	// Create a few more CQ's
	if err := c.CreateContinuousQuery("db0", "cq1", `SELECT max(value) INTO foo_max FROM foo GROUP BY time(10m)`); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateContinuousQuery("db0", "cq2", `SELECT min(value) INTO foo_min FROM foo GROUP BY time(10m)`); err != nil {
		t.Fatal(err)
	}

	// Drop a single CQ
	if err := c.DropContinuousQuery("db0", "cq1"); err != nil {
		t.Fatal(err)
	}
}

func TestMetaService_Subscriptions_Create(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}
	db, err := c.Database("db0")
	if err != nil {
		t.Fatal(err)
	} else if db.Name != "db0" {
		t.Fatalf("db name wrong: %s", db.Name)
	}

	// Create a subscription
	if err := c.CreateSubscription("db0", "default", "sub0", "ALL", []string{"udp://example.com:9090"}); err != nil {
		t.Fatal(err)
	}

	// Re-create a subscription
	if err := c.CreateSubscription("db0", "default", "sub0", "ALL", []string{"udp://example.com:9090"}); err == nil || err.Error() != `subscription already exists` {
		t.Fatalf("unexpected error: %s", err)
	}

	// Create another subscription.
	if err := c.CreateSubscription("db0", "default", "sub1", "ALL", []string{"udp://example.com:6060"}); err != nil {
		t.Fatal(err)
	}
}

func TestMetaService_Subscriptions_Drop(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a database to use
	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// DROP SUBSCRIPTION returns ErrSubscriptionNotFound when the
	// subscription is unknown.
	err := c.DropSubscription("db0", "default", "foo")
	if got, exp := err, meta.ErrSubscriptionNotFound; got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got, exp)
	}

	// Create a subscription.
	if err := c.CreateSubscription("db0", "default", "sub0", "ALL", []string{"udp://example.com:9090"}); err != nil {
		t.Fatal(err)
	}

	// DROP SUBSCRIPTION returns an influxdb.ErrDatabaseNotFound when
	// the database is unknown.
	err = c.DropSubscription("foo", "default", "sub0")
	if got, exp := err, influxdb_cluster.ErrDatabaseNotFound("foo"); got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got, exp)
	}

	// DROP SUBSCRIPTION returns an influxdb.ErrRetentionPolicyNotFound
	// when the retention policy is unknown.
	err = c.DropSubscription("db0", "foo_policy", "sub0")
	if got, exp := err, influxdb_cluster.ErrRetentionPolicyNotFound("foo_policy"); got.Error() != exp.Error() {
		t.Fatalf("got: %s, exp: %s", got, exp)
	}

	// DROP SUBSCRIPTION drops the subsciption if it can find it.
	err = c.DropSubscription("db0", "default", "sub0")
	if got := err; got != nil {
		t.Fatalf("got: %s, exp: %v", got, nil)
	}
}

func TestMetaService_Shards(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      2,
		Host:    "foo:8180",
		TCPHost: "bar:8281",
	}

	if _, err := c.CreateDataNode(exp.Host, exp.TCPHost); err != nil {
		t.Fatal(err)
	}

	if _, err := c.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	// Test creating a shard group.
	tmin := time.Now()
	sg, err := c.CreateShardGroup("db0", "default", tmin)
	if err != nil {
		t.Fatal(err)
	} else if sg == nil {
		t.Fatalf("expected ShardGroup")
	}

	// Test pre-creating shard groups.
	dur := sg.EndTime.Sub(sg.StartTime) + time.Nanosecond
	tmax := tmin.Add(dur)
	if err := c.PrecreateShardGroups(tmin, tmax); err != nil {
		t.Fatal(err)
	}

	// Test finding shard groups by time range.
	groups, err := c.ShardGroupsByTimeRange("db0", "default", tmin, tmax)
	if err != nil {
		t.Fatal(err)
	} else if len(groups) != 2 {
		t.Fatalf("wrong number of shard groups: %d", len(groups))
	}

	// Test finding shard owner.
	db, rp, owner := c.ShardOwner(groups[0].Shards[0].ID)
	if db != "db0" {
		t.Fatalf("wrong db name: %s", db)
	} else if rp != "default" {
		t.Fatalf("wrong rp name: %s", rp)
	} else if owner.ID != groups[0].ID {
		t.Fatalf("wrong owner: exp %d got %d", groups[0].ID, owner.ID)
	}

	// Test deleting a shard group.
	if err := c.DeleteShardGroup("db0", "default", groups[0].ID); err != nil {
		t.Fatal(err)
	} else if groups, err = c.ShardGroupsByTimeRange("db0", "default", tmin, tmax); err != nil {
		t.Fatal(err)
	} else if len(groups) != 1 {
		t.Fatalf("wrong number of shard groups after delete: %d", len(groups))
	}
}

func TestMetaService_CreateRemoveMetaNode(t *testing.T) {
	t.Parallel()
}

// Ensure that if we attempt to create a database and the client
// is pointed at a server that isn't the leader, it automatically
// hits the leader and finishes the command
func TestMetaService_CommandAgainstNonLeader(t *testing.T) {
	t.Parallel()
}

// Ensure that the client will fail over to another server if the leader goes
// down. Also ensure that the cluster will come back up successfully after restart
func TestMetaService_FailureAndRestartCluster(t *testing.T) {
	t.Parallel()
}

// Ensures that everything works after a host name change. This is
// skipped by default. To enable add hosts foobar and asdf to your
// /etc/hosts file and point those to 127.0.0.1
func TestMetaService_NameChangeSingleNode(t *testing.T) {
	t.Skip("not enabled")
	t.Parallel()
}

func TestMetaService_CreateDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	exp := &meta.NodeInfo{
		ID:      1,
		Host:    "foo:8180",
		TCPHost: "bar:8281",
	}

	n, err := c.CreateDataNode(exp.Host, exp.TCPHost)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(n, exp) {
		t.Fatalf("data node attributes wrong: %v", n)
	}

	nodes, err := c.DataNodes()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(nodes, []meta.NodeInfo{*exp}) {
		t.Fatalf("nodes wrong: %v", nodes)
	}
}

func TestMetaService_DropDataNode(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Dropping a data node with an invalid ID returns an error
	if err := c.DeleteDataNode(0); err == nil {
		t.Fatalf("Didn't get an error but expected %s", cluster_meta.ErrNodeNotFound)
	} else if err.Error() != cluster_meta.ErrNodeNotFound.Error() {
		t.Fatalf("got %v, expected %v", err, cluster_meta.ErrNodeNotFound)
	}

	// Create a couple of nodes.
	n1, err := c.CreateDataNode("foo:8180", "bar:8181")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c.CreateDataNode("foo:8280", "bar:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Create a database and shard group. The default retention policy
	// means that the created shards should be replicated (owned) by
	// both the data nodes.
	if _, err := c.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	sg, err := c.CreateShardGroup("foo", "default", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Dropping the first data server should result in that node ID
	// being removed as an owner of the shard.
	if err := c.DeleteDataNode(n1.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated shard group data from the Meta Store.
	rp, _ := c.RetentionPolicy("foo", "default")
	sg = &rp.ShardGroups[0]

	// The first data node should be removed as an owner of the shard on
	// the shard group
	if !reflect.DeepEqual(sg.Shards[0].Owners, []meta.ShardOwner{
		meta.ShardOwner{
			NodeID: n2.ID},
	}) {
		t.Errorf("owners for shard are %v, expected %v", sg.Shards[0].Owners, []meta.ShardOwner{
			meta.ShardOwner{
				NodeID: 2},
		})
	}

	// The shard group should still be marked as active because it still
	// has a shard with owners.
	if sg.Deleted() {
		t.Error("shard group marked as deleted, but shouldn't be")
	}

	// Dropping the second data node will orphan the shard, but as
	// there won't be any shards left in the shard group, the shard
	// group will be deleted.
	if err := c.DeleteDataNode(n2.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated data.
	rp, _ = c.RetentionPolicy("foo", "default")
	sg = &rp.ShardGroups[0]

	if got, exp := sg.Deleted(), true; got != exp {
		t.Error("Shard group not marked as deleted")
	}
}

//TODO zhexuany this is workaround for rps and rpi. need remove this later
func rpi2rps(rpi *meta.RetentionPolicyInfo) *meta.RetentionPolicySpec {
	rps := meta.RetentionPolicySpec{}
	rps.ReplicaN = &rpi.ReplicaN
	rps.Duration = &rpi.Duration
	rps.Name = rpi.Name
	return &rps
}

func TestMetaService_DropDataNode_Reassign(t *testing.T) {
	t.Parallel()

	d, s, c := newServiceAndClient()
	defer os.RemoveAll(d)
	defer s.Close()
	defer c.Close()

	// Create a couple of nodes.
	n1, err := c.CreateDataNode("foo:8180", "bar:8181")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c.CreateDataNode("foo:8280", "bar:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Create a retention policy with a replica factor of 1.
	rp := meta.NewRetentionPolicyInfo("rp0")
	rp.ReplicaN = 1

	// Create a database using rp0
	if _, err := c.CreateDatabaseWithRetentionPolicy("foo", rpi2rps(rp)); err != nil {
		t.Fatal(err)
	}

	sg, err := c.CreateShardGroup("foo", "rp0", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// Dropping the first data server should result in the shard being
	// reassigned to the other node.
	if err := c.DeleteDataNode(n1.ID); err != nil {
		t.Fatal(err)
	}

	// Retrieve updated shard group data from the Meta Store.
	rp, _ = c.RetentionPolicy("foo", "rp0")
	sg = &rp.ShardGroups[0]

	// There should still be two shards.
	if got, exp := len(sg.Shards), 2; got != exp {
		t.Errorf("there are %d shards, but should be %d", got, exp)
	}

	// The second data node should be the owner of both shards.
	for _, s := range sg.Shards {
		if !reflect.DeepEqual(s.Owners, []meta.ShardOwner{
			meta.ShardOwner{
				NodeID: n2.ID,
			},
		}) {
			t.Errorf("owners for shard are %v, expected %v", s.Owners, []meta.ShardOwner{
				meta.ShardOwner{
					NodeID: n2.ID,
				},
			})
		}
	}

	// The shard group should not be marked as deleted because both
	// shards have an owner.
	if sg.Deleted() {
		t.Error("shard group marked as deleted, but shouldn't be")
	}
}

func TestMetaService_PersistClusterIDAfterRestart(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
}

func TestMetaService_Ping(t *testing.T) {
	cfgs := make([]*cluster_meta.Config, 3)
	srvs := make([]*testService, 3)
	joinPeers := freePorts(len(cfgs))

	var swg sync.WaitGroup
	swg.Add(len(cfgs))

	for i, _ := range cfgs {
		c := newConfig()
		c.HTTPBindAddress = joinPeers[i]
		cfgs[i] = c

		srvs[i] = newService(c)
		go func(i int, srv *testService) {
			defer swg.Done()
			if err := srv.Open(); err != nil {
				t.Fatalf("error opening server %d: %s", i, err)
			}
		}(i, srvs[i])
		defer srvs[i].Close()
		defer os.RemoveAll(c.Dir)
	}

	swg.Wait()

	c := cluster_meta.NewClient(cfgs[0])
	c.SetMetaServers(joinPeers)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Ping(false); err != nil {
		t.Fatalf("ping false all failed: %s", err)
	}
	if err := c.Ping(true); err != nil {
		t.Fatalf("ping false true failed: %s", err)
	}

	srvs[1].Close()
	// give the server time to close
	time.Sleep(time.Second)

	if err := c.Ping(false); err != nil {
		t.Fatalf("ping false some failed: %s", err)
	}

	if err := c.Ping(true); err == nil {
		t.Fatal("expected error on ping")
	}
}

func TestMetaService_AcquireLease(t *testing.T) {
	t.Parallel()

	d, s, c1 := newServiceAndClient()
	c2 := newClient(s)
	defer os.RemoveAll(d)
	defer s.Close()
	defer c1.Close()
	defer c2.Close()

	n1, err := c1.CreateDataNode("foo1:8180", "bar1:8281")
	if err != nil {
		t.Fatal(err)
	}

	n2, err := c2.CreateDataNode("foo2:8180", "bar2:8281")
	if err != nil {
		t.Fatal(err)
	}

	// Client 1 acquires a lease.  Should succeed.
	l, err := c1.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n1.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n1.ID, l.Owner)
	}

	t.Logf("c1: %d, c2: %d", c1.NodeID(), c2.NodeID())
	// Client 2 attempts to acquire the same lease.  Should fail.
	l, err = c2.AcquireLease("foo")
	if err == nil {
		t.Fatal("expected to fail because another node owns the lease")
	}

	// Wait for Client 1's lease to expire.
	time.Sleep(1 * time.Second)

	// Client 2 retries to acquire the lease.  Should succeed this time.
	l, err = c2.AcquireLease("foo")
	if err != nil {
		t.Fatal(err)
	} else if l == nil {
		t.Fatal("expected *Lease")
	} else if l.Name != "foo" {
		t.Fatalf("lease name wrong: %s", l.Name)
	} else if l.Owner != n2.ID {
		t.Fatalf("owner ID wrong. exp %d got %d", n2.ID, l.Owner)
	}
}

// newServiceAndClient returns new data directory, *Service, and *Client or panics.
// Caller is responsible for deleting data dir and closing client.
func newServiceAndClient() (string, *testService, *cluster_meta.Client) {
	cfg := newConfig()
	s := newService(cfg)
	if err := s.Open(); err != nil {
		panic(err)
	}

	c := newClient(s)

	return cfg.Dir, s, c
}

// newClient will create a meta client and also open it
func newClient(s *testService) *cluster_meta.Client {
	cfg := newConfig()
	c := cluster_meta.NewClient(cfg)
	c.SetMetaServers([]string{s.HTTPAddr()})
	if err := c.Open(); err != nil {
		panic(err)
	}
	return c
}

func newConfig() *cluster_meta.Config {
	cfg := cluster_meta.NewConfig()
	cfg.BindAddress = "127.0.0.1:0"
	cfg.HTTPBindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
	cfg.LeaseDuration = toml.Duration(1 * time.Second)
	return cfg
}

func testTempDir(skip int) string {
	// Get name of the calling function.
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())
	// Make a temp dir prefixed with calling function's name.
	dir, err := ioutil.TempDir(os.TempDir(), prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

type testService struct {
	*cluster_meta.Service
	ln net.Listener
}

func (t *testService) Close() error {
	if err := t.Service.Close(); err != nil {
		return err
	}
	return t.ln.Close()
}

func newService(cfg *cluster_meta.Config) *testService {
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	if err != nil {
		panic(err)
	}
	s := cluster_meta.NewService(cfg)
	s.Node = influxdb_cluster.NewNode(cfg.Dir)
	s.RaftListener = mux.Listen(cluster_meta.MuxHeader)

	go mux.Serve(ln)

	return &testService{Service: s, ln: ln}
}

func mustParseStatement(s string) influxql.Statement {
	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	return stmt
}

func mustMarshalJSON(v interface{}) string {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return string(b)
}

func freePort() string {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().String()
}

func freePorts(i int) []string {
	var ports []string
	for j := 0; j < i; j++ {
		ports = append(ports, freePort())
	}
	return ports
}
