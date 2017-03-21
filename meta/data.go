package meta

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/zhexuany/influxdb-cluster/meta/internal"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	// DefaultRetentionPolicyReplicaN is the default value of RetentionPolicyInfo.ReplicaN.
	DefaultRetentionPolicyReplicaN = 1

	// DefaultRetentionPolicyDuration is the default value of RetentionPolicyInfo.Duration.
	DefaultRetentionPolicyDuration = time.Duration(0)

	// DefaultRetentionPolicyName is the default name for auto generated retention policies.
	DefaultRetentionPolicyName = "autogen"

	// MinRetentionPolicyDuration represents the minimum duration for a policy.
	MinRetentionPolicyDuration = time.Hour
)

// Data represents the top level collection of all metadata.
type Data struct {
	MetaNodes NodeInfos
	DataNodes NodeInfos
	Users     []UserInfo
	MaxNodeID uint64
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data
	//copy meta nodes
	if data.MetaNodes != nil {
		other.MetaNodes = make([]NodeInfo, len(data.MetaNodes))
		for i := range data.MetaNodes {
			other.MetaNodes[i] = data.MetaNodes[i].clone()
		}
	}

	// Copy data nodes.
	if data.DataNodes != nil {
		other.DataNodes = make([]NodeInfo, len(data.DataNodes))
		for i := range data.DataNodes {
			other.DataNodes[i] = data.DataNodes[i].clone()
		}
	}
	//copy users information
	other.Users = data.CloneUsers()

	return &other
}

// NodeInfos is a slice of NodeInfo used for sorting
type NodeInfos []NodeInfo

func (n NodeInfos) Len() int           { return len(n) }
func (n NodeInfos) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n NodeInfos) Less(i, j int) bool { return n[i].ID < n[j].ID }

// NodeInfo represents information about a single node in the cluster.
type NodeInfo struct {
	ID                 uint64
	Host               string
	TCPHost            string
	PendingShardOwners uint64arr
}

// clone returns a deep copy of ni.
func (ni NodeInfo) clone() NodeInfo { return ni }

// marshal serializes to a protobuf representation.
func (ni NodeInfo) marshal() *internal.NodeInfo {
	pb := &internal.NodeInfo{}
	pb.ID = proto.Uint64(ni.ID)
	pb.Host = proto.String(ni.Host)
	pb.TCPHost = proto.String(ni.TCPHost)
	pb.PendingShardOwners = make(uint64arr, len(ni.PendingShardOwners))
	for _, pso := range ni.PendingShardOwners {
		pb.PendingShardOwners = append(pb.PendingShardOwners, *proto.Uint64(pso))
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (ni *NodeInfo) unmarshal(pb *internal.NodeInfo) {
	ni.ID = pb.GetID()
	ni.Host = pb.GetHost()
	ni.TCPHost = pb.GetTCPHost()
	ni.PendingShardOwners = pb.GetPendingShardOwners()
}

func (data *Data) MetaNode(id uint64) *NodeInfo {
	for i := range data.MetaNodes {
		if data.MetaNodes[i].ID == id {
			return &data.MetaNodes[i]
		}
	}
	return nil
}

func (data *Data) CreateMetaNode(host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpHost {
			return ErrNodeExists
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	pendingShardOwners := make(uint64arr, 0)
	data.MetaNodes = append(data.MetaNodes, NodeInfo{
		ID:                 existingID,
		Host:               host,
		TCPHost:            tcpHost,
		PendingShardOwners: pendingShardOwners,
	})
	sort.Sort(NodeInfos(data.MetaNodes))

	return nil

}

// SetMetaNode adds a meta node with a pre-specified nodeID.
func (data *Data) SetMetaNode(nodeID uint64, host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.MetaNodes {
		if n.Host == host {
			return ErrNodeExists
		}
	}

	// Append new node.
	pendingShardOwners := make(uint64arr, 0)
	data.MetaNodes = append(data.MetaNodes, NodeInfo{
		ID:                 nodeID,
		Host:               host,
		TCPHost:            tcpHost,
		PendingShardOwners: pendingShardOwners,
	})

	return nil
}

func (data *Data) DeleteMetaNode(id uint64) error {
	var nodes []NodeInfo

	// Remove the data node from the store's list.
	for _, n := range data.MetaNodes {
		if n.ID != id {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == len(data.MetaNodes) {
		return ErrNodeNotFound
	}

	data.MetaNodes = nodes

	// Remove node id from all shard infos
	for di, d := range data.Data.Databases {
		for ri, rp := range d.RetentionPolicies {
			for sgi, sg := range rp.ShardGroups {
				var (
					nodeOwnerFreqs = make(map[int]int)
					orphanedShards []meta.ShardInfo
				)
				// Look through all shards in the shard group and
				// determine (1) if a shard no longer has any owner
				// (orphaned); (2) if all shards in the shard group
				// are orphaned; and (3) the number of shards in this
				// group owned by each data node in the cluster.
				for si, s := range sg.Shards {
					// Track of how many shards in the group are
					// owned by each data node in the cluster.
					var nodeIdx = -1
					for i, owner := range s.Owners {
						if owner.NodeID == id {
							nodeIdx = i
						}
						nodeOwnerFreqs[int(owner.NodeID)]++
					}

					if nodeIdx > -1 {
						// Data node owns shard, so relinquish ownerhip
						// and set new owner on the shard.
						s.Owners = append(s.Owners[:nodeIdx], s.Owners[nodeIdx+1:]...)
						data.Data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards[si].Owners = s.Owners
					}

					// Shard no longer owned. Will need reassigning
					// an owner.
					if len(s.Owners) == 0 {
						orphanedShards = append(orphanedShards, s)
					}
				}

				// Mark the shard group as deleted if it has no shards,
				// or all of its shards are orphaned.
				if len(sg.Shards) == 0 || len(orphanedShards) == len(sg.Shards) {
					data.Data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].DeletedAt = time.Now().UTC()
					continue
				}

				// Reassign any orphaned shards. Delete the node we're
				// dropping from the list of potential new owner.
				delete(nodeOwnerFreqs, int(id))

				// for _, orphan := range orphanedShards {
				// newOwnerID, err := newShardOwner(orphan, nodeOwnerFreqs)
				// if err != nil {
				// 	return err
				// }

				// for si, s := range sg.Shards {
				// 	if s.ID == orphan.ID {
				// 		sg.Shards[si].Owners = append(sg.Shards[si].Owners, ShardOwner{NodeID: newOwnerID})
				// 		data.Data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards = sg.Shards
				// 		break
				// 	}
				// }
				// }
			}
		}
	}
	return nil
}

// DataNode returns a node by id.
func (data *Data) DataNode(id uint64) *NodeInfo {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			return &data.DataNodes[i]
		}
	}
	return nil
}

// CreateDataNode adds a node to the metadata.
func (data *Data) CreateDataNode(host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpHost {
			return ErrNodeExists
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, NodeInfo{
		ID:      existingID,
		Host:    host,
		TCPHost: tcpHost,
	})
	sort.Sort(NodeInfos(data.DataNodes))

	return nil
}

func (data *Data) UpdateDataNode(nodeID uint64, host, tcpHost string) error {
	for _, n := range data.DataNodes {
		if n.ID == nodeID {
			n.Host = host
			n.TCPHost = tcpHost
			break
		}
	}
	return nil
}

// DeleteDataNode removes a node from the Meta store.
//
// If necessary, DeleteDataNode reassigns ownerhip of any shards that
// would otherwise become orphaned by the removal of the node from the
// cluster.
func (data *Data) DeleteDataNode(id uint64) error {
	var nodes []NodeInfo

	// Remove the data node from the store's list.
	for _, n := range data.DataNodes {
		if n.ID != id {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == len(data.DataNodes) {
		return ErrNodeNotFound
	}
	data.DataNodes = nodes

	return nil
}
func (data *Data) MarshalBinary() ([]byte, error) {
	return proto.Marshal(data.marshal())
}

// marshal serializes to a protobuf representation.
func (data *Data) marshal() *internal.ClusterData {
	pb := &internal.ClusterData{}

	pb.Data, _ = data.Data.MarshalBinary()

	pb.MetaNodes = make([]*internal.NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		pb.MetaNodes[i] = data.MetaNodes[i].marshal()
	}

	pb.DataNodes = make([]*internal.NodeInfo, len(data.DataNodes))
	for i := range data.DataNodes {
		pb.DataNodes[i] = data.DataNodes[i].marshal()
	}

	pb.Users = make([]*internal.UserInfo, len(data.Users))

	return pb
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
	var pb internal.ClusterData
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	data.unmarshal(&pb)
	return nil
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) unmarshal(pb *internal.ClusterData) {
	data.Data = &meta.Data{}
	data.Data.UnmarshalBinary(pb.GetData())

	data.MetaNodes = make([]NodeInfo, len(pb.GetMetaNodes()))
	for i, meta := range pb.GetMetaNodes() {
		data.MetaNodes[i].unmarshal(meta)
	}
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (data *Data) CreateShardGroup(database, policy string, timestamp time.Time) error {
	// Ensure there are nodes in the metadata.
	if len(data.DataNodes) == 0 {
		return nil
	}

	// Find retention policy.
	rpi, err := data.Data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	} else if rpi == nil {
		return influxdb.ErrRetentionPolicyNotFound(policy)
	}

	// Verify that shard group doesn't already exist for this timestamp.
	if rpi.ShardGroupByTimestamp(timestamp) != nil {
		return nil
	}

	// Require at least one replica but no more replicas than nodes.
	replicaN := rpi.ReplicaN
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > len(data.DataNodes) {
		replicaN = len(data.DataNodes)
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := len(data.DataNodes) / replicaN

	//TODO finished generatedShards
	// Create the shard group.
	data.Data.MaxShardGroupID++
	sgi := meta.ShardGroupInfo{}
	sgi.ID = data.Data.MaxShardGroupID
	sgi.StartTime = timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(rpi.ShardGroupDuration).UTC()

	sgi.Shards = data.generatedShards(shardN)
	// Assign data nodes to shards via round robin.
	// Start from a repeatably "random" place in the node list.
	nodeIndex := int(data.Data.Index % uint64(len(data.DataNodes)))
	for i := range sgi.Shards {
		si := &sgi.Shards[i]
		for j := 0; j < replicaN; j++ {
			nodeID := data.DataNodes[nodeIndex%len(data.DataNodes)].ID
			si.Owners = append(si.Owners, meta.ShardOwner{NodeID: nodeID})
			nodeIndex++
		}
	}

	// Retention policy has a new shard group, so update the policy. Shard
	// Groups must be stored in sorted order, as other parts of the system
	// assume this to be the case.
	rpi.ShardGroups = append(rpi.ShardGroups, sgi)
	sort.Sort(meta.ShardGroupInfos(rpi.ShardGroups))

	return nil
}

func (data *Data) gcd() {

}

func (data *Data) generatedShards(shardN int) []meta.ShardInfo {
	// Create shards on the group.
	shards := make([]meta.ShardInfo, shardN)
	for i := range shards {
		data.Data.MaxShardID++
		shards[i] = meta.ShardInfo{ID: data.Data.MaxShardID}
	}

	return shards
}

func (data *Data) TruncateShardsGrops(sg *meta.ShardGroupInfo) error {
	return nil
}

func (data *Data) AddPendingShardOwner(id uint64) {
	for _, node := range data.MetaNodes {
		node.PendingShardOwners = append(node.PendingShardOwners, id)
	}
}

func (data *Data) RemovePendingShardOwner(id uint64) {
	for _, node := range data.MetaNodes {
		newPso := uint64arr{}
		for _, pso := range node.PendingShardOwners {
			if id != pso {
				newPso = append(newPso, pso)
			}
		}
		node.PendingShardOwners = newPso
	}
}

type ShardOwners []meta.ShardOwner

func (so ShardOwners) Len() int {
	return len(so)
}

func (so ShardOwners) Less(i, j int) bool {
	return so[i].NodeID < so[j].NodeID
}

func (so ShardOwners) Swap(i, j int) {
	so[i], so[j] = so[j], so[i]
}

//ShardLocation return NodeInfos which is the o of the Shard
func (data *Data) ShardLocation(shardID uint64) (*meta.ShardInfo, error) {
	for _, dbi := range data.Data.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sg := range rpi.ShardGroups {
				for _, s := range sg.Shards {
					//found such shards, return shards
					if s.ID == shardID {
						return &s, nil
					}
				}
			}
		}
	}
	//does not find any shards assoicated with this shardID, just reutn nil, error
	return nil, fmt.Errorf("failed to find shards assoicated with %d", shardID)
}

// UpdateShard will update ShardOwner of a Shard according to ShardID
func (data *Data) UpdateShard(shardID uint64, newOwners []meta.ShardOwner) error {
	return fmt.Errorf("Failed to find Shard assoicated with shard ID %d", shardID)
}

// AddShardOwner will update a shards labelled by shardID in this node if such shards ownby this newly adding node
func (data *Data) AddShardOwner(shardID, nodeID uint64) error {
	si, err := data.ShardLocation(shardID)
	if err == nil {
		if !si.OwnedBy(nodeID) {
			if nodeID > data.MaxNodeID {
				return nil
			}
			o := ShardOwners{}
			o = append(o, meta.ShardOwner{NodeID: nodeID})
			sort.Sort(o)
			return data.UpdateShard(shardID, o)
		}
	}
	return err
}

// RemoveShardOwner will remove all shards in this node if such shard owned by this node
func (data *Data) RemoveShardOwner(shardID, nodeID uint64) error {
	si, err := data.ShardLocation(shardID)
	if err != nil {
		if si.OwnedBy(nodeID) {
			o, _ := data.PruneShard(si, nodeID)
			data.UpdateShard(shardID, o)
		}
	}
	return err
}

func (data *Data) PruneShard(si *meta.ShardInfo, nodeID uint64) ([]meta.ShardOwner, error) {
	found := -1
	for i, o := range si.Owners {
		if o.NodeID == nodeID {
			found = i
			break
		}
	}

	if found != -1 {
		copy(si.Owners[found:], si.Owners[found+1:])
		// si.Owners[len(si.Owners)-1] = nil
		// si.Owners = si.Owners[:len(si.Owners)-1]
		return si.Owners, nil
	}
	return nil, fmt.Errorf("failed to find shard owner %d", nodeID)
}

//TODO all role can be waited until cluster works fine
func (data *Data) CreateRole() error {
	return nil
}

func (data *Data) DropRole(role RoleInfo) error {
	// ridx := -1
	// for i, _ := range data.Roles {
	// 	if role.Users == data.Roles[i].Users {
	// 		ridx = i
	// 		break
	// 	}
	// }

	// if ridx != -1 {
	// 	return errors.New("role not found")
	// }
	// data.Rol
	// es = copy
	return nil
}

func (data *Data) Role(name string) *RoleInfo {
	// for i := rang data.Roles {
	// 	if data.Roles[i].Name == name {
	// 		return &data.Roles[i]
	// 	}
	// }
	return nil
}

func (data *Data) role() {

}

func (data *Data) AddRoleUsers() {
}

func (data *Data) RemoveRoleUsers() {
	//roles.RemoveUsers
}

func (data *Data) AddRolePermissions() {

}

func (data *Data) RemoveRolePermissions() {

}

func (data *Data) ChangeRoleName() {

}

// User returns a user by username.
func (data *Data) User(username string) *UserInfo {
	for i := range data.Users {
		if data.Users[i].Name == username {
			return &data.Users[i]
		}
	}
	return nil
}
func (data *Data) user() {

}

// CreateUser creates a new user.
func (data *Data) CreateUser(name, hash string, admin bool) error {
	// Ensure the user doesn't already exist.
	if name == "" {
		return ErrUsernameRequired
	} else if data.User(name) != nil {
		return ErrUserExists
	}

	// Append new user.
	data.Users = append(data.Users, UserInfo{
		UserInfo: meta.UserInfo{
			Name:  name,
			Hash:  hash,
			Admin: admin,
		},
	})

	return nil
}

// DropUser removes an existing user by name.
func (data *Data) DropUser(name string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users = append(data.Users[:i], data.Users[i+1:]...)
			return nil
		}
	}
	return ErrUserNotFound
}

func (data *Data) SetUserPassword(pass string) error {
	return nil
}

func (data *Data) AddUserPermissions() error {
	return nil
}

//TODO finish this until we have a demo to run
func (data *Data) ImportData(buf []byte) error {
	// other := Data{}
	// if err := other.UnmarshalBinary(buf); err != nil {
	// 	return err
	// }

	// // Restrict(other)
	// for dbidx, db := range data.Data.Databases {
	// 	dbn := other.Database(db.Name)
	// 	if dbn == nil {
	// 		if err = other.CreateDatabase(db.Name); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	for _, rpi := range db.RetentionPolicies {
	// 		other.CreateRetentionPolicy(dbn.Name, dbn.RetentionPolicy(rpi.Name))
	// 		data.generatedShards(rpi.ShardGroups)
	// 	}

	// }
	//sort
	//call gcd
	return nil
}

//TODO (zhexuany) do not worry about this until alpha version
type UserInfo struct {
	meta.UserInfo
	Privileges ScopedPermissions
}

func (u *UserInfo) unmarshal(pb internal.UserInfo) error {
	// return u.Privileges.unmarshal()
	return nil
}

type uint64arr []uint64

func (u uint64arr) Len() int {
	return len(u)
}

func (u uint64arr) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u uint64arr) Less(i, j int) bool {
	return u[i] < u[j]
}
