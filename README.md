# This project is still under developing
# influxdb-cluster
Meta node only communicates wilt meta node. Data node can not just communicate with data node but also can communicate with meta node. A plugin for influxdb which make it distributed. In this framework, we have two different nodes. Then main part of this sytem 
is `Meta` and second part of this system is `Data`. They both have different role 
in this system. 

## Meta Nodes

Meta nodes hold the following things as meta data:

1. all nodes' url and their role( meta or data). 

2. All databases and tetention policies that existe in the cluster

3. All shards and shardGroups, and on what nodes exist. To be simpple, ShardLocation
need stored in meta node.

4. Cluster users and their permissions

5. All continuous queries.

## Data Nodes

1. Measurements
2. Tags keys and values
3. Field Keys and Values

All the data is organized b `database/rention_policy/shard_id`. By default, the parent 
directory of this is `/var/lib/influxdb/data`

## Writes in a Cluster
### introduction
Storgae in a clustr is not a trival question to answer. 
We have to deal with replication factor which brings the high availability to 
system even for node crashed or network participation. 
### Shards Groups
All data stored in cluster as a form of `Shards`.  Consering the replication factor is `x` and there is
`n` node available in cluster. If we assume there are `m` shrads has to be written into cluster, then for every node, `nm/x` shards
will be written into cluster. 

When a write comes in with values that have a timestamp, we first determine which `ShardGroup` that this write goes to. After this, 
we take the concatatention of `measurement` and `tagset` as out key and hash such key for bucketing into the correct shard. In Go, it will
be the following.

~~~go
// key is measurement + tagset
// shardGroup is the group for the values based on timestamp
// hash with fnv and then bucket
shard := shardGroup.shards[fnv.New64a(key) % len(shardGroup.Shards)]
~~~

There are multiple implications to this scheme for determining where data lives in a cluster. 
First, for any given metaseries all data on any given day will exist in a single shard, and 
thus only on those servers hosting a copy of that shard. Second, once a shard group is created, 
adding new servers to the cluster won’t scale out write capacity for that shard group. 

The replication is fixed when the shard group is created. However, there is a method for expanding 
writes in the current shard group (i.e. today) when growing a cluster. 
The current shard group can be truncated to stop at the current time using `influxd-ctl truncate-shards`. 
This immediately closes the current shard group, forcing a new shard group to be created. 
That new shard group will inherit the latest retention policy and data node changes and 
will then copy itself appropriately to the newly available data nodes. 
Run `influxd-ctl truncate-shards help` for more information on the command.


## Queries in a Cluster
Queries in a cluster are distributed based on the time range being queried and the replication factor of the data. 
For example if the retention policy has a replication factor of 4, the coordinating data node receiving the query 
randomly picks any of the 4 data nodes that store a replica of the shard(s) to receive the query. If we assume that 
the system has shard durations of one day, then for each day of time covered by a query the coordinating node will 
select one data node to receive the query for that day. The coordinating node will execute and fulfill the query 
locally whenever possible. If a query must scan multiple shard groups (multiple days in our example above), 
the coordinating node will will forward queries to other nodes for shard(s) it does not have locally. 
The queries are forwarded in parallel to scanning its own local data. The queries are distributed to as 
many nodes as required to query each shard group once. As the results come back from each data node, 
the coordinating data node combines them into the final result that gets returned to the user.





UpdateShard is called by AddShardOwner. 

UpdateShard is called by AddShardOwner. Additionally, applyAddShardOwnerCommand call the AddShardOwner in raft_store.go. 
I guess UpdateShard is actually update all shards according to the information in cluster.  

~~~go
RetentionPolicyInfo
 	Name
	ReplicaN
	Duration
	ShardGroupDuration 
	ShardGroups []ShardGroupInfo

ShardGroupDuration will be used during creation of ShardGroups. The snippet is the following:
	sg := ShardGroupInfo{}
	sg.StartTime = timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	sg.EndTime = timestamp.Add(rpi.ShardGroupDuration).UTC()
	// after set endtime, we need check whether this end time exceeds the MaxNanotime in system.

ShardGroupInfo
	ID
	StartTime 
	EndTime
	DeletedAt
	ShardsInfo[]
	TruncatedAt
	
ShardInfo
	ID 
	ShardOwner

ShardOwner
	NodeID

applyAddShardOwnerCommand -> AddShardOwner  -> if success, then UpdateShard
~~~

For single node, the shardOwner is always the node itself. But for cluster version, that is not true at all. 

For a cluster system, we will have nodes which will join or leave cluster. when these operation happens. we need add shardOwner. 

2 nodes with replicating factor 1, then for each shardGroup, such shardGroup will create 2 shards. 1, 2. 1 -> a. 2 -> b. 
If replication factor is 2, then for each shardGroup, it only need created 1 shards.i.e. 1 -> a, 1-> b. 

The size of Shards is determined by replication factor and data nodes. The formula for it is s = N/X where n is the number of shards and X is the replication factor. If the replication factor is 4 here in this case and the number of data node is 4. Each shardGroup will only have 1 shard. 1 -> a, 1 -> b, 1 -> 3
1 -> 4. We can just randomly pick one data nodes and let it receive such query. If number of data node is 8 here. The shards in single shard group will increase to 2. (this is actually similar with 2 4 cases.) 1 -> a, 1 -> b, 1 -> c 1-> d; 2 ->e 2-> f 2-> e 2-> g 2-> h.
The ideal config is that we have 10 nodes and the replication factor is just 2. Then we need create 5 shards inside one shardsGroup. 


when we try to write into this cluster system, we take the measurement and tag set as our key. So we know which shards that this write should go to. 

Queries in a Cluster Design: 


## Roles
	Roles are groups of permissions. A single role can belong to several cluster accounts. Only web console Admin users can manage roles.InfluxEnterprise clusters have two built-in roles:

## Permissions
https://docs.influxdata.com/enterprise/v1.1/features/users/#permissions


## OSS to Cluster migration.
https://docs.influxdata.com/enterprise/v1.1/guides/migration/


## TODO 
1. All roles
2. All permissions issue
