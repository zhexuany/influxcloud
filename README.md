# influxdb-cluster
In order to improve the availability of influxDB, we create this project. The original plan is to replicate influxdata's cluster, but this is not good 
if influxdata changes a lot interface and method signautre. Hence, we change our plan and decide not to intrude influxDB too much. Please think this as a plugin, we will extract 
some code from influxDB for our own benefit.

## Architure 
    ┌───────┐     ┌───────┐      
    │       │     │       │      
    │ node1 │◀───▶│ node2 │      
    │       │     │       │      
    └───────┘     └───────┘      
        ▲             ▲          
        │             │          
        │  ┌───────┐  │          
        │  │       │  │          
        └─▶│ node3 │◀─┘          
           │       │             
           └───────┘          
           
In a cluster, we will have some nodes. The metadata are synchorized via Raft algorithm. The metadata includes:
1. Meta nodes hold all of the following meta data
2. all nodes in the cluster and their role
3. all databases and retention policies that exist in the cluster
4. all shards and shard groups, and on what nodes they exist
5. all continuous queries

The data node part will store the following data:
1. measurements
2. tag keys and values
3. field keys and values

Compared with influxdata's plan, our want to meta and data node reside in one machine. In futrue, we may separate them for better performance. But for now, it is the best start 
point. A large projet should evolve on the way of growing. 

### How write work in this cluster?
In a cluster, write is not easy to answer. It is a hard question. The consistency level and replication factor will affect this. For now, our primary job
 is releasing a prototype that can solve our performance issue.

Before write points into `influxDB`, we first know how many nodes that thhese points should write into and where thest nodes are. These two operation should be done in node part. 


All data stored in cluster as a unit called `Shard`. `Shard` will be distributed and replicated across different nodes. The basic rule for know this write belong to which node 
is 


~~~
points ----> any node in cluster ----->merge points  ----> if it leader --------> find nodes  -> buffer write in hinted handoff
                                                             |
                                                             |                          |
                                                             |----------------->forward to leader 
~~~
                                         
When points arrives `influxDB` either in tcp or udp, it can handle this on its own. We do not need worry about this.

#### The rule about merge points
Speaking about the merge rules, we need bring `Shards Groups` into.
##### Shards Groups
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

When a batch points arrives, we apply the rule we just described above to find the correct `shard`  and write data into disk.

### How query work in this cluster?

Query in a cluster are distributed based on time range being queried and the replication factor of the data. For example if the retention policy has a replication factor of 4, the coordinating data node receiving the query randomly picks any of the 4 data nodes that store a replica of the shard(s) to receive the query. If we assume that 
the system has shard durations of one day, then for each day of time covered by a query the coordinating node will select one data node to receive the query for that day. The coordinating node will execute and fulfill the query locally whenever possible. If a query must scan multiple shard groups (multiple days in our example above), the coordinating node will will forward queries to other nodes for shard(s) it does not have locally. The queries are forwarded in parallel to scanning its own local data. The queries are distributed to as many nodes as required to query each shard group once. As the results come back from each data node, the coordinating data node combines them into the final result that gets returned to the user.
