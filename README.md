# influxdb-cluster

## What is this prototype can do? 
- edit meta node including adding, updating and deleting 
- edit data node including adding, updating and deleting
- distributed data in a basic form of shards across nodes in cluster via endpoint /wrtie
- distributed query across cluster via endpoint /query 

## What is this prototype can not do but will add in future?
- Raft Algorithm Optimization
- Performance improvement
- Provide a way expand Shades inside ShardGroup. For now, once ShardGroup is created, there is no way that we can expand the size of Shards which means the capacity of writing will be limited.
- Add User and Role Creation and Authorization.
- Added Command that can know the status of cluster
- Enable/Disable cluster tracing. i.e: Node 0.0.0.0:8090[Follower]
- Bring docker into build
- add integration framework based on etcd's work.
- Buffer failed write into disk and retry until such buffer is empty. In order to improve the usage of disk, we need clean such buffer in some manner.
- Provide a way that can backup a cluster and restore it later.

## How to build
Well, you do not need worry this in a month. The prototype is still under implementing. But we promise, we will try hard to get things done quickly.

## Architeture
Please check the architeture.md for more details. Any inquiry and comments are welcome. You can find my email in my github's profile.

## 
