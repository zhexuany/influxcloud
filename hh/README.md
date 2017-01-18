# Hinted handoff


Hinted Handoff is an optional part of writes in cluster, enabled by default, with two purposes:

1. Hinted handoff allows influxdb to offer full write availability when consistency is not required.
2. Hinted handoff dramatically improves response consistency after temporary outages such as network failures.

##  How it works

When a write is performed and a replica node for the row is either known to be down ahead of time, or does not respond to the write request, the coordinator will store a hint locally, in the `hh` directory. This hint is basically a wrapper around the mutation indicating that it needs to be replayed to the unavailable node(s).

Once a node discovers via gossip that a node for which it holds hints has recovered, it will send the data(known as `Point` in inlfluxdb) corresponding to each hint to the target data node. 

## How data stroed inside hinted handoff
each segment has the following structue in disk.
~~~
 ┌──────────────────────────┐ ┌──────────────────────────┐ ┌────────────┐
 │         Block 1          │ │         Block 2          │ │   Footer   │
 └──────────────────────────┘ └──────────────────────────┘ └────────────┘
 ┌────────────┐┌────────────┐ ┌────────────┐┌────────────┐ ┌────────────┐
 │Block 1 Len ││Block 1 Body│ │Block 2 Len ││Block 2 Body│ │Head Offset │
 │  8 bytes   ││  N bytes   │ │  8 bytes   ││  N bytes   │ │  8 bytes   │
 └────────────┘└────────────┘ └────────────┘└────────────┘ └────────────┘
~~~

queue is a bounded, disk-backed, append-only type that combines queue and
 log semantics.  byte slices can be appended and read back in-order.
 The queue maintains a pointer to the current head
 byte slice and can re-read from the head until it has been advanced.

~~~
 ┌─────┐
 │Head │
 ├─────┘
 │
 ▼
 ┌─────────────────┐ ┌─────────────────┐┌─────────────────┐
 │Segment 1 - 10MB │ │Segment 2 - 10MB ││Segment 3 - 10MB │
 └─────────────────┘ └─────────────────┘└─────────────────┘
                                                          ▲
                                                          │
                                                          │
                                                     ┌─────┐
                                                     │Tail │
                                                     └─────┘
~~~

## How hinted handoff perfrom write requests
~~~go
type shardWriter interface {
	WriteShard(shardID, ownerID uint64, points []models.Point) error
}

type metaClient interface {
	DataNode(id uint64) (ni *meta.NodeInfo, err error)
}
~~~

`shardWriter` will write points into a data node whose node is is owner id. This must be done via rpc call. This requires tcp host infor which can be achieved from `metaClient`.

## How data stored in dish in hinted handoff system
For any incoming write request, if coordinator detect some request failed to write to some node(this is realted with consistency level), corrdinator node will forward 
such request into hinted handoff service. When hinted handoff receieved that request, it will write `shardID` and all `Points` into disk file(known as segment). 
`ownerID` is the name of the directory in `hh`.
