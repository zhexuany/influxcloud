package cluster

// ShardDeleter is a wrapper of TSDBStore which can
// delete shard from disk
type ShardDeleter struct {
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}
}

// NewShardDeleter will return a ShardDeleter instance
func NewShardDeleter() *ShardDeleter {
	return &ShardDeleter{}
}

// ShardIDs will return all shards' ID in this node
func (d ShardDeleter) ShardIDs() []uint64 {
	return d.TSDBStore.ShardIDs()
}

// DeleteShard will delete a shard according to shardID
// if failed, then return error
func (d ShardDeleter) DeleteShard(shardID uint64) error {
	return d.TSDBStore.DeleteShard(shardID)
}
