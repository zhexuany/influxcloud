package cluster

import (
	"net"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxdb-cluster/rpc"
	"github.com/zhexuany/influxdb-cluster/tlv"
	"sort"
)

type remoteIteratorCreator struct {
	nodeDialer *NodeDialer

	ids []uint64
}

//
func (ric *remoteIteratorCreator) shardIDs() uint64Slice {
	ids := make(uint64Slice, len(ric.ids))
	for i, id := range ric.ids {
		ids[i] = id
	}
	return ids
}

//
func (ric *remoteIteratorCreator) nodeIDs() uint64Slice {
	ids := make(uint64Slice, 0)
	sort.Sort(ids)
	return ids
}

// func (ric *remoteIteratorCreator) nodeIteratorCreators() influxql.IteratorCreators {
// 	shardIDs := ric.shardIDs()
// 	nodeIDS := ric.nodeIDs()
// 	return nil
// }

//
func (ric *remoteIteratorCreator) createIterators(stmt *influxql.SelectStatement, ctx *influxql.ExecutionContext) (influxql.Iterators, *influxql.SelectStatement, error) {
	// m := make
	// ric.createNodeIterator()
	return nil, nil, nil
}

func (ric *remoteIteratorCreator) createNodeIterator(id uint64, shardIDs uint64Slice, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	conn, err := ric.nodeDialer.DialNode(id)
	if err != nil {
		return nil, err
	}

	//
	//
	if err := func() error {
		req := rpc.CreateIteratorRequest{}
		req.ShardIDs = []uint64(shardIDs)
		req.Opt = opt

		if err := tlv.EncodeTLV(conn, tlv.CreateIteratorRequestMessage, &req); err != nil {

		}

		//
		resp := rpc.CreateIteratorResponse{}
		if typ, err := tlv.DecodeTLV(conn, &resp); err != nil {

		} else if typ != tlv.CreateIteratorResponseMessage {

		}

		err := resp.Err
		return err
	}(); err != nil {

	}

	// it := influxql.NewReaderIterator(conn, , stats influxql.IteratorStats)

	// it = influxql.NewCloseInterruptIterator(it, closing)
	return nil, nil
}

func (ric *remoteIteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	// return ric.nodeIteratorCreators().FieldDimensions(sources)
	return nil, nil, nil
}

func (ric *remoteIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	// return ric.nodeIteratorCreators().ExpandSources(sources)
	return nil, nil
}

type remoteNodeIteratorCreator struct {
}

func (ric *remoteNodeIteratorCreator) CreateIterator(sources influxql.Sources) {

}
func (ric *remoteNodeIteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (ric *remoteNodeIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return nil, nil
}

type metaIteratorCreator struct {
}

func (mic *metaIteratorCreator) nodeIteratorCreators() {

}

func (mic *metaIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return nil, nil
}

func (mic *metaIteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (mic *metaIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return nil, nil
}

type NodeDialer struct {
	timeout    time.Duration
	MetaClient interface {
		DataNode(id uint64) (*meta.NodeInfo, error)
	}
}

func (nd *NodeDialer) DialNode(id uint64) (net.Conn, error) {
	node, err := nd.MetaClient.DataNode(id)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", node.TCPHost, nd.timeout)
	if err != nil {
		return nil, err
	}
	//
	return conn, nil
}

type uint64Slice []uint64

func (u uint64Slice) Len() int {
	return len(u)
}

func (u uint64Slice) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u uint64Slice) Less(i, j int) bool {
	return u[i] < u[j]
}
