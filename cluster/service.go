package cluster

import (
	"expvar"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
	"github.com/zhexuany/influxcloud/rpc"
	"github.com/zhexuany/influxcloud/tlv"
)

// MaxMessageSize defines how large a message can be before we reject it
const MaxMessageSize = 1024 * 1024 * 1024 // 1GB

// MuxHeader is the header byte used in the TCP mux.
const MuxHeader = 2

// Service reprsents a cluster service
type Service struct {
	mu sync.RWMutex

	wg      sync.WaitGroup
	closing chan struct{}

	Listener net.Listener

	MetaClient interface {
		ShardOwner(shardID uint64) (string, string, meta.ShardInfo)
	}

	TSDBStore coordinator.TSDBStore

	ShardIteratorCreator coordinator.ShardIteratorCreator

	Logger      zap.Logger
	ShardWriter ShardWriter

	statMap *expvar.Map
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	return &Service{
		closing: make(chan struct{}),
		Logger:  zap.New(zap.NullEncoder()),
	}
}

// Open opens the network listener and begins serving requests
func (s *Service) Open() error {
	s.Logger.Info("Starting cluster service")
	s.wg.Add(1)
	go s.serve()

	s.Logger.Info(fmt.Sprint("Listening on HTTPS:", s.Listener.Addr().String()))
	return nil
}

// WithLogger sets the internal logger to the logger passed in
func (s *Service) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "cluster"))
}

// serve accepts connections from the listener and handles them

func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Check if the service is shutting down
		select {
		case <-s.closing:
			return
		default:
		}

		// Accept the next connection
		conn, err := s.Listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "connection closed") {
				s.Logger.Info(fmt.Sprint("cluster service accept error:", err))
				return
			}
			s.Logger.Info(fmt.Sprint("accept error:", err))
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Close close this service
func (s *Service) Close() error {
	if s.Listener != nil {
		s.Listener.Close()
	}

	close(s.closing)
	s.wg.Wait()

	return nil
}

func (s *Service) handleConn(conn net.Conn) {
	//Ensuring connection is closed when service is closed
	closing := make(chan struct{})
	defer close(closing)
	go func() {
		select {
		case <-closing:
		case <-s.closing:
		}
		conn.Close()
	}()

	s.Logger.Info(fmt.Sprint("accept remote connection from", conn.RemoteAddr()))
	defer func() {
		s.Logger.Info(fmt.Sprint("close remote connection from", conn.RemoteAddr()))
	}()
	for {
		// Read type-length-value.
		typ, err := tlv.ReadType(conn)
		if err != nil {
			if strings.HasSuffix(err.Error(), "EOF") {
				return
			}
			s.Logger.Warn("unable to read type:" + err.Error())
			return
		}

		// Delegate message processing by type.
		switch typ {
		case tlv.WriteShardRequestMessage:
			buf, err := tlv.ReadLV(conn)
			if err != nil {
				s.Logger.Warn("unable to read length-value: " + err.Error())
				return
			}

			err = s.processWriteShardRequest(buf)
			if err != nil {
				s.Logger.Warn("process write shard error: " + err.Error())
			}
			s.writeShardResponse(conn, err)
		case tlv.ExecuteStatementRequestMessage:
			buf, err := tlv.ReadLV(conn)
			if err != nil {
				s.Logger.Warn("unable to read length-value: " + err.Error())
				return
			}

			err = s.processExecuteStatementRequest(buf)
			if err != nil {
				s.Logger.Warn("process execute statement error:" + err.Error())
			}
			s.writeShardResponse(conn, err)
		case tlv.CreateIteratorRequestMessage:
			s.processCreateIteratorRequest(conn)
			return
		case tlv.FieldDimensionsRequestMessage:
			s.processFieldDimensionsRequest(conn)
			return
		// case seriesKeysRequestMessage:
		// s.processSeriesKeysRequest(conn)
		// return
		default:
			s.Logger.Warn("cluster service message type not found:" + string(typ))
		}
	}

}

func (s *Service) executeStatement(stmt influxql.Statement, database string) error {
	switch t := stmt.(type) {
	case *influxql.DropDatabaseStatement:
		return s.TSDBStore.DeleteDatabase(t.Name)
	case *influxql.DropMeasurementStatement:
		return s.TSDBStore.DeleteMeasurement(database, t.Name)
	case *influxql.DropSeriesStatement:
		return s.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
	case *influxql.DropRetentionPolicyStatement:
		return s.TSDBStore.DeleteRetentionPolicy(database, t.Name)
	default:
		return fmt.Errorf("%q should not be executed across a cluster", stmt.String())
	}
}

func (s *Service) processWriteShardRequest(buf []byte) error {
	// Build request
	var req rpc.WriteShardRequest
	if err := req.UnmarshalBinary(buf); err != nil {
		return err
	}

	points := req.Points()
	// write points locally
	err := s.TSDBStore.WriteToShard(req.ShardID(), points)

	// _, _, si := s.MetaClient.ShardOwner(req.ShardID())
	// for _, node := range si.Owners {
	// 	s.ShardWriter.WriteShard(req.ShardID(), node.NodeID, points)
	// }
	// We may have received a write for a shard that we don't have locally because the
	// sending node may have just created the shard (via the metastore) and the write
	// arrived before the local store could create the shard.  In this case, we need
	// to check the metastore to determine what database and retention policy this
	// shard should reside within.
	if err == tsdb.ErrShardNotFound {
		db, rp := req.Database(), req.RetentionPolicy()
		if db == "" || rp == "" {
			s.Logger.Warn("drop write request: shard" + string(req.ShardID()) + ". no database or rentention policy received")
			return nil
		}

		err = s.TSDBStore.CreateShard(req.Database(), req.RetentionPolicy(), req.ShardID(), true)
		if err != nil {
			return fmt.Errorf("create shard %d: %s", req.ShardID(), err)
		}

		err = s.TSDBStore.WriteToShard(req.ShardID(), points)
		if err != nil {
			return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
		}
	}

	if err != nil {
		return fmt.Errorf("write shard %d: %s", req.ShardID(), err)
	}

	return nil
}

func (s *Service) writeShardResponse(conn net.Conn, err error) {
	// Build response.
	var resp rpc.WriteShardResponse
	if err != nil {
		resp.SetCode(1)
		resp.SetMessage(err.Error())
	} else {
		resp.SetCode(0)
	}

	// Marshal response to binary.
	buf, err := resp.MarshalBinary()
	if err != nil {
		s.Logger.Warn("error marshalling shard response: " + err.Error())
		return
	}

	// Write to connection.
	if err := tlv.WriteTLV(conn, tlv.WriteShardResponseMessage, buf); err != nil {
		s.Logger.Warn("write shard response error:" + err.Error())
	}
}

func (s *Service) processCreateIteratorRequest(conn net.Conn) {
	defer conn.Close()

	var itr influxql.Iterator
	if err := func() error {
		// Parse request.
		var req rpc.CreateIteratorRequest
		if err := tlv.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect iterator creators for each shard.
		// ics := make([]influxql.IteratorCreator, 0, len(req.ShardIDs))
		// for _, shardID := range req.ShardIDs {
		// 	ic := s.ShardIteratorCreator.ShardIteratorCreator(shardID)
		// 	if ic == nil {
		// 		return nil
		// 	}
		// 	ics = append(ics, ic)
		// }

		// // Generate a single iterator from all shards.
		// i, err := influxql.IteratorCreators(ics).CreateIterator(req.Opt)
		// if err != nil {
		// 	return err
		// }
		// itr = i

		return nil
	}(); err != nil {
		itr.Close()
		s.Logger.Warn("error reading CreateIterator request:" + err.Error())
		// tlv.EncodeTLV(conn, tlv.CreateIteratorResponseMessage, &CreateIteratorResponse{Err: err})

		tlv.EncodeTLV(conn, tlv.CreateIteratorResponseMessage, nil)
		return
	}

	// Encode success response.
	if err := tlv.EncodeTLV(conn, tlv.CreateIteratorResponseMessage, nil); err != nil {
		s.Logger.Warn("error writing CreateIterator response: " + err.Error())
		return
	}

	// Exit if no iterator was produced.
	if itr == nil {
		return
	}

	// Stream iterator to connection.
	if err := influxql.NewIteratorEncoder(conn).EncodeIterator(itr); err != nil {
		s.Logger.Warn("error encoding CreateIterator iterator: " + err.Error())
		return
	}
}

func (s *Service) processFieldDimensionsRequest(conn net.Conn) {
	var fields, dimensions map[string]struct{}
	if err := func() error {
		// Parse request.
		var req rpc.FieldDimensionsRequest
		if err := tlv.DecodeLV(conn, &req); err != nil {
			return err
		}

		// Collect iterator creators for each shard.
		// ics := make(influxql.Iterators, 0, len(req.ShardIDs))
		// for _, shardID := range req.ShardIDs {
		// 	ic := s.ShardIteratorCreator.ShardIteratorCreator(shardID)
		// 	if ic == nil {
		// 		return nil
		// 	}
		// 	// ics = append(ics, ic.CreateIterator(nil))
		// }

		// // Generate a single iterator from all shards.
		// i, _ := ics.Merge(nil)
		// f, d, err := influxql.FieldMapper.FieldDimensions(nil)
		// // f, d, err := influxql.IteratorCreators(ics).FieldDimensions(req.Sources)
		// if err != nil {
		// 	return err
		// }
		// fields, dimensions = f, d

		return nil
	}(); err != nil {
		s.Logger.Warn("error reading FieldDimensions request: " + err.Error())
		tlv.EncodeTLV(conn, tlv.FieldDimensionsResponseMessage, nil)
		return
	}

	// Encode success response.
	if err := tlv.EncodeTLV(conn, tlv.FieldDimensionsResponseMessage, &rpc.FieldDimensionsResponse{
		Fields:     fields,
		Dimensions: dimensions,
	}); err != nil {
		s.Logger.Warn("error writing FieldDimensions response: " + err.Error())
		return
	}
}

func (s *Service) processJoinClusterRequest() {

}
func (s *Service) writeJoinClusterResponse() {

}
func (s *Service) importMetaData() {

}
func (s *Service) processLeaveClusterRequest() {

}
func (s *Service) writeLeaveClusterResponse() {

}
func (s *Service) processCreateShardSnapshotRequest() {

}
func (s *Service) processDeleteShardSnapshotRequest() {

}

func (s *Service) processExpandSourcesRequest() {

}
func (s *Service) processDownloadShardSnapshotRequest() {

}

func (s *Service) shardSnapshot() {

}
func (s *Service) deleteSnapshot() {

}
func (s *Service) downloadShardSnapshot() {

}
func (s *Service) processShardStatusRequest() {

}
func (s *Service) processShowQueriesRequest() {

}
func (s *Service) processKillQueryRequest() {

}
func (s *Service) processRestoreShard() {

}
func (s *Service) processShowMeasurements() {

}
func (s *Service) processShowTagValues() {

}

func (s *Service) processExecuteStatementRequest(buf []byte) error {
	return nil
}

// BufferedWriteCloser will
type BufferedWriteCloser struct {
}

// Close is actually closing this bufferedwriter
func (bfc *BufferedWriteCloser) Close() {

}
