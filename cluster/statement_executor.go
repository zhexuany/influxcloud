package cluster

import (
	"fmt"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxdb-cluster/rpc"
	"github.com/zhexuany/influxdb-cluster/tlv"
	"net"
	"time"
)

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	pool           *clientPool
	timeout        time.Duration
	maxConnections int

	MetaClient interface {
		DataNodes() (ni meta.NodeInfos, err error)
	}

	StatementExecutor coordinator.StatementExecutor
}

//
//
//
//
//
// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *influxql.ExecutionContext) error {
	switch t := stmt.(type) {
	case *influxql.ShowQueriesStatement:
		return e.executeShowQueriesStatement(stmt)
	case *influxql.KillQueryStatement:
		return e.executeKillQueryStatement(stmt)
	}
	return e.StatementExecutor.ExecuteStatement(stmt, ctx)
}

//
//
//
//
//
//
//
//
func (e *StatementExecutor) executeShowQueriesStatement(stmt *influxql.ShowQueriesStatement) error {
	dataNodes, err := e.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	for _, data := range dataNodes {
		go func(data meta.NodeInfo) error {
			conn, err := net.Dial("tcp", data.TCPHost)
			if err != nil {
				return fmt.Errorf("failed to connect:%v", err)
			}
			req := rpc.ExecuteStatementRequest{}
			req.SetDatabase("")
			req.SetStatement(stmt.String())
			err = tlv.EncodeTLV(conn, tlv.ExecuteStatementRequestMessage, &req)
			if err != nil {
				return fmt.Errorf("failed to encode tlv: %v", err)
			}

			res := rpc.ExecuteStatementResponse{}
			if _, err := tlv.DecodeTLV(conn, tlv.ExecuteStatementResponseMessage, &res); err != nil {
				return fmt.Errorf("failed to decode tlv: %v", err)
			} else if res.Code() != 200 {
				return fmt.Errorf("response status is not correct")
			}

		}(data)
	}
	return nil
}

func (e *StatementExecutor) executeKillQueryStatement(q *influxql.KillQueryStatement) error {
	return nil
}
