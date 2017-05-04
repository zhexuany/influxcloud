package cluster

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/zhexuany/influxcloud/rpc"
	"github.com/zhexuany/influxcloud/tlv"
)

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	pool           *clientPool
	timeout        time.Duration
	maxConnections int

	MetaClient interface {
		DataNodes() (ni meta.NodeInfos, err error)
	}

	// This reprsents local StatementExecutor
	StatementExecutor coordinator.StatementExecutor
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx influxql.ExecutionContext) error {
	switch t := stmt.(type) {
	case *influxql.ShowQueriesStatement:
		return e.executeShowQueriesStatement(t, ctx)
	case *influxql.KillQueryStatement:
		return e.executeKillQueryStatement(t, ctx)
	}
	return e.StatementExecutor.ExecuteStatement(stmt, ctx)
}

func (e *StatementExecutor) executeShowQueriesStatement(stmt *influxql.ShowQueriesStatement, ctx influxql.ExecutionContext) error {
	dataNodes, err := e.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	for _, data := range dataNodes {
		go func(data meta.NodeInfo) error {
			conn, err := e.pool.conn(data.ID)
			if err != nil {
				return fmt.Errorf("failed to connect:%v", err)
			}
			req := rpc.ExecuteStatementRequest{}
			req.SetDatabase(ctx.Database)
			req.SetStatement(stmt.String())
			err = tlv.EncodeTLV(conn, tlv.ExecuteStatementRequestMessage, &req)
			if err != nil {
				return fmt.Errorf("failed to encode tlv: %v", err)
			}

			res := rpc.ExecuteStatementResponse{}
			if _, err := tlv.DecodeTLV(conn, &res); err != nil {
				return fmt.Errorf("failed to decode tlv: %v", err)
			} else if res.Code() != 200 {
				return fmt.Errorf("response status is not correct")
			}
			return nil
		}(data)
	}
	return nil
}

func (e *StatementExecutor) executeKillQueryStatement(stmt *influxql.KillQueryStatement, ctx influxql.ExecutionContext) error {
	dataNodes, err := e.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	for _, data := range dataNodes {
		go func(data meta.NodeInfo) error {
			conn, err := e.pool.conn(data.ID)
			if err != nil {
				return fmt.Errorf("failed to connect:%v", err)
			}
			req := rpc.ExecuteStatementRequest{}
			req.SetDatabase(ctx.Database)
			req.SetStatement(stmt.String())
			err = tlv.EncodeTLV(conn, tlv.ExecuteStatementRequestMessage, &req)
			if err != nil {
				return fmt.Errorf("failed to encode tlv: %v", err)
			}

			res := rpc.ExecuteStatementResponse{}
			if _, err := tlv.DecodeTLV(conn, &res); err != nil {
				return fmt.Errorf("failed to decode tlv: %v", err)
			} else if res.Code() != 200 {
				return fmt.Errorf("response status is not correct")
			}
			return nil
		}(data)
	}
	return nil
}

// IntoWriteRequest is a partial copy of cluster.WriteRequest
type IntoWriteRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}
