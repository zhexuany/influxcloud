package rpc

import (
	"errors"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/influxcloud/rpc/internal"
)

//go:generate protoc --gogo_out=. internal/data.proto

// WriteShardRequest represents the a request to write a slice of points to a shard
type WriteShardRequest struct {
	pb internal.WriteShardRequest
}

// WriteShardResponse represents the response returned from a remote WriteShardRequest call
type WriteShardResponse struct {
	pb internal.WriteShardResponse
}

// SetShardID sets the ShardID
func (w *WriteShardRequest) SetShardID(id uint64) { w.pb.ShardID = &id }

// ShardID gets the ShardID
func (w *WriteShardRequest) ShardID() uint64 { return w.pb.GetShardID() }

func (w *WriteShardRequest) SetDatabase(db string) { w.pb.Database = &db }

func (w *WriteShardRequest) SetRetentionPolicy(rp string) { w.pb.RetentionPolicy = &rp }

func (w *WriteShardRequest) Database() string { return w.pb.GetDatabase() }

func (w *WriteShardRequest) RetentionPolicy() string { return w.pb.GetRetentionPolicy() }

// Points returns the time series Points
func (w *WriteShardRequest) Points() []models.Point { return w.unmarshalPoints() }

// AddPoint adds a new time series point
func (w *WriteShardRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags models.Tags) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.AddPoints([]models.Point{pt})
}

// AddPoints adds a new time series point
func (w *WriteShardRequest) AddPoints(points []models.Point) {
	for _, p := range points {
		b, err := p.MarshalBinary()
		if err != nil {
			// A error here means that we create a point higher in the stack that we could
			// not marshal to a byte slice.  If that happens, the endpoint that created that
			// point needs to be fixed.
			panic(fmt.Sprintf("failed to marshal point: `%v`: %v", p, err))
		}
		w.pb.Points = append(w.pb.Points, b)
	}
}

func (w *WriteShardRequest) SetBinaryPoints(buf []byte) {
	w.pb.Points = append(w.pb.Points, buf)
}

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

func (w *WriteShardRequest) unmarshalPoints() []models.Point {
	points := make([]models.Point, len(w.pb.GetPoints()))
	for i, p := range w.pb.GetPoints() {
		pt, err := models.NewPointFromBytes(p)
		if err != nil {
			// A error here means that one node created a valid point and sent us an
			// unparseable version.  We could log and drop the point and allow
			// anti-entropy to resolve the discrepancy, but this shouldn't ever happen.
			panic(fmt.Sprintf("failed to parse point: `%v`: %v", string(p), err))
		}

		points[i] = pt
	}
	return points
}

// SetCode sets the Code
func (w *WriteShardResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// SetMessage sets the Message
func (w *WriteShardResponse) SetMessage(message string) { w.pb.Message = &message }

// Code returns the Code
func (w *WriteShardResponse) Code() int { return int(w.pb.GetCode()) }

// Message returns the Message
func (w *WriteShardResponse) Message() string { return w.pb.GetMessage() }

// MarshalBinary encodes the object to a binary format.
func (w *WriteShardResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates WritePointRequest from a binary format.
func (w *WriteShardResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementRequest represents the a request to execute a statement on a node.
type ExecuteStatementRequest struct {
	pb internal.ExecuteStatementRequest
}

// Statement returns the InfluxQL statement.
func (r *ExecuteStatementRequest) Statement() string { return r.pb.GetStatement() }

// SetStatement sets the InfluxQL statement.
func (r *ExecuteStatementRequest) SetStatement(statement string) {
	r.pb.Statement = proto.String(statement)
}

// Database returns the database name.
func (r *ExecuteStatementRequest) Database() string { return r.pb.GetDatabase() }

// SetDatabase sets the database name.
func (r *ExecuteStatementRequest) SetDatabase(database string) { r.pb.Database = proto.String(database) }

// MarshalBinary encodes the object to a binary format.
func (r *ExecuteStatementRequest) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&r.pb)
}

// UnmarshalBinary populates ExecuteStatementRequest from a binary format.
func (r *ExecuteStatementRequest) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &r.pb); err != nil {
		return err
	}
	return nil
}

// ExecuteStatementResponse represents the response returned from a remote ExecuteStatementRequest call.
type ExecuteStatementResponse struct {
	pb internal.WriteShardResponse
}

// Code returns the response code.
func (w *ExecuteStatementResponse) Code() int { return int(w.pb.GetCode()) }

// SetCode sets the Code
func (w *ExecuteStatementResponse) SetCode(code int) { w.pb.Code = proto.Int32(int32(code)) }

// Message returns the repsonse message.
func (w *ExecuteStatementResponse) Message() string { return w.pb.GetMessage() }

// SetMessage sets the Message
func (w *ExecuteStatementResponse) SetMessage(message string) { w.pb.Message = &message }

// MarshalBinary encodes the object to a binary format.
func (w *ExecuteStatementResponse) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&w.pb)
}

// UnmarshalBinary populates ExecuteStatementResponse from a binary format.
func (w *ExecuteStatementResponse) UnmarshalBinary(buf []byte) error {
	if err := proto.Unmarshal(buf, &w.pb); err != nil {
		return err
	}
	return nil
}

// CreateIteratorRequest represents a request to create a remote iterator.
type CreateIteratorRequest struct {
	ShardIDs []uint64
	Opt      influxql.IteratorOptions
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Opt.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.CreateIteratorRequest{
		ShardIDs: r.ShardIDs,
		Opt:      buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Opt.UnmarshalBinary(pb.GetOpt()); err != nil {
		return err
	}
	return nil
}

// CreateIteratorResponse represents a response from remote iterator creation.
type CreateIteratorResponse struct {
	Err error
}

// MarshalBinary encodes r to a binary format.
func (r *CreateIteratorResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CreateIteratorResponse
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *CreateIteratorResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CreateIteratorResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// FieldDimensionsRequest represents a request to retrieve unique fields & dimensions.
type FieldDimensionsRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.FieldDimensionsRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsRequest) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// FieldDimensionsResponse represents a response from remote iterator creation.
type FieldDimensionsResponse struct {
	Fields     map[string]struct{}
	Dimensions map[string]struct{}
	Err        error
}

// MarshalBinary encodes r to a binary format.
func (r *FieldDimensionsResponse) MarshalBinary() ([]byte, error) {
	var pb internal.FieldDimensionsResponse

	pb.Fields = make([]string, 0, len(r.Fields))
	for k := range r.Fields {
		pb.Fields = append(pb.Fields, k)
	}

	pb.Dimensions = make([]string, 0, len(r.Dimensions))
	for k := range r.Dimensions {
		pb.Dimensions = append(pb.Dimensions, k)
	}

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *FieldDimensionsResponse) UnmarshalBinary(data []byte) error {
	var pb internal.FieldDimensionsResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.Fields = make(map[string]struct{}, len(pb.GetFields()))
	for _, s := range pb.GetFields() {
		r.Fields[s] = struct{}{}
	}

	r.Dimensions = make(map[string]struct{}, len(pb.GetDimensions()))
	for _, s := range pb.GetDimensions() {
		r.Dimensions[s] = struct{}{}
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

// ExpandSourcesRequest represents a request to expand regex sources.
type ExpandSourcesRequest struct {
	ShardIDs []uint64
	Sources  influxql.Sources
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesRequest) MarshalBinary() ([]byte, error) {
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&internal.ExpandSourcesRequest{
		ShardIDs: r.ShardIDs,
		Sources:  buf,
	})
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesRequest) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	r.ShardIDs = pb.GetShardIDs()
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}
	return nil
}

// ExpandSourcesResponse represents a response from source expansion.
type ExpandSourcesResponse struct {
	Sources influxql.Sources
	Err     error
}

// MarshalBinary encodes r to a binary format.
func (r *ExpandSourcesResponse) MarshalBinary() ([]byte, error) {
	var pb internal.ExpandSourcesResponse
	buf, err := r.Sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	pb.Sources = buf

	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes data into r.
func (r *ExpandSourcesResponse) UnmarshalBinary(data []byte) error {
	var pb internal.ExpandSourcesResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	if err := r.Sources.UnmarshalBinary(pb.GetSources()); err != nil {
		return err
	}

	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

type DownloadShardSnapshotRequest struct {
	Path    string
	ShardID uint64
}

func (dsr *DownloadShardSnapshotRequest) MarshalBinary() ([]byte, error) {
	var pb internal.DownloadShardSnapshotRequest
	pb.Path = proto.String(dsr.Path)
	pb.ShardID = proto.Uint64(dsr.ShardID)

	return proto.Marshal(&pb)
}

func (dsr *DownloadShardSnapshotRequest) UnmarshalBinary(data []byte) error {
	var pb internal.DownloadShardSnapshotRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	dsr.ShardID = pb.GetShardID()
	dsr.Path = pb.GetPath()

	return nil
}

type CreateShardSnapshotRequest struct {
	ShardID uint64
}

func (csr *CreateShardSnapshotRequest) MarshalBinary() ([]byte, error) {
	var pb internal.CreateShardSnapshotRequest
	pb.ShardID = proto.Uint64(csr.ShardID)

	return proto.Marshal(&pb)
}

func (csr *CreateShardSnapshotRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CreateShardSnapshotRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	csr.ShardID = pb.GetShardID()
	return nil
}

type CreateShardSnapshotResponse struct {
	Err  string
	Path string
	Size uint64
}

func (csr *CreateShardSnapshotResponse) MarsshalBinary() ([]byte, error) {
	var pb internal.CreateShardSnapshotResponse
	pb.Err = proto.String(csr.Err)
	pb.Path = proto.String(csr.Path)
	pb.Size_ = proto.Uint64(csr.Size)

	return proto.Marshal(&pb)
}

func (csr *CreateShardSnapshotResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CreateShardSnapshotResponse

	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	csr.Err = pb.GetErr()
	csr.Path = pb.GetPath()
	csr.Size = pb.GetSize_()

	return nil
}

type RestoreShardRequest struct {
	Size    uint64
	ShardID uint64
}

func (m *RestoreShardRequest) MarshalBinary() ([]byte, error) {
	var pb internal.RestoreShardRequest

	pb.Size_ = proto.Uint64(m.Size)
	pb.ShardID = proto.Uint64(m.ShardID)

	return proto.Marshal(&pb)
}

func (m *RestoreShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.RestoreShardRequest

	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	m.Size = pb.GetSize_()
	m.ShardID = pb.GetShardID()

	return nil
}

type RestoreShardResponse struct {
	Err string
}

func (m *RestoreShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.RestoreShardResponse
	pb.Err = proto.String(m.Err)

	return proto.Marshal(&pb)
}

func (m *RestoreShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.RestoreShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	m.Err = pb.GetErr()

	return nil
}

type JoinClusterRequest struct {
	NodeID    uint64
	NodeAddr  string
	MetaAddrs []string
}

func (jc *JoinClusterRequest) MarshalBinary() ([]byte, error) {
	var pb internal.JoinClusterRequest

	pb.NodeID = proto.Uint64(jc.NodeID)

	for i, metaAddr := range jc.MetaAddrs {
		pb.MetaAddrs[i] = metaAddr
	}
	pb.NodeAddr = proto.String(jc.NodeAddr)

	return proto.Marshal(&pb)

}

func (jc *JoinClusterRequest) UnmarshalBinary(data []byte) error {
	var pb internal.JoinClusterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	jc.MetaAddrs = pb.GetMetaAddrs()
	jc.NodeID = pb.GetNodeID()
	jc.NodeAddr = pb.GetNodeAddr()

	return nil

}

type JoinClusterResponse struct {
	NodeID  uint64
	TCPHost string
}

func (jcr *JoinClusterResponse) MarshalBinery() ([]byte, error) {
	var pb internal.JoinClusterResponse
	pb.NodeID = proto.Uint64(jcr.NodeID)
	pb.TCPHost = proto.String(jcr.TCPHost)

	return proto.Marshal(&pb)
}

func (jcr *JoinClusterResponse) UnmarshalBinary(data []byte) error {
	var pb internal.JoinClusterResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	jcr.NodeID = pb.GetNodeID()
	jcr.TCPHost = pb.GetTCPHost()

	return nil
}

type LeaveClusterRequest struct {
	NodeAddr string
}

func (lcr *LeaveClusterRequest) MarshalBinary() ([]byte, error) {
	var pb internal.LeaveClusterRequest
	pb.NodeAddr = proto.String(lcr.NodeAddr)

	return proto.Marshal(&pb)
}

func (lcr *LeaveClusterRequest) UnmarshalBinary(data []byte) error {
	var pb internal.LeaveClusterRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	lcr.NodeAddr = pb.GetNodeAddr()

	return nil
}

type LeaveClusterReesponse struct {
}

func (lcr *LeaveClusterReesponse) MarshalBinary() ([]byte, error) {
	var pb internal.LeaveClusterResponse

	return proto.Marshal(&pb)
}

func (lcr *LeaveClusterReesponse) UnmarshalBinary(data []byte) error {
	var pb internal.LeaveClusterResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	return nil
}

type RemoveShardRequest struct {
	Database string
	ShardID  uint64
	Policy   string
}

func (rsr *RemoveShardRequest) MarshalBinary() ([]byte, error) {
	var pb internal.RemoveShardRequest
	pb.Database = proto.String(rsr.Database)
	pb.ShardID = proto.Uint64(rsr.ShardID)
	pb.Policy = proto.String(rsr.Policy)

	return proto.Marshal(&pb)
}

func (rsr *RemoveShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	rsr.Database = pb.GetDatabase()
	rsr.ShardID = pb.GetShardID()
	rsr.Policy = pb.GetPolicy()

	return nil
}

type RemoveShardResponse struct {
}

func (rsr *RemoveShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.RemoveShardResponse

	return proto.Marshal(&pb)
}

func (rsr *RemoveShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.RemoveShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	return nil
}

type CopyShardStatusRequest struct {
}

func (csr *CopyShardStatusRequest) MarshalBinary() ([]byte, error) {
	var pb internal.CopyShardStatusRequest

	return proto.Marshal(&pb)
}

func (csr *CopyShardStatusRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardStatusRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	return nil
}

type CopyShardStatusResponse struct {
	Err   string
	Tasks []string
}

func (csr *CopyShardStatusResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CopyShardStatusResponse
	pb.Err = proto.String(csr.Err)
	for i, task := range csr.Tasks {
		pb.Tasks[i] = task
	}

	return proto.Marshal(&pb)
}

func (csr *CopyShardStatusResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardStatusResponse

	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	for i, task := range pb.GetTasks() {
		csr.Tasks[i] = task
	}
	csr.Err = pb.GetErr()

	return nil
}

type CopyShardRequest struct {
	Source  string
	Dest    string
	ShardID uint64
	Policy  string
}

func (m *CopyShardRequest) MarshalBinary() ([]byte, error) {
	var pb internal.CopyShardRequest

	if m.Source == "" {
		return nil, fmt.Errorf("Source cannot be empty string")
	}
	if m.Dest == "" {
		return nil, fmt.Errorf("Dest cannot be empty string")
	}
	if m.ShardID == 0 {
		return nil, fmt.Errorf("ShardID must be larger than 0")
	}
	if m.Policy == "" {
		return nil, fmt.Errorf("RetentionPolicy' name cannot be empty")
	}
	pb.Source = proto.String(m.Source)
	pb.Dest = proto.String(m.Dest)
	pb.ShardID = proto.Uint64(m.ShardID)
	pb.Policy = proto.String(m.Policy)

	return proto.Marshal(&pb)
}

func (m *CopyShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	m.Source = pb.GetSource()
	m.Dest = pb.GetDest()
	m.ShardID = pb.GetShardID()
	m.Policy = pb.GetPolicy()

	return nil
}

type CopyShardResponse struct {
	Err string
}

func (m *CopyShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.CopyShardResponse

	pb.Err = proto.String(m.Err)

	return proto.Marshal(&pb)
}
func (m *CopyShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.CopyShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	m.Err = pb.GetErr()

	return nil
}

type KillCopyShardRequest struct {
	Source  string
	Dest    string
	ShardID uint64
}

func (kcr *KillCopyShardRequest) MarshalBinary() ([]byte, error) {
	var pb internal.KillCopyShardRequest
	pb.Source = proto.String(kcr.Source)
	pb.Dest = proto.String(kcr.Dest)
	pb.ShardID = proto.Uint64(kcr.ShardID)

	return proto.Marshal(&pb)
}

func (kcr *KillCopyShardRequest) UnmarshalBinary(data []byte) error {
	var pb internal.KillCopyShardRequest
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	kcr.Dest = pb.GetDest()
	kcr.Source = pb.GetSource()
	kcr.ShardID = pb.GetShardID()

	return nil
}

type KillCopyShardResponse struct {
	Err string
}

func (ksr *KillCopyShardResponse) MarshalBinary() ([]byte, error) {
	var pb internal.KillCopyShardResponse
	pb.Err = proto.String(ksr.Err)

	return proto.Marshal(&pb)
}

func (ksr *KillCopyShardResponse) UnmarshalBinary(data []byte) error {
	var pb internal.KillCopyShardResponse
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	ksr.Err = pb.GetErr()

	return nil
}
