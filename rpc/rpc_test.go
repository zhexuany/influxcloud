package rpc_test

import (
	"bytes"
	"github.com/influxdata/influxdb/models"
	"github.com/zhexuany/influxcloud/rpc"
	"testing"
	"time"
)

func TestWriteShardRequestBinary(t *testing.T) {
	sr := &rpc.WriteShardRequest{}

	sr.SetShardID(uint64(1))
	if exp := uint64(1); sr.ShardID() != exp {
		t.Fatalf("ShardID mismatch: got %v, exp %v", sr.ShardID(), exp)
	}

	sr.AddPoint("cpu", 1.0, time.Now(), models.NewTags(map[string]string{"host": "serverA"}))
	sr.AddPoint("cpu", 2.0, time.Now().Add(time.Hour), nil)
	sr.AddPoint("cpu_load", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	b, err := sr.MarshalBinary()
	if err != nil {
		t.Fatalf("WritePointsRequest.MarshalBinary() failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("WritePointsRequest.MarshalBinary() returned 0 bytes")
	}

	got := &rpc.WriteShardRequest{}
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatalf("WritePointsRequest.UnmarshalMarshalBinary() failed: %v", err)
	}

	if got.ShardID() != sr.ShardID() {
		t.Errorf("ShardID mismatch: got %v, exp %v", got.ShardID(), sr.ShardID())
	}

	if len(got.Points()) != len(sr.Points()) {
		t.Errorf("Points count mismatch: got %v, exp %v", len(got.Points()), len(sr.Points()))
	}

	srPoints := sr.Points()
	gotPoints := got.Points()
	for i, p := range srPoints {
		g := gotPoints[i]

		if g.Name() != p.Name() {
			t.Errorf("Point %d name mismatch: got %v, exp %v", i, g.Name(), p.Name())
		}

		if !g.Time().Equal(p.Time()) {
			t.Errorf("Point %d time mismatch: got %v, exp %v", i, g.Time(), p.Time())
		}

		if g.HashID() != p.HashID() {
			t.Errorf("Point #%d HashID() mismatch: got %v, exp %v", i, g.HashID(), p.HashID())
		}

		for _, tag := range p.Tags() {
			if exp := bytes.Compare(g.Tags().Get(tag.Key), tag.Value); exp != 0 {
				t.Errorf("Point #%d tag mismatch: got %v, exp %v", i, tag.Key, tag.Value)
			}
		}

		pFields, _ := p.Fields()
		gFields, _ := g.Fields()
		if len(pFields) != len(gFields) {
			t.Errorf("Point %d field count mismatch: got %v, exp %v", i, len(gFields), len(pFields))
		}

		for j, f := range pFields {
			if gFields[j] != f {
				t.Errorf("Point %d field mismatch: got %v, exp %v", i, gFields[j], f)
			}
		}
	}
}

func TestWriteShardResponseBinary(t *testing.T) {
	sr := &rpc.WriteShardResponse{}
	sr.SetCode(10)
	sr.SetMessage("foo")
	b, err := sr.MarshalBinary()

	if exp := 10; sr.Code() != exp {
		t.Fatalf("Code mismatch: got %v, exp %v", sr.Code(), exp)
	}

	if exp := "foo"; sr.Message() != exp {
		t.Fatalf("Message mismatch: got %v, exp %v", sr.Message(), exp)
	}

	if err != nil {
		t.Fatalf("WritePointsResponse.MarshalBinary() failed: %v", err)
	}
	if len(b) == 0 {
		t.Fatalf("WritePointsResponse.MarshalBinary() returned 0 bytes")
	}

	got := &rpc.WriteShardResponse{}
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatalf("WritePointsResponse.UnmarshalMarshalBinary() failed: %v", err)
	}

	if got.Code() != sr.Code() {
		t.Errorf("Code mismatch: got %v, exp %v", got.Code(), sr.Code())
	}

	if got.Message() != sr.Message() {
		t.Errorf("Message mismatch: got %v, exp %v", got.Message(), sr.Message())
	}

}
