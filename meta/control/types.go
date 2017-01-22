package control

import (
	"github.com/influxdata/influxdb/services/meta"
)

type NodeByAddr meta.NodeInfos

func (n NodeByAddr) Len() int {
	return len(n)
}

func (n NodeByAddr) Less(i, j int) bool {
	return n[i].ID < n[j].ID
}

func (n NodeByAddr) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

type DataNodeByAddr meta.NodeInfos

func (d DataNodeByAddr) Len() int {
	return len(d)
}

func (d DataNodeByAddr) Less(i, j int) bool {
	return d[i].ID < d[j].ID
}

func (d DataNodeByAddr) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

type MetaNodeByAddr meta.NodeInfos

func (d MetaNodeByAddr) Len() int {
	return len(d)
}

func (d MetaNodeByAddr) Less(i, j int) bool {
	return d[i].ID < d[j].ID
}

func (d MetaNodeByAddr) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
