package storage

import (
	"time"
)

type StorageNodeType string

const (
	StorageTypeEdge  StorageNodeType = "edge"
	StorageTypeCloud StorageNodeType = "cloud"
)

type StorageServiceType string

const (
	StorageServiceMinio   StorageServiceType = "minio"
	StorageServiceGeneric StorageServiceType = "generic"
	StorageServiceLocal   StorageServiceType = "local"
)

// DataItem represents a piece of data with its metadata
type DataItem struct {
	URN          string
	Size         int64
	Locations    []string // node names that have this data
	LastModified time.Time
	ContentType  string
	Metadata     map[string]string
}

// StorageNode represents a node with storage capabilities
type StorageNode struct {
	Name              string
	NodeType          StorageNodeType // edge/cloud
	ServiceType       StorageServiceType
	Region            string
	Zone              string
	CapacityBytes     int64
	AvailableBytes    int64
	StorageTechnology string
	LastUpdated       time.Time
	Buckets           []string
	TopologyLabels    map[string]string
}

// BandwidthInfo represents network performance between nodes
type BandwidthInfo struct {
	SourceNode           string
	DestNode             string
	BandwidthBytesPerSec float64
	LatencyMs            float64
	MeasuredAt           time.Time
	Reliability          float64
}
