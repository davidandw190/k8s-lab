package storage

import (
	"time"
)

type StorageNodeType string

const (
	StorageTypeEdge  StorageNodeType = "edge"
	StorageTypeCloud StorageNodeType = "cloud"
	StorageTypeLocal StorageNodeType = "local"
)

type StorageServiceType string

const (
	StorageServiceMinIO StorageServiceType = "minio"
)

// DataItem represents a piece of data with its  metadata
type DataItem struct {
	URN          string
	Size         int64
	Locations    []string
	LastModified time.Time
	ContentType  string
	Metadata     map[string]string
}

// BandwidthInfo contains bandwidth information between nodes
type BandwidthInfo struct {
	SourceNode           string
	DestNode             string
	BandwidthBytesPerSec float64
	LatencyMs            float64
	MeasuredAt           time.Time
	Reliability          float64
}

// StorageNode represents a node that provides storage capabilities
type StorageNode struct {
	Name              string
	NodeType          StorageNodeType
	ServiceType       StorageServiceType
	CapacityBytes     int64
	AvailableBytes    int64
	StorageTechnology string
	LastUpdated       time.Time
	Buckets           []string
	Region            string
	Zone              string
	TopologyLabels    map[string]string
}

// StorageTopology represents the storage layout across the cluster
type StorageTopology struct {
	NodeLocations map[string]struct {
		Region string
		Zone   string
	}

	BucketPlacements map[string][]string
}
