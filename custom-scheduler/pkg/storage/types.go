package storage

import (
	"time"
)

// StorageNodeType classifies nodes as edge or cloud
type StorageNodeType string

const (
	StorageTypeEdge  StorageNodeType = "edge"
	StorageTypeCloud StorageNodeType = "cloud"
)

// StorageServiceType identifies the type of storage service
type StorageServiceType string

const (
	StorageServiceMinio   StorageServiceType = "minio"
	StorageServiceGeneric StorageServiceType = "generic"
	StorageServiceLocal   StorageServiceType = "local"
)

// DataItem represents a piece of data with its metadata
type DataItem struct {
	URN          string            // Uniform Resource Name (bucket/path)
	Size         int64             // Size in bytes
	Locations    []string          // Node names that have this data
	LastModified time.Time         // Last modification time
	ContentType  string            // MIME type
	Metadata     map[string]string // Additional metadata
}

// StorageNode represents a node with storage capabilities
type StorageNode struct {
	Name              string
	NodeType          StorageNodeType
	ServiceType       StorageServiceType
	Region            string
	Zone              string
	CapacityBytes     int64
	AvailableBytes    int64
	StorageTechnology string // ssd, hdd, nvme
	LastUpdated       time.Time
	Buckets           []string // Buckets available on this node
	TopologyLabels    map[string]string
}

// NetworkPath represents connection characteristics between two nodes
type NetworkPath struct {
	SourceNode  string
	DestNode    string
	Bandwidth   float64 // Bytes per second
	Latency     float64 // Milliseconds
	MeasuredAt  time.Time
	Reliability float64 // 0.0-1.0 score of reliability
	IsEstimated bool    // Whether this is measured or estimated
}
