package storage

import (
	"time"
)

// DataItem represents a piece of data with its location and size
type DataItem struct {
	URN          string
	Size         int64
	Locations    []string
	LastModified time.Time
	Metadata     map[string]string
}

// BandwidthInfo contains bandwidth information between nodes
type BandwidthInfo struct {
	SourceNode           string
	DestNode             string
	BandwidthBytesPerSec float64
	LatencyMs            float64
}

// StorageNode represents a node that provides storage capabilities
type StorageNode struct {
	Name           string
	CapacityBytes  int64
	AvailableBytes int64
	StorageType    string
}
