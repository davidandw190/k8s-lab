package storage

import (
	"fmt"
	"sync"
)

// BandwidthGraph tracks bandwidth between nodes in the cluster
type BandwidthGraph struct {
	bandwidthMap                map[string]map[string]*BandwidthInfo
	defaultBandwidthBytesPerSec float64
	mu                          sync.RWMutex
}

// NewBandwidthGraph creates a new bandwidth graph
func NewBandwidthGraph(defaultBandwidthBytesPerSec float64) *BandwidthGraph {
	return &BandwidthGraph{
		bandwidthMap:                make(map[string]map[string]*BandwidthInfo),
		defaultBandwidthBytesPerSec: defaultBandwidthBytesPerSec,
	}
}

// SetBandwidth sets the bandwidth between two nodes
func (bg *BandwidthGraph) SetBandwidth(source, dest string, bandwidthBytesPerSec float64, latencyMs float64) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	if _, exists := bg.bandwidthMap[source]; !exists {
		bg.bandwidthMap[source] = make(map[string]*BandwidthInfo)
	}

	bg.bandwidthMap[source][dest] = &BandwidthInfo{
		SourceNode:           source,
		DestNode:             dest,
		BandwidthBytesPerSec: bandwidthBytesPerSec,
		LatencyMs:            latencyMs,
	}
}

// GetBandwidth retrieves bandwidth information between nodes
func (bg *BandwidthGraph) GetBandwidth(source, dest string) *BandwidthInfo {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	if sourceMap, exists := bg.bandwidthMap[source]; exists {
		if info, exists := sourceMap[dest]; exists {
			return info
		}
	}

	// If local access
	if source == dest {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: 1e9, // 1 GB/s
			LatencyMs:            0.1, // 0.1ms
		}
	}

	return &BandwidthInfo{
		SourceNode:           source,
		DestNode:             dest,
		BandwidthBytesPerSec: bg.defaultBandwidthBytesPerSec,
		LatencyMs:            10.0, // 10ms
	}
}

// Estimate transfer time in seconds
func (bg *BandwidthGraph) EstimateTransferTime(source, dest string, sizeBytes int64) float64 {
	info := bg.GetBandwidth(source, dest)
	if info.BandwidthBytesPerSec <= 0 {
		return float64(sizeBytes) / bg.defaultBandwidthBytesPerSec
	}
	return float64(sizeBytes) / info.BandwidthBytesPerSec
}

func (bg *BandwidthGraph) Print() string {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	var result string
	for source, destMap := range bg.bandwidthMap {
		for dest, info := range destMap {
			result += fmt.Sprintf("%s -> %s: %.2f MB/s, %.2f ms\n",
				source, dest,
				info.BandwidthBytesPerSec/1e6, // convert to MB/s
				info.LatencyMs)
		}
	}
	return result
}
