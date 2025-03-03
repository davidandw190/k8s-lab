package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// NetworkTopology represents the physical arrangement of nodes
type NetworkTopology struct {
	NodeRegions map[string]string
	NodeZones   map[string]string
	NodeTypes   map[string]StorageNodeType
}

// BandwidthGraph tracks network performance between nodes
type BandwidthGraph struct {
	bandwidthMap                    map[string]map[string]*BandwidthInfo
	topology                        NetworkTopology
	defaultBandwidthBytesPerSec     float64
	defaultLatencyMs                float64
	localBandwidthBytesPerSec       float64
	localLatencyMs                  float64
	sameZoneBandwidthBytesPerSec    float64
	sameZoneLatencyMs               float64
	sameRegionBandwidthBytesPerSec  float64
	sameRegionLatencyMs             float64
	edgeToCloudBandwidthBytesPerSec float64
	edgeToCloudLatencyMs            float64
	mu                              sync.RWMutex
	lastUpdated                     time.Time
}

// NewBandwidthGraph creates a new bandwidth graph
func NewBandwidthGraph(defaultBandwidthBytesPerSec float64) *BandwidthGraph {
	return &BandwidthGraph{
		bandwidthMap: make(map[string]map[string]*BandwidthInfo),
		topology: NetworkTopology{
			NodeRegions: make(map[string]string),
			NodeZones:   make(map[string]string),
			NodeTypes:   make(map[string]StorageNodeType),
		},
		defaultBandwidthBytesPerSec:     defaultBandwidthBytesPerSec,
		defaultLatencyMs:                10.0,  // 10ms default
		localBandwidthBytesPerSec:       1e9,   // 1 GB/s local access
		localLatencyMs:                  0.1,   // 0.1ms local access
		sameZoneBandwidthBytesPerSec:    500e6, // 500 MB/s same zone
		sameZoneLatencyMs:               1.0,   // 1ms same zone
		sameRegionBandwidthBytesPerSec:  200e6, // 200 MB/s same region
		sameRegionLatencyMs:             5.0,   // 5ms for same region
		edgeToCloudBandwidthBytesPerSec: 50e6,  // 50 MB/s edge-cloud
		edgeToCloudLatencyMs:            20.0,  // 20ms edge-cloud
		lastUpdated:                     time.Now(),
	}
}

// SetNodeTopology adds topology information about a node
func (bg *BandwidthGraph) SetNodeTopology(nodeName, region, zone string, nodeType StorageNodeType) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	bg.topology.NodeRegions[nodeName] = region
	bg.topology.NodeZones[nodeName] = zone
	bg.topology.NodeTypes[nodeName] = nodeType
}

// SetBandwidth sets the bandwidth between two nodes
func (bg *BandwidthGraph) SetBandwidth(source, dest string, bandwidthBytesPerSec float64, latencyMs float64) {
	if source == "" || dest == "" {
		klog.Warningf("Attempted to set bandwidth with empty node name: [%s] -> [%s]", source, dest)
		return
	}

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
		MeasuredAt:           time.Now(),
		Reliability:          1.0,
	}

	bg.lastUpdated = time.Now()
	klog.V(4).Infof("Set bandwidth from %s to %s: %.2f MB/s, %.2f ms",
		source, dest, bandwidthBytesPerSec/1e6, latencyMs)
}

// GetBandwidth retrieves bandwidth information between nodes
func (bg *BandwidthGraph) GetBandwidth(source, dest string) *BandwidthInfo {
	if source == "" || dest == "" {
		klog.Warningf("Attempted to get bandwidth with empty node name: [%s] -> [%s]", source, dest)
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.defaultBandwidthBytesPerSec,
			LatencyMs:            bg.defaultLatencyMs,
			MeasuredAt:           time.Time{},
			Reliability:          0.5,
		}
	}

	bg.mu.RLock()
	defer bg.mu.RUnlock()

	// we check if nodes are the same (local access)
	if source == dest {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.localBandwidthBytesPerSec,
			LatencyMs:            bg.localLatencyMs,
			MeasuredAt:           time.Now(),
			Reliability:          1.0,
		}
	}

	if sourceMap, exists := bg.bandwidthMap[source]; exists {
		if info, exists := sourceMap[dest]; exists {
			return info
		}
	}

	if sourceMap, exists := bg.bandwidthMap[dest]; exists {
		if info, exists := sourceMap[source]; exists {
			return &BandwidthInfo{
				SourceNode:           source,
				DestNode:             dest,
				BandwidthBytesPerSec: info.BandwidthBytesPerSec,
				LatencyMs:            info.LatencyMs,
				MeasuredAt:           info.MeasuredAt,
				Reliability:          info.Reliability * 0.9, // Slightly less reliable in reverse
			}
		}
	}

	return bg.estimateFromTopology(source, dest)
}

// estimateFromTopology estimates bandwidth based on node topology
func (bg *BandwidthGraph) estimateFromTopology(source, dest string) *BandwidthInfo {
	if zone := bg.topology.NodeZones[source]; zone != "" && zone == bg.topology.NodeZones[dest] {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.sameZoneBandwidthBytesPerSec,
			LatencyMs:            bg.sameZoneLatencyMs,
			MeasuredAt:           time.Now(),
			Reliability:          0.8,
		}
	}

	if region := bg.topology.NodeRegions[source]; region != "" && region == bg.topology.NodeRegions[dest] {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.sameRegionBandwidthBytesPerSec,
			LatencyMs:            bg.sameRegionLatencyMs,
			MeasuredAt:           time.Now(),
			Reliability:          0.7,
		}
	}

	sourceType := bg.topology.NodeTypes[source]
	destType := bg.topology.NodeTypes[dest]

	if (sourceType == StorageTypeEdge && destType == StorageTypeCloud) ||
		(sourceType == StorageTypeCloud && destType == StorageTypeEdge) {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.edgeToCloudBandwidthBytesPerSec,
			LatencyMs:            bg.edgeToCloudLatencyMs,
			MeasuredAt:           time.Now(),
			Reliability:          0.6,
		}
	}

	return &BandwidthInfo{
		SourceNode:           source,
		DestNode:             dest,
		BandwidthBytesPerSec: bg.defaultBandwidthBytesPerSec,
		LatencyMs:            bg.defaultLatencyMs,
		MeasuredAt:           time.Now(),
		Reliability:          0.5,
	}
}

// EstimateTransferTime calculates estimated data transfer time in seconds
func (bg *BandwidthGraph) EstimateTransferTime(source, dest string, sizeBytes int64) float64 {
	if sizeBytes <= 0 {
		return 0
	}

	info := bg.GetBandwidth(source, dest)

	bandwidth := info.BandwidthBytesPerSec
	if bandwidth <= 0 {
		bandwidth = bg.defaultBandwidthBytesPerSec
	}

	transferTime := float64(sizeBytes) / bandwidth
	transferTime += info.LatencyMs / 1000.0

	if sizeBytes > 100*1024*1024 { // over 100MB
		transferTime *= 1.1 // 10% overhead
	}

	return transferTime
}

// RemoveNode removes a node and all its bandwidth information
func (bg *BandwidthGraph) RemoveNode(nodeName string) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	delete(bg.bandwidthMap, nodeName)
	for _, destMap := range bg.bandwidthMap {
		delete(destMap, nodeName)
	}

	delete(bg.topology.NodeRegions, nodeName)
	delete(bg.topology.NodeZones, nodeName)
	delete(bg.topology.NodeTypes, nodeName)

	bg.lastUpdated = time.Now()
	klog.V(4).Infof("Removed node %s from bandwidth graph", nodeName)
}

// SetTopologyDefaults sets default bandwidth values based on topology
func (bg *BandwidthGraph) SetTopologyDefaults(
	localBandwidth, localLatency,
	sameZoneBandwidth, sameZoneLatency,
	sameRegionBandwidth, sameRegionLatency,
	edgeToCloudBandwidth, edgeToCloudLatency float64) {

	bg.mu.Lock()
	defer bg.mu.Unlock()

	if localBandwidth > 0 {
		bg.localBandwidthBytesPerSec = localBandwidth
	}

	if localLatency > 0 {
		bg.localLatencyMs = localLatency
	}

	if sameZoneBandwidth > 0 {
		bg.sameZoneBandwidthBytesPerSec = sameZoneBandwidth
	}

	if sameZoneLatency > 0 {
		bg.sameZoneLatencyMs = sameZoneLatency
	}

	if sameRegionBandwidth > 0 {
		bg.sameRegionBandwidthBytesPerSec = sameRegionBandwidth
	}

	if sameRegionLatency > 0 {
		bg.sameRegionLatencyMs = sameRegionLatency
	}

	if edgeToCloudBandwidth > 0 {
		bg.edgeToCloudBandwidthBytesPerSec = edgeToCloudBandwidth
	}

	if edgeToCloudLatency > 0 {
		bg.edgeToCloudLatencyMs = edgeToCloudLatency
	}

	bg.lastUpdated = time.Now()
}

// GetAllSources returns all source nodes in the bandwidth graph
func (bg *BandwidthGraph) GetAllSources() []string {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	sources := make([]string, 0, len(bg.bandwidthMap))
	for source := range bg.bandwidthMap {
		sources = append(sources, source)
	}

	sort.Strings(sources)
	return sources
}

// GetLastUpdated returns when the bandwidth graph was last updated
func (bg *BandwidthGraph) GetLastUpdated() time.Time {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	return bg.lastUpdated
}

// PrintSummary returns a formatted summary of the bandwidth graph
func (bg *BandwidthGraph) PrintSummary() string {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	var result strings.Builder

	fmt.Fprintf(&result, "Bandwidth Graph Summary (last updated: %s)\n",
		bg.lastUpdated.Format(time.RFC3339))
	fmt.Fprintf(&result, "- Default bandwidth: %.2f MB/s\n", bg.defaultBandwidthBytesPerSec/1e6)
	fmt.Fprintf(&result, "- Default latency: %.2f ms\n", bg.defaultLatencyMs)
	fmt.Fprintf(&result, "- Local bandwidth: %.2f MB/s\n", bg.localBandwidthBytesPerSec/1e6)
	fmt.Fprintf(&result, "- Local latency: %.2f ms\n", bg.localLatencyMs)
	fmt.Fprintf(&result, "- Same zone bandwidth: %.2f MB/s\n", bg.sameZoneBandwidthBytesPerSec/1e6)
	fmt.Fprintf(&result, "- Same zone latency: %.2f ms\n", bg.sameZoneLatencyMs)
	fmt.Fprintf(&result, "- Same region bandwidth: %.2f MB/s\n", bg.sameRegionBandwidthBytesPerSec/1e6)
	fmt.Fprintf(&result, "- Same region latency: %.2f ms\n", bg.sameRegionLatencyMs)
	fmt.Fprintf(&result, "- Edge-to-cloud bandwidth: %.2f MB/s\n", bg.edgeToCloudBandwidthBytesPerSec/1e6)
	fmt.Fprintf(&result, "- Edge-to-cloud latency: %.2f ms\n", bg.edgeToCloudLatencyMs)

	connectionCount := 0
	for _, destMap := range bg.bandwidthMap {
		connectionCount += len(destMap)
	}
	fmt.Fprintf(&result, "- Total connections: %d\n", connectionCount)
	fmt.Fprintf(&result, "- Nodes with topology info: %d\n", len(bg.topology.NodeRegions))

	edgeCount := 0
	cloudCount := 0
	for _, nodeType := range bg.topology.NodeTypes {
		if nodeType == StorageTypeEdge {
			edgeCount++
		} else if nodeType == StorageTypeCloud {
			cloudCount++
		}
	}

	fmt.Fprintf(&result, "- Edge nodes: %d\n", edgeCount)
	fmt.Fprintf(&result, "- Cloud nodes: %d\n", cloudCount)

	if connectionCount > 0 {
		fmt.Fprintf(&result, "\nHighest Bandwidth Connections:\n")

		type connection struct {
			source string
			dest   string
			info   *BandwidthInfo
		}

		connections := make([]connection, 0, connectionCount)
		for source, destMap := range bg.bandwidthMap {
			for dest, info := range destMap {
				connections = append(connections, connection{
					source: source,
					dest:   dest,
					info:   info,
				})
			}
		}

		sort.Slice(connections, func(i, j int) bool {
			return connections[i].info.BandwidthBytesPerSec > connections[j].info.BandwidthBytesPerSec
		})

		limit := min(len(connections), 10)

		for i := range limit {
			conn := connections[i]
			fmt.Fprintf(&result, "- %s -> %s: %.2f MB/s, %.2f ms (reliability: %.1f)\n",
				conn.source, conn.dest,
				conn.info.BandwidthBytesPerSec/1e6,
				conn.info.LatencyMs,
				conn.info.Reliability)
		}
	}

	return result.String()
}
