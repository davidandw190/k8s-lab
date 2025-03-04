package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// BandwidthGraph tracks network performance between nodes
type BandwidthGraph struct {
	bandwidthMap         map[string]map[string]*BandwidthInfo
	nodeRegions          map[string]string
	nodeZones            map[string]string
	nodeTypes            map[string]StorageNodeType
	localBandwidth       float64
	localLatency         float64
	sameZoneBandwidth    float64
	sameZoneLatency      float64
	sameRegionBandwidth  float64
	sameRegionLatency    float64
	crossRegionBandwidth float64
	crossRegionLatency   float64
	edgeCloudBandwidth   float64
	edgeCloudLatency     float64
	defaultBandwidth     float64
	defaultLatency       float64
	mu                   sync.RWMutex
	lastUpdated          time.Time
}

// NewBandwidthGraph creates a new bandwidth graph
func NewBandwidthGraph(defaultBandwidth float64) *BandwidthGraph {
	return &BandwidthGraph{
		bandwidthMap:         make(map[string]map[string]*BandwidthInfo),
		nodeRegions:          make(map[string]string),
		nodeZones:            make(map[string]string),
		nodeTypes:            make(map[string]StorageNodeType),
		defaultBandwidth:     defaultBandwidth,
		defaultLatency:       10.0,  // 10ms
		localBandwidth:       1e9,   // 1 GB/s
		localLatency:         0.1,   // 0.1ms
		sameZoneBandwidth:    500e6, // 500 MB/s
		sameZoneLatency:      1.0,   // 1ms
		sameRegionBandwidth:  200e6, // 200 MB/s
		sameRegionLatency:    5.0,   // 5ms
		crossRegionBandwidth: 50e6,  // 50 MB/s
		crossRegionLatency:   30.0,  // 30ms
		edgeCloudBandwidth:   50e6,  // 50 MB/s
		edgeCloudLatency:     20.0,  // 20ms
		lastUpdated:          time.Now(),
	}
}

// SetNodeTopology adds topology information for a node
func (bg *BandwidthGraph) SetNodeTopology(nodeName, region, zone string, nodeType StorageNodeType) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	bg.nodeRegions[nodeName] = region
	bg.nodeZones[nodeName] = zone
	bg.nodeTypes[nodeName] = nodeType
}

// SetBandwidth sets the bandwidth and latency between two nodes
func (bg *BandwidthGraph) SetBandwidth(source, dest string, bandwidthBytesPerSec, latencyMs float64) {
	if source == "" || dest == "" {
		klog.Warningf("Cannot set bandwidth for empty node name: [%s] -> [%s]", source, dest)
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
		klog.Warningf("Cannot get bandwidth for empty node name: [%s] -> [%s]", source, dest)
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.defaultBandwidth,
			LatencyMs:            bg.defaultLatency,
			MeasuredAt:           time.Time{},
			Reliability:          0.5,
		}
	}

	bg.mu.RLock()
	defer bg.mu.RUnlock()

	if source == dest {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.localBandwidth,
			LatencyMs:            bg.localLatency,
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
	sourceZone := bg.nodeZones[source]
	destZone := bg.nodeZones[dest]

	if sourceZone != "" && sourceZone == destZone {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.sameZoneBandwidth,
			LatencyMs:            bg.sameZoneLatency,
			MeasuredAt:           time.Now(),
			Reliability:          0.8,
		}
	}

	sourceRegion := bg.nodeRegions[source]
	destRegion := bg.nodeRegions[dest]

	if sourceRegion != "" && sourceRegion == destRegion {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.sameRegionBandwidth,
			LatencyMs:            bg.sameRegionLatency,
			MeasuredAt:           time.Now(),
			Reliability:          0.7,
		}
	}

	sourceType := bg.nodeTypes[source]
	destType := bg.nodeTypes[dest]

	if (sourceType == StorageTypeEdge && destType == StorageTypeCloud) ||
		(sourceType == StorageTypeCloud && destType == StorageTypeEdge) {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.edgeCloudBandwidth,
			LatencyMs:            bg.edgeCloudLatency,
			MeasuredAt:           time.Now(),
			Reliability:          0.6,
		}
	}

	// Different regions
	if sourceRegion != "" && destRegion != "" && sourceRegion != destRegion {
		return &BandwidthInfo{
			SourceNode:           source,
			DestNode:             dest,
			BandwidthBytesPerSec: bg.crossRegionBandwidth,
			LatencyMs:            bg.crossRegionLatency,
			MeasuredAt:           time.Now(),
			Reliability:          0.5,
		}
	}

	// Default fallback
	return &BandwidthInfo{
		SourceNode:           source,
		DestNode:             dest,
		BandwidthBytesPerSec: bg.defaultBandwidth,
		LatencyMs:            bg.defaultLatency,
		MeasuredAt:           time.Now(),
		Reliability:          0.4,
	}
}

// EstimateTransferTime calculates the transfer time for data between nodes
func (bg *BandwidthGraph) EstimateTransferTime(source, dest string, sizeBytes int64) float64 {
	if sizeBytes <= 0 {
		return 0
	}

	info := bg.GetBandwidth(source, dest)

	bandwidth := info.BandwidthBytesPerSec
	if bandwidth <= 0 {
		bandwidth = bg.defaultBandwidth
	}

	transferTime := float64(sizeBytes) / bandwidth
	transferTime += info.LatencyMs / 1000.0

	if sizeBytes > 100*1024*1024 { // Over 100MB
		transferTime *= 1.1 // 10% overhead for protocol overhead, network variability
	}

	return transferTime
}

// RemoveNode removes a node from the bandwidth graph
func (bg *BandwidthGraph) RemoveNode(nodeName string) {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	delete(bg.bandwidthMap, nodeName)

	for _, destMap := range bg.bandwidthMap {
		delete(destMap, nodeName)
	}

	delete(bg.nodeRegions, nodeName)
	delete(bg.nodeZones, nodeName)
	delete(bg.nodeTypes, nodeName)

	bg.lastUpdated = time.Now()
	klog.V(4).Infof("Removed node %s from bandwidth graph", nodeName)
}

// SetTopologyDefaults sets default bandwidth and latency values based on topology
func (bg *BandwidthGraph) SetTopologyDefaults(
	localBandwidth, localLatency,
	sameZoneBandwidth, sameZoneLatency,
	sameRegionBandwidth, sameRegionLatency,
	edgeCloudBandwidth, edgeCloudLatency float64) {

	bg.mu.Lock()
	defer bg.mu.Unlock()

	if localBandwidth > 0 {
		bg.localBandwidth = localBandwidth
	}

	if localLatency > 0 {
		bg.localLatency = localLatency
	}

	if sameZoneBandwidth > 0 {
		bg.sameZoneBandwidth = sameZoneBandwidth
	}

	if sameZoneLatency > 0 {
		bg.sameZoneLatency = sameZoneLatency
	}

	if sameRegionBandwidth > 0 {
		bg.sameRegionBandwidth = sameRegionBandwidth
	}

	if sameRegionLatency > 0 {
		bg.sameRegionLatency = sameRegionLatency
	}

	if edgeCloudBandwidth > 0 {
		bg.edgeCloudBandwidth = edgeCloudBandwidth
	}

	if edgeCloudLatency > 0 {
		bg.edgeCloudLatency = edgeCloudLatency
	}

	bg.crossRegionBandwidth = bg.sameRegionBandwidth * 0.25
	bg.crossRegionLatency = bg.sameRegionLatency * 4.0

	bg.lastUpdated = time.Now()
	klog.V(4).Infof("Updated bandwidth topology defaults")
}

// PrintSummary returns a string representation of the bandwidth graph
func (bg *BandwidthGraph) PrintSummary() string {
	bg.mu.RLock()
	defer bg.mu.RUnlock()

	var result strings.Builder

	fmt.Fprintf(&result, "Bandwidth Graph Summary (last updated: %s)\n",
		bg.lastUpdated.Format(time.RFC3339))
	fmt.Fprintf(&result, "- Default bandwidth: %.2f MB/s\n", bg.defaultBandwidth/1e6)
	fmt.Fprintf(&result, "- Default latency: %.2f ms\n", bg.defaultLatency)
	fmt.Fprintf(&result, "- Local bandwidth: %.2f MB/s\n", bg.localBandwidth/1e6)
	fmt.Fprintf(&result, "- Local latency: %.2f ms\n", bg.localLatency)
	fmt.Fprintf(&result, "- Same zone bandwidth: %.2f MB/s\n", bg.sameZoneBandwidth/1e6)
	fmt.Fprintf(&result, "- Same zone latency: %.2f ms\n", bg.sameZoneLatency)
	fmt.Fprintf(&result, "- Same region bandwidth: %.2f MB/s\n", bg.sameRegionBandwidth/1e6)
	fmt.Fprintf(&result, "- Same region latency: %.2f ms\n", bg.sameRegionLatency)
	fmt.Fprintf(&result, "- Cross region bandwidth: %.2f MB/s\n", bg.crossRegionBandwidth/1e6)
	fmt.Fprintf(&result, "- Cross region latency: %.2f ms\n", bg.crossRegionLatency)
	fmt.Fprintf(&result, "- Edge-to-cloud bandwidth: %.2f MB/s\n", bg.edgeCloudBandwidth/1e6)
	fmt.Fprintf(&result, "- Edge-to-cloud latency: %.2f ms\n", bg.edgeCloudLatency)

	var connectionCount int
	for _, destMap := range bg.bandwidthMap {
		connectionCount += len(destMap)
	}
	fmt.Fprintf(&result, "- Total connections: %d\n", connectionCount)
	fmt.Fprintf(&result, "- Nodes with topology info: %d\n", len(bg.nodeRegions))

	edgeCount := 0
	cloudCount := 0
	for _, nodeType := range bg.nodeTypes {
		if nodeType == StorageTypeEdge {
			edgeCount++
		} else {
			cloudCount++
		}
	}
	fmt.Fprintf(&result, "- Edge nodes: %d\n", edgeCount)
	fmt.Fprintf(&result, "- Cloud nodes: %d\n", cloudCount)

	if connectionCount > 0 {
		fmt.Fprintf(&result, "\nTop 10 Bandwidth Connections:\n")

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
