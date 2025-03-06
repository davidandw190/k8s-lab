package storage

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// BandwidthGraph tracks network performance between nodes
type BandwidthGraph struct {
	// source->dest->path
	pathMap     map[string]map[string]*NetworkPath
	nodeRegions map[string]string
	nodeZones   map[string]string
	nodeTypes   map[string]StorageNodeType
	// default settings
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

func NewBandwidthGraph(defaultBandwidth float64) *BandwidthGraph {
	return &BandwidthGraph{
		pathMap:              make(map[string]map[string]*NetworkPath),
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
		edgeCloudBandwidth:   25e6,  // 25 MB/s
		edgeCloudLatency:     40.0,  // 40ms
		lastUpdated:          time.Now(),
	}
}

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

	if _, exists := bg.pathMap[source]; !exists {
		bg.pathMap[source] = make(map[string]*NetworkPath)
	}

	bg.pathMap[source][dest] = &NetworkPath{
		SourceNode:  source,
		DestNode:    dest,
		Bandwidth:   bandwidthBytesPerSec,
		Latency:     latencyMs,
		MeasuredAt:  time.Now(),
		Reliability: 1.0,
		IsEstimated: false,
	}

	bg.lastUpdated = time.Now()
	klog.V(4).Infof("Set bandwidth from %s to %s: %.2f MB/s, %.2f ms",
		source, dest, bandwidthBytesPerSec/1e6, latencyMs)
}

// GetNetworkPath retrieves network path information between nodes
func (bg *BandwidthGraph) GetNetworkPath(source, dest string) *NetworkPath {
	if source == "" || dest == "" {
		klog.Warningf("Cannot get network path for empty node name: [%s] -> [%s]", source, dest)
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.defaultBandwidth,
			Latency:     bg.defaultLatency,
			MeasuredAt:  time.Time{},
			Reliability: 0.5,
			IsEstimated: true,
		}
	}

	bg.mu.RLock()
	defer bg.mu.RUnlock()

	if source == dest {
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.localBandwidth,
			Latency:     bg.localLatency,
			MeasuredAt:  time.Now(),
			Reliability: 1.0,
			IsEstimated: false,
		}
	}

	// direct measurement from source to dest
	if sourceMap, exists := bg.pathMap[source]; exists {
		if path, exists := sourceMap[dest]; exists {
			return path
		}
	}

	// reverse path (assuming symmetric network)
	if sourceMap, exists := bg.pathMap[dest]; exists {
		if path, exists := sourceMap[source]; exists {
			return &NetworkPath{
				SourceNode:  source,
				DestNode:    dest,
				Bandwidth:   path.Bandwidth,
				Latency:     path.Latency,
				MeasuredAt:  path.MeasuredAt,
				Reliability: path.Reliability * 0.9,
				IsEstimated: true,
			}
		}
	}

	return bg.estimateFromTopology(source, dest)
}

func (bg *BandwidthGraph) estimateFromTopology(source, dest string) *NetworkPath {
	// check zone relationship
	sourceZone := bg.nodeZones[source]
	destZone := bg.nodeZones[dest]
	if sourceZone != "" && sourceZone == destZone {
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.sameZoneBandwidth,
			Latency:     bg.sameZoneLatency,
			MeasuredAt:  time.Now(),
			Reliability: 0.8,
			IsEstimated: true,
		}
	}

	// check region relationship
	sourceRegion := bg.nodeRegions[source]
	destRegion := bg.nodeRegions[dest]
	if sourceRegion != "" && sourceRegion == destRegion {
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.sameRegionBandwidth,
			Latency:     bg.sameRegionLatency,
			MeasuredAt:  time.Now(),
			Reliability: 0.7,
			IsEstimated: true,
		}
	}

	// check node types (edge vs cloud)
	sourceType := bg.nodeTypes[source]
	destType := bg.nodeTypes[dest]
	if (sourceType == StorageTypeEdge && destType == StorageTypeCloud) ||
		(sourceType == StorageTypeCloud && destType == StorageTypeEdge) {
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.edgeCloudBandwidth,
			Latency:     bg.edgeCloudLatency,
			MeasuredAt:  time.Now(),
			Reliability: 0.6,
			IsEstimated: true,
		}
	}

	if sourceRegion != "" && destRegion != "" && sourceRegion != destRegion {
		return &NetworkPath{
			SourceNode:  source,
			DestNode:    dest,
			Bandwidth:   bg.crossRegionBandwidth,
			Latency:     bg.crossRegionLatency,
			MeasuredAt:  time.Now(),
			Reliability: 0.5,
			IsEstimated: true,
		}
	}

	return &NetworkPath{
		SourceNode:  source,
		DestNode:    dest,
		Bandwidth:   bg.defaultBandwidth,
		Latency:     bg.defaultLatency,
		MeasuredAt:  time.Now(),
		Reliability: 0.4,
		IsEstimated: true,
	}
}

// EstimateTransferTime calculates the estimated time to transfer data between nodes
func (bg *BandwidthGraph) EstimateTransferTime(source, dest string, sizeBytes int64) float64 {
	if sizeBytes <= 0 {
		return 0
	}

	path := bg.GetNetworkPath(source, dest)

	bandwidth := path.Bandwidth
	if bandwidth <= 0 {
		bandwidth = bg.defaultBandwidth
	}

	transferTime := float64(sizeBytes) / bandwidth
	transferTime += path.Latency / 1000.0

	if sizeBytes > 10*1024*1024 { // over 10MB
		transferTime *= 1.1 // 10% overhead
	}

	// reliability factor - less reliable paths take longer in practice
	reliabilityFactor := 1.0 + (1.0-path.Reliability)*0.5
	transferTime *= reliabilityFactor

	bg.mu.RLock()
	nodeType := bg.nodeTypes[source]
	bg.mu.RUnlock()
	if nodeType == StorageTypeEdge && source != dest {
		transferTime *= 1.2 // +20% to account for limited uplink
	}

	return transferTime
}

func (bg *BandwidthGraph) RemoveNode(nodeName string) {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	delete(bg.pathMap, nodeName)
	for _, destMap := range bg.pathMap {
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
	bg.crossRegionLatency = bg.sameRegionLatency * 6.0

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

	var pathCount int
	for _, destMap := range bg.pathMap {
		pathCount += len(destMap)
	}
	fmt.Fprintf(&result, "- Total paths: %d\n", pathCount)
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

	if pathCount > 0 {
		fmt.Fprintf(&result, "\nTop 10 Bandwidth Paths:\n")

		type pathEntry struct {
			source string
			dest   string
			path   *NetworkPath
		}

		paths := make([]pathEntry, 0, pathCount)
		for source, destMap := range bg.pathMap {
			for dest, path := range destMap {
				paths = append(paths, pathEntry{
					source: source,
					dest:   dest,
					path:   path,
				})
			}
		}

		sort.Slice(paths, func(i, j int) bool {
			return paths[i].path.Bandwidth > paths[j].path.Bandwidth
		})

		limit := 10
		if len(paths) < limit {
			limit = len(paths)
		}

		for i := 0; i < limit; i++ {
			path := paths[i]
			estimatedStr := ""
			if path.path.IsEstimated {
				estimatedStr = " (estimated)"
			}
			fmt.Fprintf(&result, "- %s -> %s: %.2f MB/s, %.2f ms, reliability: %.1f%s\n",
				path.source, path.dest,
				path.path.Bandwidth/1e6,
				path.path.Latency,
				path.path.Reliability,
				estimatedStr)
		}
	}

	return result.String()
}

// MockNetworkPaths adds mock network paths for testing
func (bg *BandwidthGraph) MockNetworkPaths() {
	bg.mu.Lock()
	defer bg.mu.Unlock()

	// mock paths based on topology
	for source, sourceRegion := range bg.nodeRegions {
		sourceZone := bg.nodeZones[source]
		sourceType := bg.nodeTypes[source]

		for dest, destRegion := range bg.nodeRegions {
			if source == dest {
				continue
			}

			destZone := bg.nodeZones[dest]
			destType := bg.nodeTypes[dest]

			var bandwidth float64
			var latency float64
			var reliability float64

			// determine path characteristics based on topology relationship
			if sourceZone != "" && sourceZone == destZone {
				// Same zone
				bandwidth = bg.sameZoneBandwidth
				latency = bg.sameZoneLatency
				reliability = 0.9
			} else if sourceRegion != "" && sourceRegion == destRegion {
				// Same region
				bandwidth = bg.sameRegionBandwidth
				latency = bg.sameRegionLatency
				reliability = 0.8
			} else if (sourceType == StorageTypeEdge && destType == StorageTypeCloud) ||
				(sourceType == StorageTypeCloud && destType == StorageTypeEdge) {
				// Edge-cloud connection
				bandwidth = bg.edgeCloudBandwidth
				latency = bg.edgeCloudLatency
				reliability = 0.7
			} else {
				// Cross-region
				bandwidth = bg.crossRegionBandwidth
				latency = bg.crossRegionLatency
				reliability = 0.6
			}

			jitterFactor := 0.85 + (rand.Float64() * 0.3)
			bandwidth *= jitterFactor
			latency *= 2.0 - jitterFactor

			if _, exists := bg.pathMap[source]; !exists {
				bg.pathMap[source] = make(map[string]*NetworkPath)
			}

			bg.pathMap[source][dest] = &NetworkPath{
				SourceNode:  source,
				DestNode:    dest,
				Bandwidth:   bandwidth,
				Latency:     latency,
				MeasuredAt:  time.Now(),
				Reliability: reliability,
				IsEstimated: true,
			}
		}
	}

	bg.lastUpdated = time.Now()

	var pathCount int
	for _, destMap := range bg.pathMap {
		pathCount += len(destMap)
	}

	klog.Infof("Added %d mock network paths", pathCount)
}
