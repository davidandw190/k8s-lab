package daemon

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// Storage service labels
	StorageNodeLabel     = "node-capability/storage-service"
	StorageTypeLabel     = "node-capability/storage-type"
	StorageTechLabel     = "node-capability/storage-technology"
	StorageCapacityLabel = "node-capability/storage-capacity-bytes"
	BucketLabelPrefix    = "node-capability/storage-bucket-"

	// Network labels
	BandwidthPrefix = "node-capability/bandwidth-to-"
	LatencyPrefix   = "node-capability/latency-to-"

	// Bandwidth measurement interval
	BandwidthMeasurementInterval = 6 * time.Hour

	// Edge-cloud labels
	EdgeNodeLabel  = "node-capability/node-type"
	EdgeNodeValue  = "edge"
	CloudNodeValue = "cloud"

	// Topology labels
	RegionLabel = "topology.kubernetes.io/region"
	ZoneLabel   = "topology.kubernetes.io/zone"
)

// DataLocalityCollector collects information about storage and networking
type DataLocalityCollector struct {
	nodeName              string
	clientset             kubernetes.Interface
	bandwidthCache        map[string]int64
	bandwidthLatencyCache map[string]float64
	bandwidthCacheMutex   sync.RWMutex
	lastBandwidthUpdate   time.Time
	storageCache          map[string]interface{}
	storageCacheMutex     sync.RWMutex
	nodeType              string // "edge" or "cloud"
	region                string
	zone                  string
}

// NewDataLocalityCollector creates a new data locality collector
func NewDataLocalityCollector(nodeName string, clientset kubernetes.Interface) *DataLocalityCollector {
	return &DataLocalityCollector{
		nodeName:              nodeName,
		clientset:             clientset,
		bandwidthCache:        make(map[string]int64),
		bandwidthLatencyCache: make(map[string]float64),
		storageCache:          make(map[string]interface{}),
		lastBandwidthUpdate:   time.Now().Add(-BandwidthMeasurementInterval), // Force immediate update
	}
}

// CollectStorageCapabilities detects storage services and capabilities
func (c *DataLocalityCollector) CollectStorageCapabilities(ctx context.Context) (map[string]string, error) {
	labels := make(map[string]string)

	node, err := c.clientset.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", c.nodeName, err)
	}

	c.detectAndSetNodeTopology(node, labels)

	hasMinio, minioBuckets, storageCapacity, err := c.detectStorageService(ctx)
	if err != nil {
		klog.Warningf("Error detecting storage service: %v", err)
	}

	if hasMinio {
		labels[StorageNodeLabel] = "true"
		labels[StorageTypeLabel] = "object"

		if storageCapacity > 0 {
			labels[StorageCapacityLabel] = strconv.FormatInt(storageCapacity, 10)
		}

		// register buckets
		for _, bucket := range minioBuckets {
			labels[BucketLabelPrefix+bucket] = "true"
		}

		// detect storage technology (SSD, HDD, etc.)
		storageTech, err := c.detectStorageTechnology()
		if err == nil && storageTech != "" {
			labels[StorageTechLabel] = storageTech
		}
	} else {
		// check for other storage types (local PVs, etc.)
		hasLocalStorage, localCapacity := c.detectLocalStorage(ctx)
		if hasLocalStorage {
			labels[StorageNodeLabel] = "true"
			labels[StorageTypeLabel] = "local"

			if localCapacity > 0 {
				labels[StorageCapacityLabel] = strconv.FormatInt(localCapacity, 10)
			}

			storageTech, err := c.detectStorageTechnology()
			if err == nil && storageTech != "" {
				labels[StorageTechLabel] = storageTech
			}
		} else {
			labels[StorageNodeLabel] = ""
			labels[StorageTypeLabel] = ""
		}
	}

	// Update network measurements periodically
	if time.Since(c.lastBandwidthUpdate) > BandwidthMeasurementInterval {
		c.collectNetworkMeasurements(ctx, labels)
		c.lastBandwidthUpdate = time.Now()
	} else {
		c.bandwidthCacheMutex.RLock()
		for nodeName, bandwidth := range c.bandwidthCache {
			labels[BandwidthPrefix+nodeName] = strconv.FormatInt(bandwidth, 10)
		}

		for nodeName, latency := range c.bandwidthLatencyCache {
			labels[LatencyPrefix+nodeName] = strconv.FormatFloat(latency, 'f', 2, 64)
		}
		c.bandwidthCacheMutex.RUnlock()
	}

	return labels, nil
}

// detectAndSetNodeTopology identifies node type and topology
func (c *DataLocalityCollector) detectAndSetNodeTopology(node *v1.Node, labels map[string]string) {
	if nodeType, exists := node.Labels[EdgeNodeLabel]; exists {
		c.nodeType = nodeType
		labels[EdgeNodeLabel] = nodeType
	} else {
		isEdge := false

		if strings.Contains(strings.ToLower(node.Name), "edge") {
			isEdge = true
		}

		cpuCores := node.Status.Capacity.Cpu().Value()
		memoryBytes := node.Status.Allocatable.Memory().Value()

		if cpuCores <= 4 && memoryBytes <= 8*1024*1024*1024 {
			isEdge = true
		}

		for key, value := range node.Labels {
			if strings.Contains(key, "instance-type") &&
				(strings.Contains(value, "small") || strings.Contains(value, "micro")) {
				isEdge = true
				break
			}
		}

		if isEdge {
			c.nodeType = EdgeNodeValue
			labels[EdgeNodeLabel] = EdgeNodeValue
		} else {
			c.nodeType = CloudNodeValue
			labels[EdgeNodeLabel] = CloudNodeValue
		}
	}

	if region, exists := node.Labels[RegionLabel]; exists {
		c.region = region
		labels[RegionLabel] = region
	}

	if zone, exists := node.Labels[ZoneLabel]; exists {
		c.zone = zone
		labels[ZoneLabel] = zone
	}
}

// detectStorageService checks for storage services like MinIO running on the node
func (c *DataLocalityCollector) detectStorageService(ctx context.Context) (bool, []string, int64, error) {
	// For now, we're just using labels and mock data since we're asked to mock MinIO connections
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", c.nodeName)

	pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: "app=minio,app=storage-service",
	})

	if err != nil {
		return false, nil, 0, fmt.Errorf("failed to list storage service pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return false, nil, 0, nil
	}

	buckets := []string{"eo-scenes", "cog-data", "fmask-results", "data", "models"}

	var storageCapacity int64
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvcName := volume.PersistentVolumeClaim.ClaimName
				pvc, err := c.clientset.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
				if err == nil {
					if storage, exists := pvc.Status.Capacity["storage"]; exists {
						storageCapacity = storage.Value()
						break
					}
				}
			}
		}

		if storageCapacity > 0 {
			break
		}
	}

	return true, buckets, storageCapacity, nil
}

// detectLocalStorage checks for local storage capabilities
func (c *DataLocalityCollector) detectLocalStorage(ctx context.Context) (bool, int64) {
	// Check for local PVs attached to this node
	pvs, err := c.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list PersistentVolumes: %v", err)
		return false, 0
	}

	var totalCapacity int64
	hasLocalStorage := false

	for _, pv := range pvs.Items {
		// Check if PV is local to this node
		if c.isPVAttachedToNode(pv, c.nodeName) {
			hasLocalStorage = true

			if capacity, exists := pv.Spec.Capacity["storage"]; exists {
				totalCapacity += capacity.Value()
			}
		}
	}

	return hasLocalStorage, totalCapacity
}

// isPVAttachedToNode checks if a PV is attached to this node
func (c *DataLocalityCollector) isPVAttachedToNode(pv v1.PersistentVolume, nodeName string) bool {
	// Check node affinity
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" && expr.Operator == "In" {
					if slices.Contains(expr.Values, nodeName) {
						return true
					}
				}
			}
		}
	}

	// Check if this is a local volume
	if pv.Spec.Local != nil {
		return true
	}

	// Check annotations for node binding
	if boundNode, exists := pv.Annotations["volume.kubernetes.io/selected-node"]; exists {
		return boundNode == nodeName
	}

	return false
}

// detectStorageTechnology determines the type of storage (SSD, HDD, etc.)
func (c *DataLocalityCollector) detectStorageTechnology() (string, error) {
	// Check for NVMe drives
	cmd := exec.Command("ls", "/dev/nvme*")
	output, err := cmd.CombinedOutput()
	if err == nil && len(output) > 0 {
		return "nvme", nil
	}

	// Check for rotational status of primary disk
	cmd = exec.Command("lsblk", "-d", "-n", "-o", "NAME,ROTA")
	output, err = cmd.CombinedOutput()
	if err == nil {
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 && !strings.HasPrefix(fields[0], "loop") {
				// ROTA=0 means non-rotational (SSD), ROTA=1 means rotational (HDD)
				if fields[1] == "0" {
					return "ssd", nil
				} else if fields[1] == "1" {
					return "hdd", nil
				}
			}
		}
	}

	// Check if the root partition is mounted on SSD or HDD
	cmd = exec.Command("findmnt", "-n", "-o", "SOURCE", "/")
	output, err = cmd.CombinedOutput()
	if err == nil {
		source := strings.TrimSpace(string(output))
		if strings.HasPrefix(source, "/dev/") {
			// Extract device name
			device := strings.TrimPrefix(source, "/dev/")
			device = strings.Split(device, "p")[0] // Remove partition number
			device = strings.TrimRight(device, "0123456789")

			// Check rotational status
			rotaPath := fmt.Sprintf("/sys/block/%s/queue/rotational", device)
			cmd = exec.Command("cat", rotaPath)
			output, err = cmd.CombinedOutput()
			if err == nil {
				if strings.TrimSpace(string(output)) == "0" {
					return "ssd", nil
				} else {
					return "hdd", nil
				}
			}
		}
	}

	return "unknown", fmt.Errorf("could not determine storage technology")
}

// collectNetworkMeasurements measures network bandwidth between nodes
func (c *DataLocalityCollector) collectNetworkMeasurements(ctx context.Context, labels map[string]string) {
	klog.Infof("Starting network measurements for node %s", c.nodeName)

	// Get list of all nodes
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list nodes for bandwidth measurement: %v", err)
		return
	}

	// Local bandwidth is always high
	labels[BandwidthPrefix+"local"] = "1000000000" // 1 GB/s
	labels[LatencyPrefix+"local"] = "0.1"          // 0.1ms

	c.bandwidthCacheMutex.Lock()
	c.bandwidthCache = make(map[string]int64)
	c.bandwidthLatencyCache = make(map[string]float64)
	c.bandwidthCacheMutex.Unlock()

	// Group nodes by zone and region to prioritize measurements
	var sameZoneNodes, sameRegionNodes, otherNodes []v1.Node

	for _, node := range nodes.Items {
		if node.Name == c.nodeName {
			continue // Skip self
		}

		if !isNodeReady(&node) {
			continue // Skip unready nodes
		}

		// Categorize by zone/region proximity
		if c.zone != "" && node.Labels[ZoneLabel] == c.zone {
			sameZoneNodes = append(sameZoneNodes, node)
		} else if c.region != "" && node.Labels[RegionLabel] == c.region {
			sameRegionNodes = append(sameRegionNodes, node)
		} else {
			otherNodes = append(otherNodes, node)
		}
	}

	// Prioritize measurement order: same zone, same region, others
	// (with limits to avoid excessive measurements)
	nodesToMeasure := append(sameZoneNodes, sameRegionNodes...)
	nodesToMeasure = append(nodesToMeasure, otherNodes...)

	// Limit to 10 nodes to avoid excess measurements
	maxNodesToMeasure := 10
	if len(nodesToMeasure) > maxNodesToMeasure {
		nodesToMeasure = nodesToMeasure[:maxNodesToMeasure]
	}

	measuredCount := 0
	for _, node := range nodesToMeasure {
		// Get node IP
		var nodeIP string
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeInternalIP {
				nodeIP = address.Address
				break
			}
		}

		if nodeIP == "" {
			klog.V(4).Infof("Skipping bandwidth measurement to node %s (no internal IP)", node.Name)
			continue
		}

		// Measure bandwidth and latency
		bandwidth, latency, err := c.measureBandwidth(nodeIP)
		if err != nil {
			klog.Warningf("Failed to measure bandwidth to node %s (%s): %v", node.Name, nodeIP, err)
			// Use topology-based estimation instead
			bandwidth, latency = c.estimateBandwidthFromTopology(node)
		}

		// Store results
		labels[BandwidthPrefix+node.Name] = strconv.FormatInt(bandwidth, 10)
		labels[LatencyPrefix+node.Name] = strconv.FormatFloat(latency, 'f', 2, 64)

		c.bandwidthCacheMutex.Lock()
		c.bandwidthCache[node.Name] = bandwidth
		c.bandwidthLatencyCache[node.Name] = latency
		c.bandwidthCacheMutex.Unlock()

		measuredCount++
		klog.V(4).Infof("Measured bandwidth to %s: %d bytes/sec, %.2f ms", node.Name, bandwidth, latency)
	}

	klog.Infof("Network measurements complete for node %s: measured %d nodes", c.nodeName, measuredCount)
}

// measureBandwidth measures network performance between nodes
func (c *DataLocalityCollector) measureBandwidth(targetIP string) (int64, float64, error) {
	if targetIP == "" {
		return 0, 0, fmt.Errorf("empty target IP")
	}

	// First get ping latency as it's more reliable
	pingLatency, err := c.measurePingLatency(targetIP)
	if err != nil {
		return 0, 0, fmt.Errorf("ping failed: %w", err)
	}

	// Based on latency, estimate bandwidth using a simple model
	var bandwidth int64

	switch {
	case pingLatency < 1.0:
		bandwidth = 1000000000 // 1 GB/s for very low latency (local network)
	case pingLatency < 5.0:
		bandwidth = 500000000 // 500 MB/s for low latency (same datacenter)
	case pingLatency < 20.0:
		bandwidth = 200000000 // 200 MB/s for medium latency (same region)
	case pingLatency < 50.0:
		bandwidth = 100000000 // 100 MB/s for higher latency (regional)
	case pingLatency < 100.0:
		bandwidth = 50000000 // 50 MB/s for high latency (cross-region)
	default:
		bandwidth = 10000000 // 10 MB/s for very high latency (cross-continent)
	}

	// Adjust bandwidth for edge nodes
	if c.nodeType == EdgeNodeValue {
		bandwidth = int64(float64(bandwidth) * 0.7) // 30% reduction for edge nodes
	}

	return bandwidth, pingLatency, nil
}

// measurePingLatency measures round-trip time to target
func (c *DataLocalityCollector) measurePingLatency(targetIP string) (float64, error) {
	// Execute ping command
	cmd := exec.Command("ping", "-c", "3", "-W", "1", targetIP)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ping failed: %w", err)
	}

	// Parse ping output
	outputStr := string(output)

	// Look for the average round-trip time
	rttIndex := strings.Index(outputStr, "min/avg/max")
	if rttIndex == -1 {
		return 0, fmt.Errorf("couldn't parse ping output")
	}

	avgStart := strings.Index(outputStr[rttIndex:], "=") + rttIndex + 1
	if avgStart == -1 {
		return 0, fmt.Errorf("couldn't find average ping time")
	}

	parts := strings.Split(outputStr[avgStart:], "/")
	if len(parts) < 2 {
		return 0, fmt.Errorf("unexpected ping output format")
	}

	// Second part is the average
	avgStr := strings.TrimSpace(parts[1])
	avg, err := strconv.ParseFloat(avgStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse average ping time: %w", err)
	}

	return avg, nil
}

// estimateBandwidthFromTopology estimates bandwidth based on node topology
func (c *DataLocalityCollector) estimateBandwidthFromTopology(node v1.Node) (int64, float64) {
	// Get node type
	targetType := CloudNodeValue
	if value, exists := node.Labels[EdgeNodeLabel]; exists && value == EdgeNodeValue {
		targetType = EdgeNodeValue
	} else if strings.Contains(strings.ToLower(node.Name), "edge") {
		targetType = EdgeNodeValue
	}

	// Get region and zone
	targetRegion := node.Labels[RegionLabel]
	targetZone := node.Labels[ZoneLabel]

	// Default values
	var bandwidth int64
	var latency float64

	// Estimate based on topology relationship
	if c.zone != "" && c.zone == targetZone {
		// Same zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = 500000000 // 500 MB/s for edge-to-edge in same zone
			latency = 1.0         // 1ms latency
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = 1000000000 // 1 GB/s for cloud-to-cloud in same zone
			latency = 0.5          // 0.5ms latency
		} else {
			bandwidth = 750000000 // 750 MB/s for edge-to-cloud in same zone
			latency = 1.0         // 1ms latency
		}
	} else if c.region != "" && c.region == targetRegion {
		// Same region, different zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = 200000000 // 200 MB/s for edge-to-edge in same region
			latency = 5.0         // 5ms latency
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = 500000000 // 500 MB/s for cloud-to-cloud in same region
			latency = 2.0         // 2ms latency
		} else {
			bandwidth = 100000000 // 100 MB/s for edge-to-cloud in same region
			latency = 10.0        // 10ms latency
		}
	} else {
		// Different regions
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = 50000000 // 50 MB/s edge-to-edge
			latency = 50.0       // 50ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = 100000000 // 100 MB/s cloud-to-cloud
			latency = 20.0        // 20ms latency
		} else {
			bandwidth = 20000000 // 20 MB/s  edge-to-cloud
			latency = 100.0      // 100ms latency
		}
	}

	return bandwidth, latency
}

// isNodeReady checks if a node is in the Ready condition
func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
