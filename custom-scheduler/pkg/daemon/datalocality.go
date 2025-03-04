package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"maps"
	"slices"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// Storage service
	StorageServiceLabelSelector = "app=storage-service"
	MinIOSelector               = "app=minio"
	StorageNodeLabel            = "node-capability/storage-service"
	StorageTypeLabel            = "node-capability/storage-type"

	// Bandwidth
	BandwidthPrefix = "node-capability/bandwidth-to-"
	BandwidthPeriod = 24 * time.Hour

	// Edge-cloud
	EdgeNodeLabel  = "node-capability/node-type"
	EdgeNodeValue  = "edge"
	CloudNodeValue = "cloud"

	// Topology
	RegionLabel = "topology.kubernetes.io/region"
	ZoneLabel   = "topology.kubernetes.io/zone"
)

// StorageInfo represents storage capabilities and status
type StorageInfo struct {
	Provider  string   `json:"provider"`
	Buckets   []string `json:"buckets,omitempty"`
	Capacity  int64    `json:"capacity"`
	Available int64    `json:"available"`
	Endpoints []string `json:"endpoints,omitempty"`
}

// DataLocalityCollector collects information about data locality
type DataLocalityCollector struct {
	nodeName              string
	clientset             kubernetes.Interface
	bandwidthCache        map[string]int64
	bandwidthLatencyCache map[string]float64
	bandwidthCacheLock    sync.RWMutex
	lastUpdate            time.Time
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
		lastUpdate:            time.Now().Add(-BandwidthPeriod), // to force immediate update
		nodeType:              CloudNodeValue,                   // default to cloud
	}
}

// CollectStorageCapabilities collects storage service information on the node
func (c *DataLocalityCollector) CollectStorageCapabilities(ctx context.Context) (map[string]string, error) {
	labels := make(map[string]string)

	node, err := c.clientset.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", c.nodeName, err)
	}

	c.detectAndSetNodeType(node, labels)
	c.detectAndSetTopology(node, labels)

	hasMinio, minioBuckets, minioCap, err := c.detectMinIOService(ctx)
	if err != nil {
		klog.Warningf("Error detecting MinIO service: %v", err)
	}

	if hasMinio {
		labels[StorageNodeLabel] = "true"
		labels[StorageTypeLabel] = "minio"

		if minioCap > 0 {
			labels["node-capability/storage-capacity"] = strconv.FormatInt(minioCap, 10)
		}

		for _, bucket := range minioBuckets {
			labels[fmt.Sprintf("node-capability/storage-bucket-%s", bucket)] = "true"
		}
	}

	hasStorage, storageLabels := c.detectNodeStorage(ctx)
	if hasStorage {
		for k, v := range storageLabels {
			labels[k] = v
		}
	}

	if !hasMinio && !hasStorage {
		for key := range node.Labels {
			if strings.HasPrefix(key, StorageNodeLabel) ||
				strings.HasPrefix(key, StorageTypeLabel) ||
				strings.HasPrefix(key, "node-capability/storage-bucket-") {
				labels[key] = "" // mark for deletion
			}
		}
	}

	if time.Since(c.lastUpdate) > BandwidthPeriod {
		c.collectNetworkMeasurements(ctx, labels)
		c.lastUpdate = time.Now()
	} else {
		c.bandwidthCacheLock.RLock()
		for nodeName, bandwidth := range c.bandwidthCache {
			labels[fmt.Sprintf("%s%s", BandwidthPrefix, nodeName)] = strconv.FormatInt(bandwidth, 10)
		}
		c.bandwidthCacheLock.RUnlock()
	}

	deviceType, err := c.detectStorageDeviceType()
	if err == nil && deviceType != "" {
		labels["node-capability/device-storage-type"] = deviceType
	}

	gpuCapability, gpuLabels := c.detectGPUCapabilities()
	if gpuCapability {
		maps.Copy(labels, gpuLabels)
	}

	return labels, nil
}

// detectAndSetNodeType identifies if a node is edge or cloud
func (c *DataLocalityCollector) detectAndSetNodeType(node *v1.Node, labels map[string]string) {
	if nodeType, ok := node.Labels[EdgeNodeLabel]; ok {
		c.nodeType = nodeType
		labels[EdgeNodeLabel] = nodeType
		return
	}

	if strings.Contains(strings.ToLower(node.Name), "edge") {
		c.nodeType = EdgeNodeValue
		labels[EdgeNodeLabel] = EdgeNodeValue
		return
	}

	isEdge := false

	cpuCores := node.Status.Capacity.Cpu().Value()
	memoryBytes := node.Status.Capacity.Memory().Value()

	if cpuCores < 4 && memoryBytes < 8*1024*1024*1024 {
		isEdge = true
	}

	for k, v := range node.Labels {
		if (strings.Contains(k, "instance-type") &&
			(strings.Contains(v, "small") || strings.Contains(v, "micro"))) ||
			strings.Contains(k, "edge") ||
			strings.Contains(k, "iot") {
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

// detectAndSetTopology extracts region/zone information from node labels
func (c *DataLocalityCollector) detectAndSetTopology(node *v1.Node, labels map[string]string) {
	// extract region
	if region, ok := node.Labels[RegionLabel]; ok {
		c.region = region
		labels[RegionLabel] = region
	} else {
		for k, v := range node.Labels {
			if strings.Contains(k, "region") {
				c.region = v
				labels[RegionLabel] = v
				break
			}
		}
	}

	// extract zone
	if zone, ok := node.Labels[ZoneLabel]; ok {
		c.zone = zone
		labels[ZoneLabel] = zone
	} else {
		for k, v := range node.Labels {
			if strings.Contains(k, "zone") {
				c.zone = v
				labels[ZoneLabel] = v
				break
			}
		}
	}
}

// detectMinIOService checks if MinIO is running on the node and gets bucket information
func (c *DataLocalityCollector) detectMinIOService(ctx context.Context) (bool, []string, int64, error) {
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", c.nodeName)

	minioPods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: MinIOSelector,
	})

	if err != nil {
		return false, nil, 0, fmt.Errorf("failed to list MinIO pods: %w", err)
	}

	if len(minioPods.Items) == 0 {
		return false, nil, 0, nil
	}

	buckets, err := c.collectMinioBuckets(ctx, minioPods.Items[0])
	if err != nil {
		klog.Warningf("Failed to collect MinIO buckets: %v", err)
		// buckets = []string{"data", "models", "results", "eo-scenes", "cog-data"}
	}

	// Get storage capacity
	var capacity int64
	for _, pod := range minioPods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				pvcName := volume.PersistentVolumeClaim.ClaimName
				pvc, err := c.clientset.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
				if err == nil {
					if storage, ok := pvc.Status.Capacity["storage"]; ok {
						capacity = storage.Value()
						break
					}
				}
			}
		}

		if capacity > 0 {
			break
		}
	}

	return true, buckets, capacity, nil
}

// detectNodeStorage checks for storage attached to the node via PersistentVolumes
func (c *DataLocalityCollector) detectNodeStorage(ctx context.Context) (bool, map[string]string) {
	labels := make(map[string]string)
	hasStorage := false

	// detect attached PVs
	pvs, err := c.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list PersistentVolumes: %v", err)
		return false, labels
	}

	var totalCapacity int64
	var storageClasses []string

	for _, pv := range pvs.Items {
		if c.isPVAttachedToNode(pv, c.nodeName) {
			hasStorage = true
			labels[StorageNodeLabel] = "true"

			if pv.Spec.StorageClassName != "" && !containsString(storageClasses, pv.Spec.StorageClassName) {
				storageClasses = append(storageClasses, pv.Spec.StorageClassName)
				labels[fmt.Sprintf("node-capability/storage-class-%s", pv.Spec.StorageClassName)] = "true"
			}

			if capacity, ok := pv.Spec.Capacity["storage"]; ok {
				totalCapacity += capacity.Value()
			}
		}
	}

	if hasStorage && totalCapacity > 0 {
		labels["node-capability/storage-capacity"] = strconv.FormatInt(totalCapacity, 10)
	}

	return hasStorage, labels
}

// isPVAttachedToNode checks if a PersistentVolume is attached to a specific node
func (c *DataLocalityCollector) isPVAttachedToNode(pv v1.PersistentVolume, nodeName string) bool {
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

	if pv.Spec.Local != nil {
		return true
	}

	// Check if the volume is bound to a node via annotations
	if boundNode, ok := pv.Annotations["volume.kubernetes.io/selected-node"]; ok {
		return boundNode == c.nodeName
	}

	return false
}

// collectMinioBuckets attempts to list buckets from a MinIO instance
func (c *DataLocalityCollector) collectMinioBuckets(_ context.Context, pod v1.Pod) ([]string, error) {
	return []string{"data", "temp", "results", "scenes"}, nil
}

// collectNetworkMeasurements collects network performance measurements
func (c *DataLocalityCollector) collectNetworkMeasurements(ctx context.Context, labels map[string]string) {
	klog.Infof("Starting network measurement collection for node %s", c.nodeName)

	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list nodes for bandwidth measurement: %v", err)
		return
	}

	labels["node-capability/bandwidth-local"] = "1000000000" // 1 GB/s

	c.bandwidthCacheLock.Lock()
	c.bandwidthCache = make(map[string]int64)
	c.bandwidthLatencyCache = make(map[string]float64)
	c.bandwidthCacheLock.Unlock()

	measuredCount := 0

	var edgeNodes []v1.Node
	var cloudNodes []v1.Node
	var sameRegionNodes []v1.Node
	var differentRegionNodes []v1.Node

	for _, node := range nodes.Items {
		if node.Name == c.nodeName {
			continue // skip self
		}

		if !isNodeReady(&node) {
			continue
		}

		nodeType := CloudNodeValue
		if val, ok := node.Labels[EdgeNodeLabel]; ok {
			nodeType = val
		} else if strings.Contains(strings.ToLower(node.Name), "edge") {
			nodeType = EdgeNodeValue
		}

		if nodeType == EdgeNodeValue {
			edgeNodes = append(edgeNodes, node)
		} else {
			cloudNodes = append(cloudNodes, node)
		}

		if c.region != "" {
			nodeRegion := node.Labels[RegionLabel]
			if nodeRegion == c.region {
				sameRegionNodes = append(sameRegionNodes, node)
			} else if nodeRegion != "" {
				differentRegionNodes = append(differentRegionNodes, node)
			}
		}
	}

	var nodesToMeasure []v1.Node

	nodesToMeasure = append(nodesToMeasure, sameRegionNodes...)

	// Then add nodes based on edge/cloud type
	if c.nodeType == EdgeNodeValue {
		// Edge node should prioritize other edge nodes, then cloud
		remainingEdge := filterOutNodes(edgeNodes, nodesToMeasure)
		nodesToMeasure = append(nodesToMeasure, remainingEdge...)
		remainingCloud := filterOutNodes(cloudNodes, nodesToMeasure)
		nodesToMeasure = append(nodesToMeasure, remainingCloud...)
	} else {
		// Cloud node should prioritize other cloud nodes, then edge
		remainingCloud := filterOutNodes(cloudNodes, nodesToMeasure)
		nodesToMeasure = append(nodesToMeasure, remainingCloud...)
		remainingEdge := filterOutNodes(edgeNodes, nodesToMeasure)
		nodesToMeasure = append(nodesToMeasure, remainingEdge...)
	}

	// Finally add any remaining nodes
	for _, node := range nodes.Items {
		if node.Name == c.nodeName {
			continue
		}

		if !isNodeReady(&node) {
			continue
		}

		if !containsNode(nodesToMeasure, node.Name) {
			nodesToMeasure = append(nodesToMeasure, node)
		}
	}

	// Limit to measuring at most 10 nodes to avoid excessive load
	maxNodesToMeasure := 10
	if len(nodesToMeasure) > maxNodesToMeasure {
		nodesToMeasure = nodesToMeasure[:maxNodesToMeasure]
	}

	// Perform measurements
	for _, node := range nodesToMeasure {
		// Get internal IP of the node
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

		// Measure bandwidth
		bandwidth, latency, err := c.MeasureBandwidth(nodeIP)
		if err != nil {
			klog.Warningf("Failed to measure bandwidth to %s (%s): %v", node.Name, nodeIP, err)
			// Use topology-aware estimation
			bandwidth, latency = c.estimateBandwidthFromTopology(node)
		}

		// Store in labels
		bandwidthLabel := fmt.Sprintf("%s%s", BandwidthPrefix, node.Name)
		labels[bandwidthLabel] = strconv.FormatInt(bandwidth, 10)

		// Store latency in a separate label
		latencyLabel := fmt.Sprintf("node-capability/latency-to-%s", node.Name)
		labels[latencyLabel] = strconv.FormatFloat(latency, 'f', 2, 64)

		// Update cache
		c.bandwidthCacheLock.Lock()
		c.bandwidthCache[node.Name] = bandwidth
		c.bandwidthLatencyCache[node.Name] = latency
		c.bandwidthCacheLock.Unlock()

		measuredCount++
		klog.V(4).Infof("Measured bandwidth to %s: %d bytes/sec, %.2f ms", node.Name, bandwidth, latency)
	}

	klog.Infof("Completed network measurements for node %s: measured %d nodes", c.nodeName, measuredCount)
}

// MeasureBandwidth measures bandwidth between current node and another node
func (c *DataLocalityCollector) MeasureBandwidth(targetIP string) (int64, float64, error) {
	if targetIP == "" {
		return 10000000, 10.0, fmt.Errorf("empty target IP")
	}

	// First, check if we can reach the target
	timeout := 1 * time.Second
	conn, err := net.DialTimeout("tcp", targetIP+":22", timeout)
	if err != nil {
		// If we can't connect to SSH, try HTTP or HTTPS ports
		conn, err = net.DialTimeout("tcp", targetIP+":80", timeout)
		if err != nil {
			conn, err = net.DialTimeout("tcp", targetIP+":443", timeout)
			if err != nil {
				// Fall back to ping-based estimation
				return c.measureWithPing(targetIP)
			}
		}
	}

	if conn != nil {
		conn.Close()
	}

	// Measure ping latency first for a quick estimate
	pingLatency, err := c.getPingLatency(targetIP)
	if err != nil {
		pingLatency = 10.0 // Default 10ms if ping fails
	}

	// Simple model based on latency:
	// - Less than 1ms: likely local network, high bandwidth
	// - 1-5ms: likely same datacenter, good bandwidth
	// - 5-20ms: likely same region, decent bandwidth
	// - 20-100ms: likely cross-region, lower bandwidth
	// - 100ms+: likely cross-continent, poor bandwidth
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

	// For geospatial workloads, which often involve large file transfers,
	// adjust bandwidth estimates based on workload characteristics
	// Earth Observation data typically requires high-throughput connections
	if c.nodeType == EdgeNodeValue {
		// Edge nodes typically have more limited bandwidth
		bandwidth = int64(float64(bandwidth) * 0.7) // 30% reduction for edge nodes
	}

	return bandwidth, pingLatency, nil
}

// measureWithPing estimates bandwidth based on ping results
func (c *DataLocalityCollector) measureWithPing(targetIP string) (int64, float64, error) {
	latency, err := c.getPingLatency(targetIP)
	if err != nil {
		// Conservative default if ping fails
		if c.nodeType == EdgeNodeValue {
			return 10000000, 100.0, err // 10 MB/s, 100ms for edge nodes
		}
		return 50000000, 50.0, err // 50 MB/s, 50ms for cloud nodes
	}

	// Model bandwidth as inversely related to latency, with minimum thresholds
	var bandwidth int64

	switch {
	case latency < 1.0:
		bandwidth = 1000000000 // 1 GB/s
	case latency < 5.0:
		bandwidth = 500000000 // 500 MB/s
	case latency < 20.0:
		bandwidth = 200000000 // 200 MB/s
	case latency < 50.0:
		bandwidth = 100000000 // 100 MB/s
	case latency < 100.0:
		bandwidth = 50000000 // 50 MB/s
	default:
		bandwidth = 10000000 // 10 MB/s
	}

	// Adjust for edge nodes
	if c.nodeType == EdgeNodeValue {
		bandwidth = int64(float64(bandwidth) * 0.7) // 30% reduction for edge nodes
	}

	return bandwidth, latency, nil
}

// getPingLatency sends ICMP pings and calculates average latency
func (c *DataLocalityCollector) getPingLatency(targetIP string) (float64, error) {
	// Execute ping with 3 packets, 1 second timeout
	cmd := exec.Command("ping", "-c", "3", "-W", "1", targetIP)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ping failed: %w", err)
	}

	// Parse ping output to extract latency
	outputStr := string(output)

	// Look for the average round-trip time
	// Example: rtt min/avg/max/mdev = 0.042/0.045/0.048/0.002 ms
	rttIndex := strings.Index(outputStr, "min/avg/max")
	if rttIndex == -1 {
		return 100.0, fmt.Errorf("couldn't parse ping output")
	}

	// Extract the part after "="
	rttPart := outputStr[strings.Index(outputStr[rttIndex:], "=")+rttIndex+1:]

	// Split by "/"
	parts := strings.Split(rttPart, "/")
	if len(parts) < 4 {
		return 100.0, fmt.Errorf("unexpected ping output format")
	}

	// Second part is the average
	avgStr := strings.TrimSpace(parts[1])
	avg, err := strconv.ParseFloat(avgStr, 64)
	if err != nil {
		return 100.0, fmt.Errorf("failed to parse average ping time: %w", err)
	}

	return avg, nil
}

// estimateBandwidthFromTopology estimates bandwidth based on node topology
func (c *DataLocalityCollector) estimateBandwidthFromTopology(node v1.Node) (int64, float64) {
	// Determine node type (edge vs cloud)
	nodeType := CloudNodeValue
	if val, ok := node.Labels[EdgeNodeLabel]; ok {
		nodeType = val
	} else if strings.Contains(strings.ToLower(node.Name), "edge") {
		nodeType = EdgeNodeValue
	}

	// Default bandwidth values based on node types
	var bandwidth int64
	var latency float64

	// Get region and zone
	nodeRegion := node.Labels[RegionLabel]
	nodeZone := node.Labels[ZoneLabel]

	// Check if nodes are in the same zone
	if c.zone != "" && c.zone == nodeZone {
		if c.nodeType == EdgeNodeValue && nodeType == EdgeNodeValue {
			// Edge to edge in same zone
			bandwidth = 500000000 // 500 MB/s
			latency = 1.0         // 1ms
		} else if c.nodeType == CloudNodeValue && nodeType == CloudNodeValue {
			// Cloud to cloud in same zone
			bandwidth = 1000000000 // 1 GB/s
			latency = 0.5          // 0.5ms
		} else {
			// Edge to cloud in same zone
			bandwidth = 750000000 // 750 MB/s
			latency = 1.0         // 1ms
		}
	} else if c.region != "" && c.region == nodeRegion {
		if c.nodeType == EdgeNodeValue && nodeType == EdgeNodeValue {
			// Edge to edge in same region
			bandwidth = 200000000 // 200 MB/s
			latency = 5.0         // 5ms
		} else if c.nodeType == CloudNodeValue && nodeType == CloudNodeValue {
			// Cloud to cloud in same region
			bandwidth = 500000000 // 500 MB/s
			latency = 2.0         // 2ms
		} else {
			// Edge to cloud in same region
			bandwidth = 100000000 // 100 MB/s
			latency = 10.0        // 10ms
		}
	} else {
		if c.nodeType == EdgeNodeValue && nodeType == EdgeNodeValue {
			// Edge to edge across regions
			bandwidth = 50000000 // 50 MB/s
			latency = 50.0       // 50ms
		} else if c.nodeType == CloudNodeValue && nodeType == CloudNodeValue {
			// Cloud to cloud across regions
			bandwidth = 100000000 // 100 MB/s
			latency = 20.0        // 20ms
		} else {
			// Edge to cloud across regions
			bandwidth = 20000000 // 20 MB/s
			latency = 100.0      // 100ms
		}
	}

	// Add specific adjustment for Earth Observation workloads
	// EO data is typically large and benefits from locality-aware placement
	if c.nodeType == EdgeNodeValue && nodeType == CloudNodeValue {
		// Edge to cloud transfers are especially challenging for EO data
		bandwidth = int64(float64(bandwidth) * 0.8) // 20% reduction
		latency = latency * 1.2                     // 20% increase
	}

	return bandwidth, latency
}

// detectStorageDeviceType attempts to determine the underlying storage type (SSD/HDD/NVMe)
func (c *DataLocalityCollector) detectStorageDeviceType() (string, error) {
	cmd := exec.Command("ls", "/dev/nvme*")
	output, err := cmd.CombinedOutput()
	if err == nil && len(output) > 0 {
		return "nvme", nil
	}

	cmd = exec.Command("lsblk", "-d", "-o", "NAME,ROTA", "--json")
	output, err = cmd.CombinedOutput()
	if err == nil {
		var result struct {
			Blockdevices []struct {
				Name string `json:"name"`
				Rota bool   `json:"rota"`
			} `json:"blockdevices"`
		}

		if err := json.Unmarshal(output, &result); err == nil {
			for _, device := range result.Blockdevices {
				if strings.HasPrefix(device.Name, "loop") {
					continue
				}

				if device.Rota {
					return "hdd", nil
				} else {
					return "ssd", nil
				}
			}
		}
	}

	// Check for nvme module loaded
	cmd = exec.Command("lsmod")
	output, err = cmd.CombinedOutput()
	if err == nil && strings.Contains(string(output), "nvme") {
		return "nvme", nil
	}

	// Fall back to checking if rotational or not for the root filesystem
	cmd = exec.Command("findmnt", "-n", "-o", "SOURCE", "/")
	output, err = cmd.CombinedOutput()
	if err == nil {
		source := strings.TrimSpace(string(output))
		if strings.HasPrefix(source, "/dev/") {
			device := strings.TrimPrefix(source, "/dev/")
			device = strings.TrimRight(device, "0123456789") // Remove partition number

			// Check if it's rotational
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

	// Fallback to using df to estimate storage performance
	cmd = exec.Command("df", "-T", "/")
	output, err = cmd.CombinedOutput()
	if err == nil {
		outputStr := string(output)
		if strings.Contains(outputStr, "ext4") || strings.Contains(outputStr, "xfs") {
			// Most modern filesystems are on SSD now
			return "ssd", nil
		}
	}

	// Unknown type
	return "", fmt.Errorf("could not determine storage type")
}

// detectGPUCapabilities checks for GPU availability for EO workloads
func (c *DataLocalityCollector) detectGPUCapabilities() (bool, map[string]string) {
	labels := make(map[string]string)

	// Check for NVIDIA GPUs
	cmd := exec.Command("nvidia-smi", "--query-gpu=name", "--format=csv,noheader")
	output, err := cmd.CombinedOutput()
	if err == nil && len(output) > 0 {
		gpuName := strings.TrimSpace(string(output))
		labels["node-capability/gpu-nvidia"] = "true"
		labels["node-capability/gpu-model"] = sanitizeValue(gpuName)
		labels["node-capability/gpu-accelerated"] = "true"

		// Check for CUDA
		cmd = exec.Command("nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader")
		output, err = cmd.CombinedOutput()
		if err == nil && len(output) > 0 {
			driver := strings.TrimSpace(string(output))
			labels["node-capability/nvidia-driver"] = sanitizeValue(driver)
		}

		// Special case for EO processing
		labels["node-capability/eo-processing"] = "true"
		return true, labels
	}

	// Check if GPU is listed in PCI devices
	cmd = exec.Command("lspci")
	output, err = cmd.CombinedOutput()
	if err == nil {
		outputStr := strings.ToLower(string(output))

		if strings.Contains(outputStr, "nvidia") {
			labels["node-capability/gpu-nvidia"] = "true"
			labels["node-capability/gpu-accelerated"] = "true"
			labels["node-capability/eo-processing"] = "true"
			return true, labels
		} else if strings.Contains(outputStr, "amd") &&
			(strings.Contains(outputStr, "graphics") ||
				strings.Contains(outputStr, "display")) {
			labels["node-capability/gpu-amd"] = "true"
			labels["node-capability/gpu-accelerated"] = "true"
			labels["node-capability/eo-processing"] = "true"
			return true, labels
		} else if strings.Contains(outputStr, "intel") &&
			(strings.Contains(outputStr, "graphics") ||
				strings.Contains(outputStr, "display")) {
			labels["node-capability/gpu-intel"] = "true"
			labels["node-capability/eo-processing"] = "medium"
			return true, labels
		}
	}

	return false, labels
}

// isNodeReady checks if a node is in Ready condition
func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// Utility function to filter out nodes that are already in the list
func filterOutNodes(allNodes []v1.Node, existingNodes []v1.Node) []v1.Node {
	var result []v1.Node

	for _, node := range allNodes {
		if !containsNode(existingNodes, node.Name) {
			result = append(result, node)
		}
	}

	return result
}

// containsNode checks if a node name exists in a node list
func containsNode(nodes []v1.Node, name string) bool {
	for _, node := range nodes {
		if node.Name == name {
			return true
		}
	}
	return false
}

// containsString checks if a string exists in a slice
func containsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}

// sanitizeValue sanitizes a label value according to Kubernetes requirements
func sanitizeValue(value string) string {
	// Replace invalid characters with hyphens
	value = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '.' || r == '_' {
			return r
		}
		return '-'
	}, value)

	// Trim length to 63 characters as required by Kubernetes
	if len(value) > 63 {
		value = value[:63]
	}

	// Fix invalid starting/ending characters
	if len(value) > 0 {
		if !((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= '0' && value[0] <= '9')) {
			value = "x" + value[1:]
		}

		if len(value) > 1 && !((value[len(value)-1] >= 'a' && value[len(value)-1] <= 'z') ||
			(value[len(value)-1] >= 'A' && value[len(value)-1] <= 'Z') ||
			(value[len(value)-1] >= '0' && value[len(value)-1] <= '9')) {
			value = value[:len(value)-1] + "x"
		}
	}

	return value
}
