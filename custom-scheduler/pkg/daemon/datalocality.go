package daemon

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const maxNodesToMeasure = 10

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

	BandwidthMeasurementInterval = 6 * time.Hour

	// Edge-cloud labels
	EdgeNodeLabel  = "node-capability/node-type"
	EdgeNodeValue  = "edge"
	CloudNodeValue = "cloud"

	// Topology labels
	RegionLabel = "topology.kubernetes.io/region"
	ZoneLabel   = "topology.kubernetes.io/zone"
)

// DataLocalityCollector collects info about storage and networking
type DataLocalityCollector struct {
	nodeName              string
	clientset             kubernetes.Interface
	bandwidthCache        map[string]int64
	bandwidthLatencyCache map[string]float64
	bandwidthCacheMutex   sync.RWMutex
	lastBandwidthUpdate   time.Time
	storageCache          map[string]interface{}
	storageCacheMutex     sync.RWMutex
	nodeType              string // edge/cloud
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

	// update network measurements periodically
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
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", c.nodeName)

	pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
	})

	if err != nil {
		return false, nil, 0, fmt.Errorf("failed to list pods: %w", err)
	}

	// Look for MinIO pods
	var minioPods []v1.Pod
	for _, pod := range pods.Items {
		if strings.Contains(strings.ToLower(pod.Name), "minio") ||
			(pod.Labels != nil && pod.Labels["app"] == "minio") {
			minioPods = append(minioPods, pod)
		}
	}

	if len(minioPods) == 0 {
		return false, nil, 0, nil
	}

	buckets := []string{"eo-scenes", "cog-data", "fmask-results", "data", "models"}

	var storageCapacity int64
	for _, pod := range minioPods {
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
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == "kubernetes.io/hostname" && expr.Operator == "In" {
					for _, value := range expr.Values {
						if value == nodeName {
							return true
						}
					}
				}
			}
		}
	}

	if pv.Spec.Local != nil {
		return true
	}

	if boundNode, exists := pv.Annotations["volume.kubernetes.io/selected-node"]; exists {
		return boundNode == nodeName
	}

	return false
}

// detectStorageTechnology determines the type of storage (SSD, HDD, etc.)
func (c *DataLocalityCollector) detectStorageTechnology() (string, error) {
	cmd := exec.Command("ls", "/dev/nvme*")
	output, err := cmd.CombinedOutput()
	if err == nil && len(output) > 0 {
		return "nvme", nil
	}

	// rotational status of primary disk
	cmd = exec.Command("lsblk", "-d", "-n", "-o", "NAME,ROTA")
	output, err = cmd.CombinedOutput()
	if err == nil {
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 2 && !strings.HasPrefix(fields[0], "loop") {
				// ROTA=0 = non-rotational (SSD), ROTA=1 = rotational (HDD)
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

			device := strings.TrimPrefix(source, "/dev/")
			device = strings.Split(device, "p")[0] // removing partition number
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

	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list nodes for bandwidth measurement: %v", err)
		return
	}

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
			continue
		}

		if !isNodeReady(&node) {
			continue
		}

		if c.zone != "" && node.Labels[ZoneLabel] == c.zone {
			sameZoneNodes = append(sameZoneNodes, node)
		} else if c.region != "" && node.Labels[RegionLabel] == c.region {
			sameRegionNodes = append(sameRegionNodes, node)
		} else {
			otherNodes = append(otherNodes, node)
		}
	}

	// measurement order: same zone -> same region -> others
	nodesToMeasure := append(sameZoneNodes, sameRegionNodes...)
	nodesToMeasure = append(nodesToMeasure, otherNodes...)

	maxNodesToMeasure := maxNodesToMeasure
	if len(nodesToMeasure) > maxNodesToMeasure {
		nodesToMeasure = nodesToMeasure[:maxNodesToMeasure]
	}

	measuredCount := 0
	for _, node := range nodesToMeasure {
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

		bandwidth, latency := c.mockBandwidthMeasurement(node)

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

// mockBandwidthMeasurement creates realistic bandwidth/latency measurements based on topology
func (c *DataLocalityCollector) mockBandwidthMeasurement(node v1.Node) (int64, float64) {
	targetType := CloudNodeValue
	if value, exists := node.Labels[EdgeNodeLabel]; exists && value == EdgeNodeValue {
		targetType = EdgeNodeValue
	} else if strings.Contains(strings.ToLower(node.Name), "edge") {
		targetType = EdgeNodeValue
	}

	targetRegion := node.Labels[RegionLabel]
	targetZone := node.Labels[ZoneLabel]

	var bandwidth int64
	var latency float64

	jitter := 0.85 + (rand.Float64() * 0.3)

	// Estimations based on topology relationship
	if c.zone != "" && c.zone == targetZone {
		// Same zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(500000000) * jitter) // 500 MB/s edge-edge
			latency = 1.0 / jitter                         // 1ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(1000000000) * jitter) // 1 GB/s cloud-cloud
			latency = 0.5 / jitter                          // 0.5ms
		} else {
			bandwidth = int64(float64(750000000) * jitter) // 750 MB/s edge-cloud
			latency = 1.0 / jitter                         // 1ms
		}
	} else if c.region != "" && c.region == targetRegion {
		// Same region, different zone
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(200000000) * jitter) // 200 MB/s edge-edge
			latency = 5.0 / jitter                         // 5ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(500000000) * jitter) // 500 MB/s  cloud-cloud
			latency = 2.0 / jitter                         // 2ms
		} else {
			bandwidth = int64(float64(100000000) * jitter) // 100 MB/s edge-cloud
			latency = 10.0 / jitter                        // 10ms
		}
	} else {
		// Different regions
		if c.nodeType == EdgeNodeValue && targetType == EdgeNodeValue {
			bandwidth = int64(float64(50000000) * jitter) // 50 MB/s edge-edge
			latency = 50.0 / jitter                       // 50ms
		} else if c.nodeType == CloudNodeValue && targetType == CloudNodeValue {
			bandwidth = int64(float64(100000000) * jitter) // 100 MB/s cloud-cloud
			latency = 20.0 / jitter                        // 20ms
		} else {
			bandwidth = int64(float64(20000000) * jitter) // 20 MB/s  edge-cloud
			latency = 100.0 / jitter                      // 100ms
		}
	}

	return bandwidth, latency
}

func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
