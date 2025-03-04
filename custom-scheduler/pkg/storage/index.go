package storage

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"slices"

	"k8s.io/klog/v2"
)

// StorageIndex tracks data storage across the cluster
type StorageIndex struct {
	dataItems     map[string]*DataItem
	bucketNodes   map[string][]string
	storageNodes  map[string]*StorageNode
	regionNodes   map[string][]string
	zoneNodes     map[string][]string
	mu            sync.RWMutex
	lastRefreshed time.Time
}

// NewStorageIndex creates a new storage index
func NewStorageIndex() *StorageIndex {
	return &StorageIndex{
		dataItems:     make(map[string]*DataItem),
		bucketNodes:   make(map[string][]string),
		storageNodes:  make(map[string]*StorageNode),
		regionNodes:   make(map[string][]string),
		zoneNodes:     make(map[string][]string),
		lastRefreshed: time.Now(),
	}
}

// RegisterStorageNode adds or updates a storage node
func (si *StorageIndex) RegisterStorageNode(node *StorageNode) {
	si.mu.Lock()
	defer si.mu.Unlock()

	node.LastUpdated = time.Now()
	oldNode, exists := si.storageNodes[node.Name]

	si.storageNodes[node.Name] = node

	if node.Region != "" {
		if exists && oldNode.Region != "" && oldNode.Region != node.Region {
			si.removeNodeFromRegion(node.Name, oldNode.Region)
		}

		si.addNodeToRegion(node.Name, node.Region)
	}

	if node.Zone != "" {
		if exists && oldNode.Zone != "" && oldNode.Zone != node.Zone {
			si.removeNodeFromZone(node.Name, oldNode.Zone)
		}

		si.addNodeToZone(node.Name, node.Zone)
	}

	for _, bucket := range node.Buckets {
		si.registerBucketForNode(bucket, node.Name)
	}

	klog.V(4).Infof("Registered storage node %s (%s) in %s/%s",
		node.Name, string(node.NodeType), node.Region, node.Zone)
}

// removeNodeFromRegion removes a node from a region mapping
func (si *StorageIndex) removeNodeFromRegion(nodeName, region string) {
	nodes := si.regionNodes[region]
	for i, name := range nodes {
		if name == nodeName {
			nodes[i] = nodes[len(nodes)-1]
			si.regionNodes[region] = nodes[:len(nodes)-1]
			return
		}
	}
}

// addNodeToRegion adds a node to a region mapping
func (si *StorageIndex) addNodeToRegion(nodeName, region string) {
	if slices.Contains(si.regionNodes[region], nodeName) {
		return
	}
	si.regionNodes[region] = append(si.regionNodes[region], nodeName)
}

// removeNodeFromZone removes a node from a zone mapping
func (si *StorageIndex) removeNodeFromZone(nodeName, zone string) {
	nodes := si.zoneNodes[zone]
	for i, name := range nodes {
		if name == nodeName {
			// Remove by swapping with last element and truncating
			nodes[i] = nodes[len(nodes)-1]
			si.zoneNodes[zone] = nodes[:len(nodes)-1]
			return
		}
	}
}

// addNodeToZone adds a node to a zone mapping
func (si *StorageIndex) addNodeToZone(nodeName, zone string) {
	if slices.Contains(si.zoneNodes[zone], nodeName) {
		return
	}
	si.zoneNodes[zone] = append(si.zoneNodes[zone], nodeName)
}

// registerBucketForNode associates a bucket with a node
func (si *StorageIndex) registerBucketForNode(bucket, nodeName string) {
	if slices.Contains(si.bucketNodes[bucket], nodeName) {
		return
	}
	si.bucketNodes[bucket] = append(si.bucketNodes[bucket], nodeName)
}

// RemoveStorageNode removes a storage node from the index
func (si *StorageIndex) RemoveStorageNode(nodeName string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	node, exists := si.storageNodes[nodeName]
	if !exists {
		return
	}

	if node.Region != "" {
		si.removeNodeFromRegion(nodeName, node.Region)
	}

	if node.Zone != "" {
		si.removeNodeFromZone(nodeName, node.Zone)
	}

	for bucket, nodes := range si.bucketNodes {
		newNodes := make([]string, 0, len(nodes))
		for _, name := range nodes {
			if name != nodeName {
				newNodes = append(newNodes, name)
			}
		}

		if len(newNodes) > 0 {
			si.bucketNodes[bucket] = newNodes
		} else {
			delete(si.bucketNodes, bucket)
		}
	}

	delete(si.storageNodes, nodeName)

	klog.V(4).Infof("Removed storage node %s from index", nodeName)
}

// RegisterBucket associates a bucket with nodes
func (si *StorageIndex) RegisterBucket(bucket string, nodes []string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	nodesCopy := make([]string, len(nodes))
	copy(nodesCopy, nodes)

	si.bucketNodes[bucket] = nodesCopy

	klog.V(4).Infof("Registered bucket %s on nodes %v", bucket, nodes)
}

// AddDataItem adds or updates a data item
func (si *StorageIndex) AddDataItem(item *DataItem) {
	si.mu.Lock()
	defer si.mu.Unlock()

	locationsCopy := make([]string, len(item.Locations))
	copy(locationsCopy, item.Locations)

	item.Locations = locationsCopy

	if existing, exists := si.dataItems[item.URN]; exists {
		existing.Size = item.Size
		existing.Locations = item.Locations
		existing.LastModified = item.LastModified
		existing.ContentType = item.ContentType

		if item.Metadata != nil {
			if existing.Metadata == nil {
				existing.Metadata = make(map[string]string)
			}
			for k, v := range item.Metadata {
				existing.Metadata[k] = v
			}
		}
	} else {
		si.dataItems[item.URN] = item
	}

	klog.V(4).Infof("Added/updated data item %s (size: %d bytes, type: %s) on nodes %v",
		item.URN, item.Size, item.ContentType, item.Locations)
}

// GetDataItem retrieves a data item by URN
func (si *StorageIndex) GetDataItem(urn string) (*DataItem, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	item, exists := si.dataItems[urn]
	return item, exists
}

// GetBucketNodes returns the nodes containing a bucket
func (si *StorageIndex) GetBucketNodes(bucket string) []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	nodes := si.bucketNodes[bucket]
	if len(nodes) == 0 {
		return nil
	}

	result := make([]string, len(nodes))
	copy(result, nodes)
	return result
}

// GetStorageNodesForData returns storage nodes that contain the specified data
func (si *StorageIndex) GetStorageNodesForData(urn string) []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if item, exists := si.dataItems[urn]; exists && len(item.Locations) > 0 {
		result := make([]string, len(item.Locations))
		copy(result, item.Locations)
		return result
	}

	// Otherwise infer from bucket information
	// Assuming URN format: bucket/path/to/data
	parts := strings.SplitN(urn, "/", 2)
	if len(parts) < 1 {
		return nil
	}

	bucket := parts[0]
	nodes := si.bucketNodes[bucket]

	if len(nodes) == 0 {
		return nil
	}

	result := make([]string, len(nodes))
	copy(result, nodes)
	return result
}

// GetStorageNode returns information about a storage node
func (si *StorageIndex) GetStorageNode(nodeName string) (*StorageNode, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	node, exists := si.storageNodes[nodeName]
	return node, exists
}

// GetNodesInRegion returns all storage nodes in a given region
func (si *StorageIndex) GetNodesInRegion(region string) []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	nodes := si.regionNodes[region]
	if len(nodes) == 0 {
		return nil
	}

	result := make([]string, len(nodes))
	copy(result, nodes)
	return result
}

// GetNodesInZone returns all storage nodes in a given zone
func (si *StorageIndex) GetNodesInZone(zone string) []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	nodes := si.zoneNodes[zone]
	if len(nodes) == 0 {
		return nil
	}

	result := make([]string, len(nodes))
	copy(result, nodes)
	return result
}

// GetAllStorageNodes returns a list of all registered storage nodes
func (si *StorageIndex) GetAllStorageNodes() []*StorageNode {
	si.mu.RLock()
	defer si.mu.RUnlock()

	nodes := make([]*StorageNode, 0, len(si.storageNodes))
	for _, node := range si.storageNodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetAllBuckets returns a list of all registered buckets
func (si *StorageIndex) GetAllBuckets() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	buckets := make([]string, 0, len(si.bucketNodes))
	for bucket := range si.bucketNodes {
		buckets = append(buckets, bucket)
	}

	return buckets
}

// GetNodeType returns the type of a node (edge or cloud)
func (si *StorageIndex) GetNodeType(nodeName string) StorageNodeType {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if node, exists := si.storageNodes[nodeName]; exists {
		return node.NodeType
	}

	return StorageTypeCloud // default to cloud if unknown
}

// MarkRefreshed updates the last refreshed timestamp
func (si *StorageIndex) MarkRefreshed() {
	si.mu.Lock()
	defer si.mu.Unlock()

	si.lastRefreshed = time.Now()
}

// GetLastRefreshed returns the time when the index was last refreshed
func (si *StorageIndex) GetLastRefreshed() time.Time {
	si.mu.RLock()
	defer si.mu.RUnlock()

	return si.lastRefreshed
}

// PruneStaleBuckets removes buckets without nodes
func (si *StorageIndex) PruneStaleBuckets() int {
	si.mu.Lock()
	defer si.mu.Unlock()

	count := 0
	for bucket, nodes := range si.bucketNodes {
		if len(nodes) == 0 {
			delete(si.bucketNodes, bucket)
			count++
		}
	}

	return count
}

// PruneStaleDataItems removes data items with no locations
func (si *StorageIndex) PruneStaleDataItems() int {
	si.mu.Lock()
	defer si.mu.Unlock()

	count := 0
	for urn, item := range si.dataItems {
		if len(item.Locations) == 0 {
			delete(si.dataItems, urn)
			count++
		}
	}

	return count
}

// PrintSummary returns a string representation of the storage index
func (si *StorageIndex) PrintSummary() string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	var result strings.Builder

	fmt.Fprintf(&result, "Storage Index Summary (last refreshed: %s)\n",
		si.lastRefreshed.Format(time.RFC3339))
	fmt.Fprintf(&result, "- Storage Nodes: %d\n", len(si.storageNodes))
	fmt.Fprintf(&result, "- Buckets: %d\n", len(si.bucketNodes))
	fmt.Fprintf(&result, "- Data Items: %d\n", len(si.dataItems))
	fmt.Fprintf(&result, "- Regions: %d\n", len(si.regionNodes))
	fmt.Fprintf(&result, "- Zones: %d\n", len(si.zoneNodes))

	edgeCount := 0
	cloudCount := 0
	for _, node := range si.storageNodes {
		if node.NodeType == StorageTypeEdge {
			edgeCount++
		} else {
			cloudCount++
		}
	}

	fmt.Fprintf(&result, "- Edge Nodes: %d\n", edgeCount)
	fmt.Fprintf(&result, "- Cloud Nodes: %d\n", cloudCount)

	if len(si.storageNodes) > 0 {
		fmt.Fprintf(&result, "\nStorage Nodes:\n")
		for name, node := range si.storageNodes {
			fmt.Fprintf(&result, "- %s: %s (%s), %s/%s, %d/%d bytes, buckets: %v\n",
				name,
				string(node.NodeType),
				node.StorageTechnology,
				node.Region,
				node.Zone,
				node.AvailableBytes,
				node.CapacityBytes,
				strings.Join(node.Buckets, ", "))
		}
	}

	if len(si.bucketNodes) > 0 {
		fmt.Fprintf(&result, "\nBuckets:\n")
		for bucket, nodes := range si.bucketNodes {
			fmt.Fprintf(&result, "- %s: %v\n", bucket, nodes)
		}
	}

	return result.String()
}
