package storage

import (
	"strings"
	"sync"
)

// StorageIndex tracks data storage across the cluster
type StorageIndex struct {
	dataItems    map[string]*DataItem
	bucketNodes  map[string][]string
	storageNodes map[string]*StorageNode
	mu           sync.RWMutex
}

// NewStorageIndex creates a new storage index
func NewStorageIndex() *StorageIndex {
	return &StorageIndex{
		dataItems:    make(map[string]*DataItem),
		bucketNodes:  make(map[string][]string),
		storageNodes: make(map[string]*StorageNode),
	}
}

// RegisterStorageNode adds or updates a storage node
func (si *StorageIndex) RegisterStorageNode(node *StorageNode) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.storageNodes[node.Name] = node
}

// RegisterBucket associates a bucket with nodes
func (si *StorageIndex) RegisterBucket(bucket string, nodes []string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.bucketNodes[bucket] = nodes
}

// AddDataItem adds or updates a data item
func (si *StorageIndex) AddDataItem(item *DataItem) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.dataItems[item.URN] = item
}

// GetAllStorageNodes returns all storage nodes
func (si *StorageIndex) GetAllStorageNodes() []*StorageNode {
	si.mu.RLock()
	defer si.mu.RUnlock()

	nodes := make([]*StorageNode, 0, len(si.storageNodes))
	for _, node := range si.storageNodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetAllBuckets returns all bucket names
func (si *StorageIndex) GetAllBuckets() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	buckets := make([]string, 0, len(si.bucketNodes))
	for bucket := range si.bucketNodes {
		buckets = append(buckets, bucket)
	}
	return buckets
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
	return si.bucketNodes[bucket]
}

// GetStorageNodesForData returns storage nodes that contain the specified data
func (si *StorageIndex) GetStorageNodesForData(urn string) []string {
	si.mu.RLock()
	defer si.mu.RUnlock()

	// assumed format like "bucket/path/to/data"
	parts := strings.SplitN(urn, "/", 2)
	if len(parts) < 1 {
		return nil
	}

	bucket := parts[0]
	nodes := si.bucketNodes[bucket]

	if item, exists := si.dataItems[urn]; exists {
		return item.Locations
	}

	return nodes
}

// GetStorageNode returns information about a storage node
func (si *StorageIndex) GetStorageNode(nodeName string) (*StorageNode, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	node, exists := si.storageNodes[nodeName]
	return node, exists
}
