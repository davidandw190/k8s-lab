package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/storage"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// Labels used by the scheduler
	StorageNodeLabel = "node-capability/storage-service"
	EdgeNodeLabel    = "node-capability/node-type"
	EdgeNodeValue    = "edge"
	CloudNodeValue   = "cloud"
	RegionLabel      = "topology.kubernetes.io/region"
	ZoneLabel        = "topology.kubernetes.io/zone"

	// Default settings
	schedulerInterval      = 1 * time.Second
	storageRefreshInterval = 5 * time.Minute
)

// NodeScore represents a node's score for scheduling
type NodeScore struct {
	Name  string
	Score int
}

// Scheduler implements a data-locality aware Kubernetes scheduler
type Scheduler struct {
	clientset      kubernetes.Interface
	schedulerName  string
	podQueue       chan *v1.Pod
	storageIndex   *storage.StorageIndex
	bandwidthGraph *storage.BandwidthGraph
	storageMutex   sync.RWMutex

	// Priority functions
	priorityFuncs []PriorityFunc

	// Data locality priority
	dataLocalityPriority *DataLocalityPriority
}

// PriorityFunc is a function that scores nodes for scheduling
type PriorityFunc func(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error)

// NewScheduler creates a new custom scheduler
func NewScheduler(clientset kubernetes.Interface, schedulerName string) *Scheduler {
	return &Scheduler{
		clientset:      clientset,
		schedulerName:  schedulerName,
		podQueue:       make(chan *v1.Pod, 100),
		storageIndex:   storage.NewStorageIndex(),
		bandwidthGraph: storage.NewBandwidthGraph(50 * 1024 * 1024), // 50 MB/s default
		priorityFuncs:  make([]PriorityFunc, 0),
	}
}

// SetStorageIndex sets the storage index
func (s *Scheduler) SetStorageIndex(idx *storage.StorageIndex) {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	s.storageIndex = idx
}

// SetBandwidthGraph sets the bandwidth graph
func (s *Scheduler) SetBandwidthGraph(graph *storage.BandwidthGraph) {
	s.storageMutex.Lock()
	defer s.storageMutex.Unlock()

	s.bandwidthGraph = graph
}

// Run starts the scheduler
func (s *Scheduler) Run(ctx context.Context) error {
	// Initialize default priority functions
	s.initPriorityFunctions()

	// Initialize data locality priority
	s.dataLocalityPriority = NewDataLocalityPriority(s.storageIndex, s.bandwidthGraph)

	// Start pod informer to watch for pods to schedule
	podInformer := s.createPodInformer(ctx)
	go podInformer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for pod informer caches to sync")
	}

	// Initialize storage information
	if err := s.initStorageInformation(ctx); err != nil {
		klog.Warningf("Failed to initialize storage information: %v", err)
		klog.Warningf("Continuing with limited storage awareness")
	}

	// Create mock data for testing
	s.storageIndex.MockMinioData()
	s.bandwidthGraph.MockNetworkPaths()

	// Start periodic refresh of storage data
	go s.refreshStorageDataPeriodically(ctx)

	// Start health check server
	go s.startHealthCheckServer(ctx)

	// Start scheduler loop
	go wait.UntilWithContext(ctx, s.scheduleOne, schedulerInterval)

	// Wait for context cancellation
	klog.Infof("Scheduler %s started successfully", s.schedulerName)
	<-ctx.Done()
	klog.Info("Scheduler shutting down")
	return nil
}

// initPriorityFunctions initializes the default priority functions
func (s *Scheduler) initPriorityFunctions() {
	// Add resource-based priority function
	s.priorityFuncs = append(s.priorityFuncs, s.scoreResourcePriority)

	// Add node affinity priority function
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeAffinity)

	// Add node type priority function (edge vs cloud)
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeType)

	// Add capability-based priority function
	s.priorityFuncs = append(s.priorityFuncs, s.scoreNodeCapabilities)
}

// initStorageInformation initializes storage information from node labels
func (s *Scheduler) initStorageInformation(ctx context.Context) error {
	klog.Info("Initializing storage information")

	// List all nodes
	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Process each node to find storage capabilities
	var storageNodes []*storage.StorageNode
	var edgeNodes []string
	var cloudNodes []string
	var regions []string
	var zones []string

	for _, node := range nodes.Items {
		// Extract topology information
		region := node.Labels[RegionLabel]
		zone := node.Labels[ZoneLabel]

		// Determine node type
		nodeType := storage.StorageTypeCloud // Default to cloud
		if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
			nodeType = storage.StorageTypeEdge
			edgeNodes = append(edgeNodes, node.Name)
		} else {
			cloudNodes = append(cloudNodes, node.Name)
		}

		// Track regions and zones
		if region != "" && !containsString(regions, region) {
			regions = append(regions, region)
		}

		if zone != "" && !containsString(zones, zone) {
			zones = append(zones, zone)
		}

		// Set node topology in bandwidth graph
		s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

		// Check if this is a storage node
		if value, ok := node.Labels[StorageNodeLabel]; ok && value == "true" {
			storageType := "generic"
			if t, ok := node.Labels["node-capability/storage-type"]; ok {
				storageType = t
			}

			// Get storage capacity
			var capacity int64
			if capStr, ok := node.Labels["node-capability/storage-capacity-bytes"]; ok {
				capacity, _ = strconv.ParseInt(capStr, 10, 64)
			}

			if capacity == 0 {
				capacity = node.Status.Capacity.Storage().Value()
			}

			// Get storage technology
			storageTech := "unknown"
			if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
				storageTech = tech
			}

			// Extract bucket information
			var buckets []string
			for label := range node.Labels {
				if strings.HasPrefix(label, "node-capability/storage-bucket-") {
					bucket := strings.TrimPrefix(label, "node-capability/storage-bucket-")
					buckets = append(buckets, bucket)
				}
			}

			// Create storage node entry
			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceType(storageType),
				Region:            region,
				Zone:              zone,
				CapacityBytes:     capacity,
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: storageTech,
				LastUpdated:       time.Now(),
				Buckets:           buckets,
				TopologyLabels:    make(map[string]string),
			}

			// Track capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			storageNodes = append(storageNodes, storageNode)
		} else {
			// Register non-storage nodes in storage index for topology tracking
			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceGeneric,
				Region:            region,
				Zone:              zone,
				CapacityBytes:     node.Status.Capacity.Storage().Value(),
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: "unknown",
				LastUpdated:       time.Now(),
				Buckets:           []string{},
				TopologyLabels:    make(map[string]string),
			}

			// Track capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			storageNodes = append(storageNodes, storageNode)
		}
	}

	// Register storage nodes in the index
	s.storageMutex.Lock()
	for _, node := range storageNodes {
		s.storageIndex.RegisterStorageNode(node)

		// Register buckets
		for _, bucket := range node.Buckets {
			bucketNodes := s.storageIndex.GetBucketNodes(bucket)
			if bucketNodes == nil {
				bucketNodes = []string{node.Name}
			} else if !containsString(bucketNodes, node.Name) {
				bucketNodes = append(bucketNodes, node.Name)
			}
			s.storageIndex.RegisterBucket(bucket, bucketNodes)
		}
	}
	s.storageMutex.Unlock()

	// Initialize bandwidth information
	s.initBandwidthInformation(nodes.Items)

	klog.Infof("Storage initialization complete: %d storage nodes, %d edge nodes, %d cloud nodes, %d regions, %d zones",
		len(storageNodes), len(edgeNodes), len(cloudNodes), len(regions), len(zones))

	return nil
}

// initBandwidthInformation initializes the bandwidth graph from node labels
func (s *Scheduler) initBandwidthInformation(nodes []v1.Node) {
	// Set topology-based defaults for the bandwidth graph
	s.bandwidthGraph.SetTopologyDefaults(
		1e9, 0.1, // Local bandwidth: 1 GB/s, 0.1ms latency
		500e6, 1.0, // Same zone: 500 MB/s, 1ms latency
		200e6, 5.0, // Same region: 200 MB/s, 5ms latency
		50e6, 20.0, // Edge-cloud: 50 MB/s, 20ms latency
	)

	// Extract bandwidth information from node labels
	for _, source := range nodes {
		for _, dest := range nodes {
			if source.Name == dest.Name {
				continue // Skip self
			}

			// Check for bandwidth label
			bandwidthLabel := fmt.Sprintf("node-capability/bandwidth-to-%s", dest.Name)
			if bandwidthStr, ok := source.Labels[bandwidthLabel]; ok {
				if bandwidth, err := strconv.ParseInt(bandwidthStr, 10, 64); err == nil && bandwidth > 0 {
					// Check for latency label
					latencyLabel := fmt.Sprintf("node-capability/latency-to-%s", dest.Name)
					latency := 5.0 // Default 5ms
					if latencyStr, ok := source.Labels[latencyLabel]; ok {
						if l, err := strconv.ParseFloat(latencyStr, 64); err == nil && l > 0 {
							latency = l
						}
					}

					s.bandwidthGraph.SetBandwidth(source.Name, dest.Name, float64(bandwidth), latency)
				}
			}
		}
	}
}

// refreshStorageDataPeriodically refreshes storage information regularly
func (s *Scheduler) refreshStorageDataPeriodically(ctx context.Context) {
	ticker := time.NewTicker(storageRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.refreshStorageInformation(ctx); err != nil {
				klog.Warningf("Failed to refresh storage information: %v", err)
			}
		case <-ctx.Done():
			klog.Info("Stopping storage refresh loop")
			return
		}
	}
}

// refreshStorageInformation updates storage and bandwidth information
func (s *Scheduler) refreshStorageInformation(ctx context.Context) error {
	klog.V(4).Info("Refreshing storage information")

	// Get all nodes
	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Track existing storage nodes to detect deleted ones
	existingStorageNodes := make(map[string]bool)

	// Process each node
	s.storageMutex.Lock()
	for _, node := range nodes.Items {
		// Check if this is a storage node
		if value, ok := node.Labels[StorageNodeLabel]; ok && value == "true" {
			existingStorageNodes[node.Name] = true

			// Extract node type
			nodeType := storage.StorageTypeCloud
			if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
				nodeType = storage.StorageTypeEdge
			}

			// Get storage type
			storageType := "generic"
			if t, ok := node.Labels["node-capability/storage-type"]; ok {
				storageType = t
			}

			// Get region and zone
			region := node.Labels[RegionLabel]
			zone := node.Labels[ZoneLabel]

			// Update topology in bandwidth graph
			s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

			// Get storage capacity
			var capacity int64
			if capStr, ok := node.Labels["node-capability/storage-capacity-bytes"]; ok {
				capacity, _ = strconv.ParseInt(capStr, 10, 64)
			}

			if capacity == 0 {
				capacity = node.Status.Capacity.Storage().Value()
			}

			// Get storage technology
			storageTech := "unknown"
			if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
				storageTech = tech
			}

			// Extract bucket information
			var buckets []string
			for label, value := range node.Labels {
				if strings.HasPrefix(label, "node-capability/storage-bucket-") && value == "true" {
					bucket := strings.TrimPrefix(label, "node-capability/storage-bucket-")
					buckets = append(buckets, bucket)
				}
			}

			// Create or update storage node
			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceType(storageType),
				Region:            region,
				Zone:              zone,
				CapacityBytes:     capacity,
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: storageTech,
				LastUpdated:       time.Now(),
				Buckets:           buckets,
				TopologyLabels:    make(map[string]string),
			}

			// Copy capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			s.storageIndex.RegisterStorageNode(storageNode)

			// Register buckets
			for _, bucket := range buckets {
				bucketNodes := s.storageIndex.GetBucketNodes(bucket)
				if bucketNodes == nil {
					bucketNodes = []string{node.Name}
				} else if !containsString(bucketNodes, node.Name) {
					bucketNodes = append(bucketNodes, node.Name)
				}
				s.storageIndex.RegisterBucket(bucket, bucketNodes)
			}
		} else {
			// Update topology for non-storage nodes
			nodeType := storage.StorageTypeCloud
			if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
				nodeType = storage.StorageTypeEdge
			}

			region := node.Labels[RegionLabel]
			zone := node.Labels[ZoneLabel]

			s.bandwidthGraph.SetNodeTopology(node.Name, region, zone, nodeType)

			// Update storage node entry for topology tracking
			storageNode := &storage.StorageNode{
				Name:              node.Name,
				NodeType:          nodeType,
				ServiceType:       storage.StorageServiceGeneric,
				Region:            region,
				Zone:              zone,
				CapacityBytes:     node.Status.Capacity.Storage().Value(),
				AvailableBytes:    node.Status.Allocatable.Storage().Value(),
				StorageTechnology: "unknown",
				LastUpdated:       time.Now(),
				Buckets:           []string{},
				TopologyLabels:    make(map[string]string),
			}

			// Copy capability labels
			for k, v := range node.Labels {
				if strings.HasPrefix(k, "node-capability/") {
					storageNode.TopologyLabels[k] = v
				}
			}

			s.storageIndex.RegisterStorageNode(storageNode)
		}

		// Update bandwidth information
		for _, dest := range nodes.Items {
			if node.Name == dest.Name {
				continue // Skip self
			}

			bandwidthLabel := fmt.Sprintf("node-capability/bandwidth-to-%s", dest.Name)
			if bandwidthStr, ok := node.Labels[bandwidthLabel]; ok {
				if bandwidth, err := strconv.ParseInt(bandwidthStr, 10, 64); err == nil && bandwidth > 0 {
					latencyLabel := fmt.Sprintf("node-capability/latency-to-%s", dest.Name)
					latency := 5.0 // Default 5ms
					if latencyStr, ok := node.Labels[latencyLabel]; ok {
						if l, err := strconv.ParseFloat(latencyStr, 64); err == nil && l > 0 {
							latency = l
						}
					}

					s.bandwidthGraph.SetBandwidth(node.Name, dest.Name, float64(bandwidth), latency)
				}
			}
		}
	}

	// Remove nodes that no longer exist or are no longer storage nodes
	currentStorageNodes := s.storageIndex.GetAllStorageNodes()
	for _, node := range currentStorageNodes {
		if node.ServiceType == storage.StorageServiceMinio && !existingStorageNodes[node.Name] {
			s.storageIndex.RemoveStorageNode(node.Name)
			s.bandwidthGraph.RemoveNode(node.Name)
		}
	}

	// Prune stale data
	s.storageIndex.PruneStaleBuckets()
	s.storageIndex.PruneStaleDataItems()
	s.storageIndex.MarkRefreshed()
	s.storageMutex.Unlock()

	klog.V(4).Info("Storage refresh complete")
	return nil
}

// createPodInformer creates a pod informer to watch for unscheduled pods
func (s *Scheduler) createPodInformer(ctx context.Context) cache.Controller {
	// Watch pods that:
	// 1. Use our scheduler name
	// 2. Don't have a node assigned yet
	fieldSelector := fields.SelectorFromSet(fields.Set{
		"spec.schedulerName": s.schedulerName,
		"spec.nodeName":      "",
	})

	podListWatcher := cache.NewListWatchFromClient(
		s.clientset.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		fieldSelector,
	)

	_, informer := cache.NewIndexerInformer(
		podListWatcher,
		&v1.Pod{},
		0, // resync disabled
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod, ok := obj.(*v1.Pod)
				if !ok {
					klog.Errorf("Failed to convert object to Pod")
					return
				}
				s.enqueuePod(pod)
			},
			UpdateFunc: func(_, newObj interface{}) {
				pod, ok := newObj.(*v1.Pod)
				if !ok {
					klog.Errorf("Failed to convert object to Pod")
					return
				}
				s.enqueuePod(pod)
			},
		},
		cache.Indexers{},
	)

	return informer
}

// enqueuePod adds a pod to the scheduling queue
func (s *Scheduler) enqueuePod(pod *v1.Pod) {
	// Skip pods that already have a node or use a different scheduler
	if pod.Spec.NodeName != "" || pod.Spec.SchedulerName != s.schedulerName {
		return
	}

	// Skip pods that are being deleted
	if pod.DeletionTimestamp != nil {
		return
	}

	// Add to queue
	s.podQueue <- pod
}

// scheduleOne schedules a single pod
func (s *Scheduler) scheduleOne(ctx context.Context) {
	// Get a pod from the queue or return if none available
	var pod *v1.Pod
	select {
	case pod = <-s.podQueue:
	case <-ctx.Done():
		return
	default:
		return // No pods to schedule
	}

	klog.Infof("Attempting to schedule pod: %s/%s", pod.Namespace, pod.Name)

	// Find the best node for this pod
	nodeName, err := s.findBestNodeForPod(ctx, pod)
	if err != nil {
		klog.Errorf("Failed to find suitable node for pod %s/%s: %v",
			pod.Namespace, pod.Name, err)
		return
	}

	// Bind the pod to the selected node
	err = s.bindPod(ctx, pod, nodeName)
	if err != nil {
		klog.Errorf("Failed to bind pod %s/%s to node %s: %v",
			pod.Namespace, pod.Name, nodeName, err)
		return
	}

	klog.Infof("Successfully scheduled pod %s/%s to node %s",
		pod.Namespace, pod.Name, nodeName)
}

// findBestNodeForPod finds the best node for a pod
func (s *Scheduler) findBestNodeForPod(ctx context.Context, pod *v1.Pod) (string, error) {
	// Get all nodes
	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no nodes available in the cluster")
	}

	// Filter nodes based on predicates
	filteredNodes, err := s.filterNodes(ctx, pod, nodes.Items)
	if err != nil {
		return "", fmt.Errorf("node filtering error: %w", err)
	}

	if len(filteredNodes) == 0 {
		return "", fmt.Errorf("no suitable nodes found after filtering")
	}

	// Score nodes using priority functions
	nodeScores, err := s.prioritizeNodes(pod, filteredNodes)
	if err != nil {
		return "", fmt.Errorf("node prioritization error: %w", err)
	}

	if len(nodeScores) == 0 {
		return "", fmt.Errorf("no suitable nodes found after scoring")
	}

	// Sort nodes by score (highest first)
	sort.Slice(nodeScores, func(i, j int) bool {
		return nodeScores[i].Score > nodeScores[j].Score
	})

	// Return the highest scoring node
	return nodeScores[0].Name, nil
}

// filterNodes applies predicates to filter out unsuitable nodes
func (s *Scheduler) filterNodes(ctx context.Context, pod *v1.Pod, nodes []v1.Node) ([]v1.Node, error) {
	var filteredNodes []v1.Node

	for _, node := range nodes {
		// Skip nodes that are not ready
		if !isNodeReady(&node) {
			klog.V(4).Infof("Node %s is not ready", node.Name)
			continue
		}

		// Check resource requirements
		if !s.nodeFitsResources(ctx, pod, &node) {
			klog.V(4).Infof("Node %s does not have sufficient resources", node.Name)
			continue
		}

		// Check node capability requirements
		if !s.nodeHasRequiredCapabilities(pod, &node) {
			klog.V(4).Infof("Node %s does not have required capabilities", node.Name)
			continue
		}

		// Check node affinity
		if !s.satisfiesNodeAffinity(pod, &node) {
			klog.V(4).Infof("Node %s does not satisfy node affinity", node.Name)
			continue
		}

		// Check node taints
		if !s.toleratesNodeTaints(pod, &node) {
			klog.V(4).Infof("Node %s has taints that pod does not tolerate", node.Name)
			continue
		}

		filteredNodes = append(filteredNodes, node)
	}

	return filteredNodes, nil
}

// prioritizeNodes scores nodes based on multiple priority functions
func (s *Scheduler) prioritizeNodes(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var allScores [][]NodeScore

	// Apply all priority functions
	for _, priorityFunc := range s.priorityFuncs {
		scores, err := priorityFunc(pod, nodes)
		if err != nil {
			klog.Warningf("Error applying priority function: %v", err)
			continue
		}
		allScores = append(allScores, scores)
	}

	// Apply data locality priority if available
	s.storageMutex.RLock()
	if s.dataLocalityPriority != nil {
		var dataLocalityScores []NodeScore

		for _, node := range nodes {
			score, err := s.dataLocalityPriority.Score(pod, node.Name)
			if err != nil {
				klog.Warningf("Error calculating data locality score for pod %s/%s on node %s: %v",
					pod.Namespace, pod.Name, node.Name, err)
				score = 50 // Default score
			}

			dataLocalityScores = append(dataLocalityScores, NodeScore{
				Name:  node.Name,
				Score: score,
			})
		}

		allScores = append(allScores, dataLocalityScores)
	}
	s.storageMutex.RUnlock()

	// Combine all scores with appropriate weights
	return s.combineScores(pod, nodes, allScores), nil
}

// combineScores combines scores from different priority functions
func (s *Scheduler) combineScores(pod *v1.Pod, nodes []v1.Node, scoresList [][]NodeScore) []NodeScore {
	// Define weights for different priority functions
	weights := s.getWeightsForPod(pod)

	// Initialize combined scores
	finalScores := make(map[string]int)
	for _, node := range nodes {
		finalScores[node.Name] = 0
	}

	// Apply weights and combine scores
	for i, scores := range scoresList {
		weight := 1.0
		if i < len(weights) {
			weight = weights[i]
		}

		for _, score := range scores {
			finalScores[score.Name] += int(float64(score.Score) * weight)
		}
	}

	// Convert to slice
	var result []NodeScore
	for nodeName, score := range finalScores {
		result = append(result, NodeScore{
			Name:  nodeName,
			Score: score,
		})
	}

	// Normalize to 0-100 range
	return normalizeScores(result)
}

// getWeightsForPod returns priority weights based on pod characteristics
func (s *Scheduler) getWeightsForPod(pod *v1.Pod) []float64 {
	// Default weights - balanced for general workloads
	weights := []float64{
		0.3, // Resource priority
		0.2, // Node affinity
		0.1, // Node type (edge/cloud)
		0.1, // Node capabilities
		0.3, // Data locality
	}

	// Check for workload-specific annotations that affect weights
	if pod.Annotations != nil {
		// Data-intensive workloads prioritize data locality
		if _, ok := pod.Annotations["scheduler.thesis/data-intensive"]; ok {
			weights = []float64{
				0.2, // Resource priority
				0.1, // Node affinity
				0.1, // Node type
				0.1, // Node capabilities
				0.5, // Data locality (higher)
			}
		}

		// Compute-intensive workloads prioritize resources
		if _, ok := pod.Annotations["scheduler.thesis/compute-intensive"]; ok {
			weights = []float64{
				0.5, // Resource priority (higher)
				0.2, // Node affinity
				0.1, // Node type
				0.1, // Node capabilities
				0.1, // Data locality (lower)
			}
		}

		// Edge-preferring workloads
		if _, ok := pod.Annotations["scheduler.thesis/prefer-edge"]; ok {
			weights[2] = 0.3 // Increase edge/cloud type weight
		}
	}

	return weights
}

// normalizeScores normalizes scores to a 0-100 range
func normalizeScores(scores []NodeScore) []NodeScore {
	// Find max score
	maxScore := 0
	for _, score := range scores {
		if score.Score > maxScore {
			maxScore = score.Score
		}
	}

	// Normalize scores
	normalizedScores := make([]NodeScore, len(scores))
	for i, score := range scores {
		normalizedScore := 0
		if maxScore > 0 {
			normalizedScore = score.Score * 100 / maxScore
		}
		normalizedScores[i] = NodeScore{
			Name:  score.Name,
			Score: normalizedScore,
		}
	}

	return normalizedScores
}

// scoreResourcePriority scores nodes based on resource availability
func (s *Scheduler) scoreResourcePriority(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	for _, node := range nodes {
		score := s.calculateResourceScore(pod, &node)

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return scores, nil
}

// calculateResourceScore computes a resource-based score for a node
func (s *Scheduler) calculateResourceScore(pod *v1.Pod, node *v1.Node) int {
	// Check for pre-calculated score in node labels
	if computeScoreStr, exists := node.Labels["node-capability/compute-score"]; exists {
		if computeScore, err := strconv.Atoi(computeScoreStr); err == nil {
			return computeScore
		}
	}

	// Calculate resources requested by the pod
	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // Default: 100m CPU
		}

		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // Default: 200Mi
		}
	}

	// Get allocatable resources from the node
	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	// Calculate usage ratios
	cpuRatio := float64(requestedCPU) / float64(allocatableCPU)
	memoryRatio := float64(requestedMemory) / float64(allocatableMemory)

	// For balanced allocation, prefer nodes where resource allocation
	// after placing the pod would be balanced between CPU and memory
	diff := math.Abs(cpuRatio - memoryRatio)

	// Convert to a 0-100 score (smaller diff = higher score)
	score := int((1 - diff) * 100)

	// Add bonus for nodes with special hardware when appropriate
	if hasGPUCapability(node) {
		// Check if pod needs GPU
		if podNeedsGPU(pod) {
			score += 20 // Significant bonus for matching GPU needs
		}
	}

	// Add bonus for nodes with fast storage when appropriate
	if hasFastStorage(node) {
		// Check if pod needs fast storage
		if podNeedsFastStorage(pod) {
			score += 10 // Bonus for matching storage needs
		}
	}

	// Ensure score is in range
	if score > 100 {
		score = 100
	} else if score < 0 {
		score = 0
	}

	return score
}

// scoreNodeAffinity scores nodes based on node affinity preferences
func (s *Scheduler) scoreNodeAffinity(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	// Check if pod has node affinity preferences
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution == nil {
		// No affinity preferences, give all nodes a neutral score
		for _, node := range nodes {
			scores = append(scores, NodeScore{
				Name:  node.Name,
				Score: 50, // Neutral score
			})
		}
		return scores, nil
	}

	// Process node affinity preferences
	preferences := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution

	for _, node := range nodes {
		totalScore := 0
		maxPossibleScore := 0

		for _, preference := range preferences {
			weight := preference.Weight
			maxPossibleScore += int(weight)

			// Check if node matches the preference
			nodeSelector := preference.Preference
			if s.nodeSelectorMatches(&node, &nodeSelector) {
				totalScore += int(weight)
			}
		}

		// Normalize to 0-100 range
		normalizedScore := 0
		if maxPossibleScore > 0 {
			normalizedScore = totalScore * 100 / maxPossibleScore
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: normalizedScore,
		})
	}

	return scores, nil
}

// nodeSelectorMatches checks if a node matches node selector terms
func (s *Scheduler) nodeSelectorMatches(node *v1.Node, selector *v1.NodeSelectorTerm) bool {
	// Check match expressions
	for _, expr := range selector.MatchExpressions {
		switch expr.Operator {
		case v1.NodeSelectorOpIn:
			if !nodeHasValueForKey(node, expr.Key, expr.Values) {
				return false
			}
		case v1.NodeSelectorOpNotIn:
			if nodeHasValueForKey(node, expr.Key, expr.Values) {
				return false
			}
		case v1.NodeSelectorOpExists:
			if !nodeHasKey(node, expr.Key) {
				return false
			}
		case v1.NodeSelectorOpDoesNotExist:
			if nodeHasKey(node, expr.Key) {
				return false
			}
		case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
			if !nodeMatchesNumericComparison(node, expr.Key, expr.Operator, expr.Values) {
				return false
			}
		}
	}

	return true
}

// scoreNodeType scores nodes based on preference for edge or cloud
func (s *Scheduler) scoreNodeType(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	// Determine if pod has a locality preference
	preferEdge := false
	preferCloud := false

	if pod.Annotations != nil {
		_, preferEdge = pod.Annotations["scheduler.thesis/prefer-edge"]
		_, preferCloud = pod.Annotations["scheduler.thesis/prefer-cloud"]
	}

	for _, node := range nodes {
		score := 50 // Default neutral score

		// Determine if node is edge or cloud
		isEdge := isEdgeNode(&node)

		// Adjust score based on preference
		if preferEdge && isEdge {
			score = 100
		} else if preferEdge && !isEdge {
			score = 0
		} else if preferCloud && !isEdge {
			score = 100
		} else if preferCloud && isEdge {
			score = 0
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return scores, nil
}

// scoreNodeCapabilities scores nodes based on their capabilities
func (s *Scheduler) scoreNodeCapabilities(pod *v1.Pod, nodes []v1.Node) ([]NodeScore, error) {
	var scores []NodeScore

	// Extract capability requirements from pod
	requiredCapabilities := extractPodCapabilityRequirements(pod)

	for _, node := range nodes {
		// Start with base score
		score := 50

		// Check each capability
		matchCount := 0
		for capability, required := range requiredCapabilities {
			// Look for corresponding label on node
			labelKey := fmt.Sprintf("node-capability/%s", capability)
			if value, exists := node.Labels[labelKey]; exists && (value == "true" || value == required) {
				matchCount++
			}
		}

		// Adjust score based on matches
		if len(requiredCapabilities) > 0 {
			score = matchCount * 100 / len(requiredCapabilities)
		}

		// Add special hardware bonus
		if hasGPUCapability(&node) && podNeedsGPU(pod) {
			score += 20
		}

		if hasFastStorage(&node) && podNeedsFastStorage(pod) {
			score += 10
		}

		// Ensure score is in range
		if score > 100 {
			score = 100
		}

		scores = append(scores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return scores, nil
}

// nodeFitsResources checks if node has enough resources for the pod
func (s *Scheduler) nodeFitsResources(ctx context.Context, pod *v1.Pod, node *v1.Node) bool {
	var requestedCPU, requestedMemory int64

	// Calculate resource requests
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests.Cpu() != nil {
			requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		} else {
			requestedCPU += 100 // Default 100m CPU
		}

		if container.Resources.Requests.Memory() != nil {
			requestedMemory += container.Resources.Requests.Memory().Value()
		} else {
			requestedMemory += 200 * 1024 * 1024 // Default 200Mi memory
		}
	}

	// Get node capacity
	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	// Get resources used by existing pods
	fieldSelector := fields.SelectorFromSet(fields.Set{"spec.nodeName": node.Name})
	pods, err := s.clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector.String(),
	})

	if err != nil {
		klog.Errorf("Error getting pods on node %s: %v", node.Name, err)
		// Be conservative if we can't check usage
		return false
	}

	var usedCPU, usedMemory int64
	for _, p := range pods.Items {
		// Skip pods being deleted
		if p.DeletionTimestamp != nil {
			continue
		}

		for _, container := range p.Spec.Containers {
			if container.Resources.Requests.Cpu() != nil {
				usedCPU += container.Resources.Requests.Cpu().MilliValue()
			} else {
				usedCPU += 100 // Default 100m
			}

			if container.Resources.Requests.Memory() != nil {
				usedMemory += container.Resources.Requests.Memory().Value()
			} else {
				usedMemory += 200 * 1024 * 1024 // Default 200Mi
			}
		}
	}

	// Check if node has enough remaining resources
	return (usedCPU+requestedCPU <= allocatableCPU) &&
		(usedMemory+requestedMemory <= allocatableMemory)
}

// nodeHasRequiredCapabilities checks if a node has required capabilities
func (s *Scheduler) nodeHasRequiredCapabilities(pod *v1.Pod, node *v1.Node) bool {
	if pod.Annotations == nil {
		return true // No specific requirements
	}

	// Check for required capability annotation
	if requiredCap, exists := pod.Annotations["scheduler.thesis/required-capability"]; exists {
		capLabel := fmt.Sprintf("node-capability/%s", requiredCap)
		if value, ok := node.Labels[capLabel]; !ok || value != "true" {
			return false
		}
	}

	// Check for GPU requirement
	if _, ok := pod.Annotations["scheduler.thesis/requires-gpu"]; ok {
		if !hasGPUCapability(node) {
			return false
		}
	}

	// Check for storage requirement
	if _, ok := pod.Annotations["scheduler.thesis/requires-local-storage"]; ok {
		if _, hasStorage := node.Labels[StorageNodeLabel]; !hasStorage {
			return false
		}
	}

	return true
}

// satisfiesNodeAffinity checks if node satisfies pod's node affinity
func (s *Scheduler) satisfiesNodeAffinity(pod *v1.Pod, node *v1.Node) bool {
	// If pod has no affinity requirements, all nodes pass
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return true
	}

	// Check required affinity
	affinity := pod.Spec.Affinity.NodeAffinity
	if affinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelectorTerms := affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(nodeSelectorTerms) > 0 {
			// At least one term must match
			for _, term := range nodeSelectorTerms {
				if s.nodeSelectorMatches(node, &term) {
					return true
				}
			}
			return false
		}
	}

	return true
}

// toleratesNodeTaints checks if pod tolerates node taints
func (s *Scheduler) toleratesNodeTaints(pod *v1.Pod, node *v1.Node) bool {
	if len(node.Spec.Taints) == 0 {
		return true // No taints to tolerate
	}

	// For each taint on the node, check if pod has a matching toleration
	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule ||
			taint.Effect == v1.TaintEffectNoExecute {
			if !tolerationsTolerateTaint(pod.Spec.Tolerations, &taint) {
				return false
			}
		}
	}

	return true
}

// bindPod binds a pod to a node
func (s *Scheduler) bindPod(ctx context.Context, pod *v1.Pod, nodeName string) error {
	klog.Infof("Binding pod %s/%s to node %s", pod.Namespace, pod.Name, nodeName)

	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Target: v1.ObjectReference{
			Kind:       "Node",
			Name:       nodeName,
			APIVersion: "v1",
		},
	}

	return s.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
}

// startHealthCheckServer starts a HTTP server for health checks
func (s *Scheduler) startHealthCheckServer(ctx context.Context) {
	mux := http.NewServeMux()

	// Basic health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Storage information endpoint
	mux.HandleFunc("/storage-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		s.storageMutex.RLock()
		defer s.storageMutex.RUnlock()

		// Build a report
		report := map[string]interface{}{
			"storageNodes": len(s.storageIndex.GetAllStorageNodes()),
			"buckets":      s.storageIndex.GetAllBuckets(),
			"lastUpdated":  s.storageIndex.GetLastRefreshed().Format(time.RFC3339),
		}

		json.NewEncoder(w).Encode(report)
	})

	// Storage summary endpoint
	mux.HandleFunc("/storage-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.storageIndex.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	// Bandwidth summary endpoint
	mux.HandleFunc("/bandwidth-summary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		s.storageMutex.RLock()
		summary := s.bandwidthGraph.PrintSummary()
		s.storageMutex.RUnlock()

		w.Write([]byte(summary))
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		klog.Info("Starting health check server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Health check server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		// Give it a timeout for shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		server.Shutdown(shutdownCtx)
	}()
}

// Helper functions

// isNodeReady checks if a node is in Ready condition
func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// isEdgeNode determines if a node is an edge node
func isEdgeNode(node *v1.Node) bool {
	// Check node label
	if value, ok := node.Labels[EdgeNodeLabel]; ok && value == EdgeNodeValue {
		return true
	}

	// Check node name for edge indicator
	if strings.Contains(strings.ToLower(node.Name), "edge") {
		return true
	}

	return false
}

// hasGPUCapability checks if a node has GPU capabilities
func hasGPUCapability(node *v1.Node) bool {
	for k, v := range node.Labels {
		if (strings.Contains(k, "gpu") || strings.Contains(k, "accelerator")) && v == "true" {
			return true
		}
	}

	return false
}

// hasFastStorage checks if a node has fast storage capabilities
func hasFastStorage(node *v1.Node) bool {
	// Check for fast storage labels
	if tech, ok := node.Labels["node-capability/storage-technology"]; ok {
		if tech == "nvme" || tech == "ssd" {
			return true
		}
	}

	// Check for explicit fast storage label
	if val, ok := node.Labels["node-capability/fast-storage"]; ok && val == "true" {
		return true
	}

	return false
}

// podNeedsGPU checks if a pod requires GPU capabilities
func podNeedsGPU(pod *v1.Pod) bool {
	if pod.Annotations != nil {
		if _, ok := pod.Annotations["scheduler.thesis/requires-gpu"]; ok {
			return true
		}
	}

	// Check for GPU resource requests
	for _, container := range pod.Spec.Containers {
		if container.Resources.Requests != nil {
			for resourceName := range container.Resources.Requests {
				if strings.Contains(string(resourceName), "gpu") ||
					strings.Contains(string(resourceName), "nvidia.com") {
					return true
				}
			}
		}
	}

	return false
}

// podNeedsFastStorage checks if a pod benefits from fast storage
func podNeedsFastStorage(pod *v1.Pod) bool {
	if pod.Annotations != nil {
		if _, ok := pod.Annotations["scheduler.thesis/requires-fast-storage"]; ok {
			return true
		}
	}

	return false
}

// nodeHasKey checks if a node has a specific label key
func nodeHasKey(node *v1.Node, key string) bool {
	_, exists := node.Labels[key]
	return exists
}

// nodeHasValueForKey checks if a node has one of the specified values for a key
func nodeHasValueForKey(node *v1.Node, key string, values []string) bool {
	if value, exists := node.Labels[key]; exists {
		for _, v := range values {
			if value == v {
				return true
			}
		}
	}
	return false
}

// nodeMatchesNumericComparison checks if a node's label matches numeric comparison
func nodeMatchesNumericComparison(node *v1.Node, key string, op v1.NodeSelectorOperator, values []string) bool {
	if val, exists := node.Labels[key]; exists && len(values) > 0 {
		nodeVal, err1 := strconv.Atoi(val)
		compareVal, err2 := strconv.Atoi(values[0])

		if err1 == nil && err2 == nil {
			if op == v1.NodeSelectorOpGt {
				return nodeVal > compareVal
			} else if op == v1.NodeSelectorOpLt {
				return nodeVal < compareVal
			}
		}
	}
	return false
}

// extractPodCapabilityRequirements extracts capability requirements from pod
func extractPodCapabilityRequirements(pod *v1.Pod) map[string]string {
	capabilities := make(map[string]string)

	if pod.Annotations != nil {
		for key, value := range pod.Annotations {
			if strings.HasPrefix(key, "scheduler.thesis/capability-") {
				capName := strings.TrimPrefix(key, "scheduler.thesis/capability-")
				capabilities[capName] = value
			}
		}

		// Special handling for common capability annotations
		if _, ok := pod.Annotations["scheduler.thesis/requires-gpu"]; ok {
			capabilities["gpu-accelerated"] = "true"
		}

		if _, ok := pod.Annotations["scheduler.thesis/requires-fast-storage"]; ok {
			capabilities["fast-storage"] = "true"
		}
	}

	return capabilities
}

// tolerationsTolerateTaint checks if tolerations tolerate the taint
func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for _, toleration := range tolerations {
		if toleration.Effect == taint.Effect &&
			(toleration.Key == taint.Key || toleration.Key == "") &&
			(toleration.Value == taint.Value || toleration.Operator == v1.TolerationOpExists) {
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
