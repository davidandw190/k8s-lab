package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// Default interval between scheduler runs
	schedulerInterval = 1 * time.Second
)

// NodeScore represents a node's score for scheduling
type NodeScore struct {
	Name  string
	Score int
}

// Scheduler implements a custom Kubernetes scheduler
type Scheduler struct {
	clientset     kubernetes.Interface
	schedulerName string
	podQueue      chan *v1.Pod
}

// NewScheduler creates a new custom scheduler
func NewScheduler(clientset kubernetes.Interface, schedulerName string) *Scheduler {
	return &Scheduler{
		clientset:     clientset,
		schedulerName: schedulerName,
		podQueue:      make(chan *v1.Pod, 100),
	}
}

// Run starts the scheduler
func (s *Scheduler) Run(ctx context.Context) error {
	// Create a pod informer to watch for unscheduled pods
	podInformer := s.createPodInformer(ctx)
	go podInformer.Run(ctx.Done())

	// Wait for the informer cache to sync
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for pod informer caches to sync")
	}

	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			klog.Errorf("Health check server failed: %v", err)
		}
	}()

	// Start the scheduling loop
	go wait.UntilWithContext(ctx, s.scheduleOne, schedulerInterval)

	// Wait for the context to be canceled
	<-ctx.Done()
	klog.Info("Scheduler shutting down")
	return nil
}

// createPodInformer creates a pod informer to watch for unscheduled pods
func (s *Scheduler) createPodInformer(ctx context.Context) cache.Controller {
	// Create a field selector to filter pods by scheduler name and nodeName=""
	fieldSelector := fields.SelectorFromSet(fields.Set{
		"spec.schedulerName": s.schedulerName,
		"spec.nodeName":      "",
	})

	// Create a pod lister and informer
	podListWatcher := cache.NewListWatchFromClient(
		s.clientset.CoreV1().RESTClient(),
		"pods",
		metav1.NamespaceAll,
		fieldSelector,
	)

	// Create an informer to watch for unscheduled pods
	_, informer := cache.NewIndexerInformer(
		podListWatcher,
		&v1.Pod{},
		0,
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
	// Check if the pod is already scheduled
	if pod.Spec.NodeName != "" {
		return
	}

	// Check if the pod is using our scheduler
	if pod.Spec.SchedulerName != s.schedulerName {
		return
	}

	// Add the pod to the queue
	s.podQueue <- pod
}

// scheduleOne schedules a single pod
func (s *Scheduler) scheduleOne(ctx context.Context) {
	// Try to get a pod from the queue
	var pod *v1.Pod
	select {
	case pod = <-s.podQueue:
	case <-ctx.Done():
		return
	default:
		return // No pods to schedule
	}

	klog.Infof("Attempting to schedule pod: %s/%s", pod.Namespace, pod.Name)

	// Find the best node for the pod
	nodeName, err := s.findBestNodeForPod(ctx, pod)
	if err != nil {
		klog.Errorf("Failed to find a suitable node for pod %s/%s: %v", pod.Namespace, pod.Name, err)
		return
	}

	// Bind the pod to the node
	err = s.bindPod(ctx, pod, nodeName)
	if err != nil {
		klog.Errorf("Failed to bind pod %s/%s to node %s: %v", pod.Namespace, pod.Name, nodeName, err)
		return
	}

	klog.Infof("Successfully scheduled pod %s/%s to node %s", pod.Namespace, pod.Name, nodeName)
}

// findBestNodeForPod finds the best node for a pod based on our custom logic
func (s *Scheduler) findBestNodeForPod(ctx context.Context, pod *v1.Pod) (string, error) {
	// Get the list of nodes
	nodes, err := s.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no nodes available")
	}

	// Filter nodes (predicates)
	filteredNodes := s.filterNodes(ctx, pod, nodes.Items)
	if len(filteredNodes) == 0 {
		return "", fmt.Errorf("no suitable nodes found after filtering")
	}

	// Score nodes (priorities)
	nodeScores := s.scoreNodes(pod, filteredNodes)
	if len(nodeScores) == 0 {
		return "", fmt.Errorf("no suitable nodes found after scoring")
	}

	// Sort nodes by score in descending order
	sort.Slice(nodeScores, func(i, j int) bool {
		return nodeScores[i].Score > nodeScores[j].Score
	})

	// Return the highest scoring node
	return nodeScores[0].Name, nil
}

// filterNodes filters out nodes that cannot run the pod
func (s *Scheduler) filterNodes(ctx context.Context, pod *v1.Pod, nodes []v1.Node) []v1.Node {
	var filteredNodes []v1.Node

	for _, node := range nodes {
		// Check if the node is ready
		if !isNodeReady(&node) {
			klog.V(4).Infof("Node %s is not ready", node.Name)
			continue
		}

		// Check node capability predicates based on node labels
		if !s.nodeCapabilityFilter(pod, &node) {
			klog.V(4).Infof("Node %s failed node capability filter", node.Name)
			continue
		}

		// Check resource availability
		if !s.resourcesAvailableFilter(ctx, pod, &node) {
			klog.V(4).Infof("Node %s failed resource availability filter", node.Name)
			continue
		}

		// Node passed all filters
		filteredNodes = append(filteredNodes, node)
	}

	return filteredNodes
}

// isNodeReady checks if a node is ready
func isNodeReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// nodeCapabilityFilter checks if a node meets the capability requirements
func (s *Scheduler) nodeCapabilityFilter(pod *v1.Pod, node *v1.Node) bool {
	// Check if pod requires specific capabilities through annotations
	if pod.Annotations != nil {
		if requiredCapability, exists := pod.Annotations["scheduler.thesis/required-capability"]; exists {
			// Check if node has the required capability
			if nodeCapability, hasLabel := node.Labels[fmt.Sprintf("node-capability/%s", requiredCapability)]; !hasLabel || nodeCapability != "true" {
				return false
			}
		}
	}

	return true
}

// resourcesAvailableFilter checks if a node has enough resources
func (s *Scheduler) resourcesAvailableFilter(ctx context.Context, pod *v1.Pod, node *v1.Node) bool {
	// Get the pods on the node
	fieldSelector := fields.SelectorFromSet(fields.Set{"spec.nodeName": node.Name})
	pods, err := s.clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector.String(),
	})

	if err != nil {
		klog.Errorf("Error getting pods on node %s: %v", node.Name, err)
		return false
	}

	// Calculate used resources on the node
	var usedCPU, usedMemory int64
	for _, p := range pods.Items {
		// Skip pods that are terminating
		if p.DeletionTimestamp != nil {
			continue
		}

		// Add up resource requests
		for _, container := range p.Spec.Containers {
			usedCPU += container.Resources.Requests.Cpu().MilliValue()
			usedMemory += container.Resources.Requests.Memory().Value()
		}
	}

	// Calculate resources requested by the pod
	var requestedCPU, requestedMemory int64
	for _, container := range pod.Spec.Containers {
		requestedCPU += container.Resources.Requests.Cpu().MilliValue()
		requestedMemory += container.Resources.Requests.Memory().Value()
	}

	// Get allocatable resources on the node
	allocatableCPU := node.Status.Allocatable.Cpu().MilliValue()
	allocatableMemory := node.Status.Allocatable.Memory().Value()

	// Check if the node has enough resources
	return (usedCPU+requestedCPU <= allocatableCPU) && (usedMemory+requestedMemory <= allocatableMemory)
}

// scoreNodes scores the nodes based on custom criteria
func (s *Scheduler) scoreNodes(pod *v1.Pod, nodes []v1.Node) []NodeScore {
	var nodeScores []NodeScore

	for _, node := range nodes {
		score := 50 // Default score

		// Score based on node capability scores from node labels
		if computeScoreStr, exists := node.Labels["node-capability/compute-score"]; exists {
			if computeScore, err := strconv.Atoi(computeScoreStr); err == nil {
				score = computeScore
			}
		}

		// Adjust score for memory-intensive workloads
		if pod.Annotations != nil {
			if _, exists := pod.Annotations["scheduler.thesis/memory-intensive"]; exists {
				if memBandwidth, exists := node.Labels["node-capability/memory-bandwidth"]; exists {
					if memScore, err := strconv.Atoi(memBandwidth); err == nil {
						// Add a bonus of up to 20 points for high memory bandwidth
						score += memScore / 5
					}
				}
			}
		}

		// Add storage capability score for I/O intensive workloads
		if pod.Annotations != nil {
			if _, exists := pod.Annotations["scheduler.thesis/io-intensive"]; exists {
				if storageScore, exists := node.Labels["node-capability/storage-score"]; exists {
					if ioScore, err := strconv.Atoi(storageScore); err == nil {
						// Add a bonus of up to 25 points for high I/O performance
						score += ioScore / 4
					}
				}
			}
		}

		// Ensure score stays within bounds (0-100)
		if score > 100 {
			score = 100
		} else if score < 0 {
			score = 0
		}

		nodeScores = append(nodeScores, NodeScore{
			Name:  node.Name,
			Score: score,
		})
	}

	return nodeScores
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
