package daemon

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	StorageServiceLabelSelector = "app=storage-service"
	MinIOSelector               = "app=minio"
	StorageNodeLabel            = "node-capability/storage-service"
	StorageTypeLabel            = "node-capability/storage-type"
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
	nodeName  string
	clientset kubernetes.Interface
}

// NewDataLocalityCollector creates a new data locality collector
func NewDataLocalityCollector(nodeName string, clientset kubernetes.Interface) *DataLocalityCollector {
	return &DataLocalityCollector{
		nodeName:  nodeName,
		clientset: clientset,
	}
}

// CollectStorageCapabilities collects storage service information on the node
func (c *DataLocalityCollector) CollectStorageCapabilities(ctx context.Context) (map[string]string, error) {
	labels := make(map[string]string)

	fieldSelector := fmt.Sprintf("spec.nodeName=%s", c.nodeName)
	pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: StorageServiceLabelSelector,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list storage service pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return labels, nil
	}

	minioPods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
		LabelSelector: MinIOSelector,
	})

	if err != nil {
		klog.Warningf("Failed to list MinIO pods: %v", err)
	}

	if len(minioPods.Items) > 0 {
		labels[StorageNodeLabel] = "true"
		labels[StorageTypeLabel] = "minio"

		buckets, err := c.collectMinioBuckets(ctx, minioPods.Items[0])
		if err == nil && len(buckets) > 0 {
			for i, bucket := range buckets {
				if i < 10 {
					labels[fmt.Sprintf("node-capability/storage-bucket-%s", bucket)] = "true"
				}
			}
		}
	}

	for _, pod := range pods.Items {
		if storageType, ok := pod.Labels["storage-type"]; ok {
			labels[StorageNodeLabel] = "true"
			labels[fmt.Sprintf("node-capability/storage-type-%s", storageType)] = "true"
		}
	}

	c.collectNetworkMeasurements(ctx, labels)

	return labels, nil
}

// collectMinioBuckets attempts to list buckets from a MinIO instance
func (c *DataLocalityCollector) collectMinioBuckets(ctx context.Context, pod v1.Pod) ([]string, error) {
	// TODO: this is a placeholder implementation
	// -------
	// For this implementation we will use the MinIO API client with proper credentials
	// or query the MinIO service endpoint

	return []string{"data", "models", "artifacts"}, nil
}

// collectNetworkMeasurements collects network performance measurements between nodes
func (c *DataLocalityCollector) collectNetworkMeasurements(ctx context.Context, labels map[string]string) {
	// TODO: this is a placeholder implementation
	// -------
	// For this implementation we will measure bandwidth using iperf, store results in
	// a ConfigMap or other shared storage and use these measurements for decisions

	// Simulated bandwidth measurements
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list nodes for bandwidth measurement: %v", err)
		return
	}

	labels["node-capability/bandwidth-local"] = "1000000000" // 1 GB/s

	for i, node := range nodes.Items {
		if node.Name == c.nodeName {
			continue
		}

		if i < 5 {
			bandwidth := "100000000" // 100 MB/s

			if val, ok := node.Labels[StorageNodeLabel]; ok && val == "true" {
				if _, ok := labels[StorageNodeLabel]; ok {
					bandwidth = "500000000"
				}
			}

			labels[fmt.Sprintf("node-capability/bandwidth-to-%s", node.Name)] = bandwidth
		}
	}
}

// MeasureBandwidth measures bandwidth between the current node and another node
func (c *DataLocalityCollector) MeasureBandwidth(targetNode string) (int64, error) {
	// TODO: This is a placeholder implementation
	// -------
	// We would use a tool such as iperf for actual measurements
	// previous measurements from a database, network topology info

	// Simulated bandwidth value
	cmd := exec.Command("ping", "-c", "3", targetNode)
	output, err := cmd.CombinedOutput()

	if err != nil {
		return 10000000, fmt.Errorf("ping failed: %w", err) // 10 MB/s default
	}

	if strings.Contains(string(output), "min/avg/max") {
		return 100000000, nil
	}

	return 50000000, nil
}
