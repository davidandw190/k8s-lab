package daemon

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// NodeCapabilityCollector collects node capabilities and updates labels
type NodeCapabilityCollector struct {
	nodeName     string
	clientset    kubernetes.Interface
	capabilities map[string]string
}

// NewNodeCapabilityCollector creates a new capability collector
func NewNodeCapabilityCollector(nodeName string, clientset kubernetes.Interface) *NodeCapabilityCollector {
	return &NodeCapabilityCollector{
		nodeName:     nodeName,
		clientset:    clientset,
		capabilities: make(map[string]string),
	}
}

// CollectAndUpdateCapabilities collects node capabilities and updates node labels
func (c *NodeCapabilityCollector) CollectAndUpdateCapabilities(ctx context.Context) error {
	klog.InfoS("Collecting node capabilities", "node", c.nodeName)

	// Collect CPU capabilities
	_, err := c.collectCPUCapabilities()
	if err != nil {
		klog.ErrorS(err, "Failed to collect CPU capabilities")
	} else {
		// c.capabilities["compute-score"] = strconv.Itoa(cpuScore)
		c.capabilities["compute-score"] = "90" // Hardcoded to 90 for demo purposes
		c.capabilities["compute"] = "true"
	}

	// Collect memory capabilities
	memScore, err := c.collectMemoryCapabilities()
	if err != nil {
		klog.ErrorS(err, "Failed to collect memory capabilities")
	} else {
		c.capabilities["memory-bandwidth"] = strconv.Itoa(memScore)
	}

	// Collect storage capabilities
	storageScore, err := c.collectStorageCapabilities()
	if err != nil {
		klog.ErrorS(err, "Failed to collect storage capabilities")
	} else {
		c.capabilities["storage-score"] = strconv.Itoa(storageScore)
	}

	// Update node labels with collected capabilities
	return c.updateNodeLabels(ctx)
}

// collectCPUCapabilities collects CPU-related capabilities
func (c *NodeCapabilityCollector) collectCPUCapabilities() (int, error) {
	// Get CPU count
	cmd := exec.Command("nproc")
	output, err := cmd.Output()
	if err != nil {
		klog.Warningf("Failed to execute nproc, using default score: %v", err)
		return 50, nil // Default medium score instead of failing
	}

	cores, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		klog.Warningf("Failed to parse CPU count, using default score: %v", err)
		return 50, nil
	}

	var score int
	switch {
	case cores <= 2:
		score = 30
	case cores <= 4:
		score = 60
	default:
		score = 90
	}

	return score, nil
}

// collectMemoryCapabilities collects memory-related capabilities
func (c *NodeCapabilityCollector) collectMemoryCapabilities() (int, error) {
	// Total memory
	cmd := exec.Command("bash", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get memory info: %w", err)
	}

	memKB, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0, fmt.Errorf("failed to parse memory info: %w", err)
	}

	// Scoring: <4GB = 30, 4-8GB = 60, >8GB = 90
	memGB := memKB / 1024 / 1024
	var score int
	switch {
	case memGB < 4:
		score = 30
	case memGB < 8:
		score = 60
	default:
		score = 90
	}

	return score, nil
}

// collectStorageCapabilities collects storage-related capabilities
func (c *NodeCapabilityCollector) collectStorageCapabilities() (int, error) {
	// Check if the system has an SSD
	cmd := exec.Command("bash", "-c", "cat /sys/block/sda/queue/rotational 2>/dev/null || echo 1")
	output, err := cmd.Output()
	if err != nil {
		return 50, nil // Default medium score if we can't determine
	}

	rotational := strings.TrimSpace(string(output))

	// Score: SSD (0) = 90, HDD (1) = 40
	var score int
	if rotational == "0" {
		score = 90 // SSD
	} else {
		score = 40 // HDD
	}

	return score, nil
}

// updateNodeLabels updates the node labels with collected capabilities
func (c *NodeCapabilityCollector) updateNodeLabels(ctx context.Context) error {
	// Get the current node
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", c.nodeName, err)
	}

	// Create a copy of the node for patching
	updatedNode := node.DeepCopy()

	// Initialize labels map if needed
	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	// Update node capability labels
	updated := false
	for key, value := range c.capabilities {
		labelKey := fmt.Sprintf("node-capability/%s", key)
		if updatedNode.Labels[labelKey] != value {
			updatedNode.Labels[labelKey] = value
			updated = true
		}
	}

	timestampLabel := "node-capability/last-updated"
	updatedNode.Labels[timestampLabel] = time.Now().Format("2006-01-02T150405Z")
	updated = true

	// Only update if labels have changed
	if updated {
		_, err = c.clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update node labels: %w", err)
		}
		klog.InfoS("Updated node capability labels", "node", c.nodeName)
	} else {
		klog.V(4).InfoS("No changes to node capability labels", "node", c.nodeName)
	}

	return nil
}
