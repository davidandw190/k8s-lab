package daemon

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	LabelPrefix  = "node-capability"
	MaxScore     = 100
	MinScore     = 10
	DefaultScore = 50
)

// NodeCapabilityCollector collects node capabilities and updates labels
type NodeCapabilityCollector struct {
	nodeName      string
	clientset     kubernetes.Interface
	capabilities  map[string]string
	metricCache   map[string]interface{}
	lastCollected time.Time
}

// NewNodeCapabilityCollector creates a new capability collector
func NewNodeCapabilityCollector(nodeName string, clientset kubernetes.Interface) *NodeCapabilityCollector {
	return &NodeCapabilityCollector{
		nodeName:     nodeName,
		clientset:    clientset,
		capabilities: make(map[string]string),
		metricCache:  make(map[string]interface{}),
	}
}

// CollectAndUpdateCapabilities collects node capabilities and updates node labels
func (c *NodeCapabilityCollector) CollectAndUpdateCapabilities(ctx context.Context) error {
	klog.InfoS("Collecting node capabilities", "node", c.nodeName)

	startTime := time.Now()

	c.capabilities = make(map[string]string)

	c.capabilities["compute"] = "true"
	c.capabilities["storage"] = "true"
	c.capabilities["memory"] = "true"
	c.capabilities["network"] = "true"

	c.collectPlatformInfo()

	if err := c.collectComputeCapabilities(); err != nil {
		klog.Warning(err, "Partial failure collecting compute capabilities")
	}

	if err := c.collectMemoryCapabilities(); err != nil {
		klog.Warning(err, "Partial failure collecting memory capabilities")
	}

	if err := c.collectStorageCapabilities(); err != nil {
		klog.Warning(err, "Partial failure collecting storage capabilities")
	}

	if err := c.collectNetworkCapabilities(); err != nil {
		klog.Warning(err, "Partial failure collecting network capabilities")
	}

	c.detectSpecializedHardware()

	c.capabilities["last-updated"] = time.Now().Format("20060102-150405")
	c.capabilities["collection-duration-ms"] = strconv.Itoa(int(time.Since(startTime).Milliseconds()))

	if err := c.updateNodeLabels(ctx); err != nil {
		return fmt.Errorf("failed to update node labels: %w", err)
	}

	c.lastCollected = time.Now()

	return nil
}

// collectPlatformInfo gathers basic OS and platform information
func (c *NodeCapabilityCollector) collectPlatformInfo() {
	osType := "unknown"
	osDistro := "unknown"

	// Try to detect Linux distribution
	if output, err := c.executeCommand("uname", "-s"); err == nil {
		osType = strings.TrimSpace(string(output))
		c.capabilities["os-type"] = sanitizeValue(osType)
	}

	// Check for specific distributions
	if _, err := c.executeCommand("test", "-f", "/etc/os-release"); err == nil {
		if output, err := c.executeCommand("cat", "/etc/os-release"); err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "ID=") {
					osDistro = strings.Trim(strings.TrimPrefix(line, "ID="), "\"")
					c.capabilities["os-distro"] = sanitizeValue(osDistro)
					break
				}
			}
		}
	}

	if output, err := c.executeCommand("uname", "-r"); err == nil {
		kernelVersion := strings.TrimSpace(string(output))
		c.capabilities["kernel-version"] = sanitizeValue(kernelVersion)
	}

	c.capabilities["is-container"] = "false"
	if _, err := c.executeCommand("test", "-f", "/.dockerenv"); err == nil {
		c.capabilities["is-container"] = "true"
	} else if output, err := c.executeCommand("cat", "/proc/1/cgroup"); err == nil && strings.Contains(string(output), "docker") {
		c.capabilities["is-container"] = "true"
	}
}

// collectComputeCapabilities collects CPU-related capabilities with fallbacks
func (c *NodeCapabilityCollector) collectComputeCapabilities() error {
	var cpuCores int = 1 // Default to minimum
	var cpuThreads int = 1
	var cpuModel string = "unknown"
	var cpuScore int = DefaultScore
	var errors []string

	if output, err := c.executeCommand("nproc"); err == nil {
		if count, err := strconv.Atoi(strings.TrimSpace(string(output))); err == nil {
			cpuThreads = count
		} else {
			errors = append(errors, fmt.Sprintf("failed to parse nproc output: %v", err))
		}
	} else if output, err := c.executeCommand("grep", "-c", "^processor", "/proc/cpuinfo"); err == nil {
		if count, err := strconv.Atoi(strings.TrimSpace(string(output))); err == nil {
			cpuThreads = count
		} else {
			errors = append(errors, fmt.Sprintf("failed to parse cpuinfo processor count: %v", err))
		}
	} else {
		errors = append(errors, "could not determine CPU thread count")
	}

	if output, err := c.executeCommand("cat", "/proc/cpuinfo"); err == nil {
		physicalIds := make(map[string]bool)
		coreIds := make(map[string]bool)

		lines := strings.Split(string(output), "\n")
		currentPhysicalId := ""
		currentCoreId := ""

		for _, line := range lines {
			line = strings.TrimSpace(line)

			if strings.HasPrefix(line, "model name") && cpuModel == "unknown" {
				cpuModel = strings.TrimSpace(strings.Split(line, ":")[1])
			}

			if strings.HasPrefix(line, "physical id") {
				currentPhysicalId = strings.TrimSpace(strings.Split(line, ":")[1])
				physicalIds[currentPhysicalId] = true
			} else if strings.HasPrefix(line, "core id") {
				currentCoreId = currentPhysicalId + ":" + strings.TrimSpace(strings.Split(line, ":")[1])
				coreIds[currentCoreId] = true
			}
		}

		if len(coreIds) > 0 {
			cpuCores = len(coreIds)
		} else {
			cpuCores = len(physicalIds)
			if cpuCores == 0 {
				cpuCores = cpuThreads
			}
		}
	} else {
		errors = append(errors, "could not open /proc/cpuinfo")
		cpuCores = cpuThreads
	}

	cpuScore = calculateCpuScore(cpuCores, cpuThreads)

	var maxFreq float64 = 0
	if output, err := c.executeCommand("cat", "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq"); err == nil {
		if freq, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64); err == nil {
			maxFreq = freq / 1000.0
		}
	}

	hasAvx := c.checkCpuFlag("avx")
	hasAvx2 := c.checkCpuFlag("avx2")
	hasAvx512 := c.checkCpuFlag("avx512")

	c.capabilities["cpu-cores"] = strconv.Itoa(cpuCores)
	c.capabilities["cpu-threads"] = strconv.Itoa(cpuThreads)
	c.capabilities["cpu-model"] = sanitizeValue(cpuModel)
	c.capabilities["compute-score"] = strconv.Itoa(cpuScore)

	if maxFreq > 0 {
		c.capabilities["cpu-freq-mhz"] = strconv.Itoa(int(maxFreq))
	}

	if hasAvx {
		c.capabilities["cpu-avx"] = "true"
	}

	if hasAvx2 {
		c.capabilities["cpu-avx2"] = "true"
	}

	if hasAvx512 {
		c.capabilities["cpu-avx512"] = "true"
	}

	if cpuScore >= 70 {
		c.capabilities["high-compute"] = "true"
	}

	if hasAvx2 || hasAvx512 {
		c.capabilities["vector-optimized"] = "true"
	}

	if len(errors) > 0 {
		return fmt.Errorf("compute capability collection had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	return nil
}

// checkCpuFlag checks if a CPU flag is present
func (c *NodeCapabilityCollector) checkCpuFlag(flag string) bool {
	if output, err := c.executeCommand("grep", flag, "/proc/cpuinfo"); err == nil {
		return strings.Contains(strings.ToLower(string(output)), flag)
	}
	return false
}

// calculateCpuScore determines a score based on CPU capabilities
func calculateCpuScore(cores, threads int) int {
	var score int
	switch {
	case cores >= 16:
		score = 95
	case cores >= 8:
		score = 80
	case cores >= 4:
		score = 65
	case cores >= 2:
		score = 45
	default:
		score = 25
	}

	if cores > 0 {
		threadsPerCore := threads / cores
		if threadsPerCore >= 2 {
			score += 5
		}
	}

	if score > MaxScore {
		score = MaxScore
	}

	return score
}

// collectMemoryCapabilities collects memory-related capabilities
func (c *NodeCapabilityCollector) collectMemoryCapabilities() error {
	var memTotal int64 = 0
	var memScore int = DefaultScore
	var errors []string

	if output, err := c.executeCommand("grep", "MemTotal", "/proc/meminfo"); err == nil {
		fields := strings.Fields(string(output))
		if len(fields) >= 2 {
			if memKB, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				memTotal = memKB * 1024
			} else {
				errors = append(errors, fmt.Sprintf("failed to parse memory value: %v", err))
			}
		} else {
			errors = append(errors, "unexpected format in meminfo")
		}
	} else {
		errors = append(errors, fmt.Sprintf("failed to get memory info: %v", err))
	}

	var memGB int
	if memTotal > 0 {
		memGB = int(memTotal / (1024 * 1024 * 1024))
		memScore = calculateMemoryScore(memGB)
	} else {
		if output, err := c.executeCommand("cat", "/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
			if limit, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64); err == nil && limit != 9223372036854771712 { // Not the max value
				memTotal = limit
				memGB = int(memTotal / (1024 * 1024 * 1024))
				memScore = calculateMemoryScore(memGB)
			}
		}
	}

	var memBandwidthScore int = DefaultScore

	memType := c.detectMemoryType()
	if memType != "" {
		c.capabilities["memory-type"] = memType

		switch {
		case strings.Contains(memType, "DDR4"):
			memBandwidthScore = 70
		case strings.Contains(memType, "DDR5"):
			memBandwidthScore = 90
		case strings.Contains(memType, "DDR3"):
			memBandwidthScore = 50
		case strings.Contains(memType, "LPDDR"):
			memBandwidthScore = 40
		}
	}

	if memTotal > 0 {
		c.capabilities["memory-bytes"] = strconv.FormatInt(memTotal, 10)
		c.capabilities["memory-gb"] = strconv.Itoa(memGB)
	}

	c.capabilities["memory-score"] = strconv.Itoa(memScore)
	c.capabilities["memory-bandwidth"] = strconv.Itoa(memBandwidthScore)

	if memGB >= 16 {
		c.capabilities["high-memory"] = "true"
	}

	if memBandwidthScore >= 70 {
		c.capabilities["high-bandwidth"] = "true"
	}

	if len(errors) > 0 {
		return fmt.Errorf("memory capability collection had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	return nil
}

// detectMemoryType attempts to identify the type of memory (DDR4, DDR5, etc.)
func (c *NodeCapabilityCollector) detectMemoryType() string {
	if output, err := c.executeCommandWithSudo("dmidecode", "-t", "memory"); err == nil {
		if strings.Contains(string(output), "DDR5") {
			return "DDR5"
		} else if strings.Contains(string(output), "DDR4") {
			return "DDR4"
		} else if strings.Contains(string(output), "DDR3") {
			return "DDR3"
		} else if strings.Contains(string(output), "LPDDR") {
			return "LPDDR"
		}
	}

	if output, err := c.executeCommand("cat", "/sys/devices/system/memory/memory0/dmi_memory_info"); err == nil {
		if strings.Contains(string(output), "DDR5") {
			return "DDR5"
		} else if strings.Contains(string(output), "DDR4") {
			return "DDR4"
		} else if strings.Contains(string(output), "DDR3") {
			return "DDR3"
		}
	}

	if output, err := c.executeCommandWithSudo("lshw", "-c", "memory"); err == nil {
		if strings.Contains(string(output), "DDR5") {
			return "DDR5"
		} else if strings.Contains(string(output), "DDR4") {
			return "DDR4"
		} else if strings.Contains(string(output), "DDR3") {
			return "DDR3"
		}
	}

	return ""
}

// calculateMemoryScore determines a score based on available memory
func calculateMemoryScore(memGB int) int {
	var score int
	switch {
	case memGB >= 64:
		score = 95
	case memGB >= 32:
		score = 80
	case memGB >= 16:
		score = 65
	case memGB >= 8:
		score = 50
	case memGB >= 4:
		score = 35
	default:
		score = 20
	}

	if score > MaxScore {
		score = MaxScore
	}

	return score
}

// collectStorageCapabilities collects storage-related capabilities
func (c *NodeCapabilityCollector) collectStorageCapabilities() error {
	var storageType string = "unknown"
	var storageScore int = DefaultScore
	var iopsScore int = DefaultScore
	var diskSizeGB int64 = 0
	var errors []string

	var isRotational bool = true
	var primaryDisk string = ""

	if output, err := c.executeCommand("lsblk", "-d", "-o", "NAME,SIZE", "-n"); err == nil {
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) >= 1 {
				// Skip loop devices, RAM disks, etc.
				if strings.HasPrefix(fields[0], "loop") || strings.HasPrefix(fields[0], "ram") {
					continue
				}
				primaryDisk = fields[0]
				break
			}
		}
	}

	if primaryDisk == "" {
		if output, err := c.executeCommand("mount"); err == nil {
			lines := strings.Split(string(output), "\n")
			for _, line := range lines {
				fields := strings.Fields(line)
				if len(fields) >= 3 && fields[2] == "/" {
					primaryDisk = filepath.Base(fields[0])
					primaryDisk = strings.TrimLeft(primaryDisk, "/dev/")
					primaryDisk = strings.TrimSuffix(primaryDisk, "1")
					primaryDisk = strings.TrimSuffix(primaryDisk, "2")
					primaryDisk = strings.TrimSuffix(primaryDisk, "3")
					primaryDisk = strings.TrimSuffix(primaryDisk, "p")
					break
				}
			}
		}
	}

	if primaryDisk != "" {
		rotPath := fmt.Sprintf("/sys/block/%s/queue/rotational", primaryDisk)
		if output, err := c.executeCommand("cat", rotPath); err == nil {
			rotValue := strings.TrimSpace(string(output))
			isRotational = (rotValue != "0")
		}
	}

	var deviceModel string
	if primaryDisk != "" {
		modelPath := fmt.Sprintf("/sys/block/%s/device/model", primaryDisk)
		if output, err := c.executeCommand("cat", modelPath); err == nil {
			deviceModel = strings.TrimSpace(string(output))
		}
	}

	hasNVMe := false
	if output, err := c.executeCommand("ls", "/dev"); err == nil {
		hasNVMe = strings.Contains(string(output), "nvme")
	}

	if hasNVMe {
		storageType = "nvme"
		storageScore = 90
		iopsScore = 85
	} else if !isRotational {
		storageType = "ssd"
		storageScore = 75
		iopsScore = 70
	} else {
		storageType = "hdd"
		storageScore = 40
		iopsScore = 30
	}

	if deviceModel != "" {
		deviceModelLower := strings.ToLower(deviceModel)
		if strings.Contains(deviceModelLower, "nvme") ||
			strings.Contains(deviceModelLower, "ssd") {
			if storageType != "nvme" {
				storageType = "ssd"
				storageScore = 75
				iopsScore = 70
			}
		}
	}

	if primaryDisk != "" {
		if output, err := c.executeCommand("lsblk", "-b", "-d", "-o", "SIZE", "-n", fmt.Sprintf("/dev/%s", primaryDisk)); err == nil {
			size, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
			if err == nil {
				diskSizeGB = size / (1024 * 1024 * 1024)
			}
		}
	}

	if diskSizeGB == 0 {
		if output, err := c.executeCommand("df", "-BG", "/"); err == nil {
			lines := strings.Split(string(output), "\n")
			if len(lines) > 1 {
				fields := strings.Fields(lines[1])
				if len(fields) >= 2 {
					sizeStr := strings.TrimSuffix(fields[1], "G")
					if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
						diskSizeGB = size
					}
				}
			}
		}
	}

	// Store collected information
	c.capabilities["storage-type"] = storageType
	c.capabilities["storage-score"] = strconv.Itoa(storageScore)
	c.capabilities["storage-iops"] = strconv.Itoa(iopsScore)

	if diskSizeGB > 0 {
		c.capabilities["storage-size-gb"] = strconv.FormatInt(diskSizeGB, 10)
	}

	if storageType == "nvme" || (storageType == "ssd" && diskSizeGB > 200) {
		c.capabilities["fast-storage"] = "true"
	}

	if diskSizeGB > 500 {
		c.capabilities["high-capacity"] = "true"
	}

	if len(errors) > 0 {
		return fmt.Errorf("storage capability collection had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	return nil
}

// collectNetworkCapabilities collects network interface capabilities
func (c *NodeCapabilityCollector) collectNetworkCapabilities() error {
	var networkScore int = DefaultScore
	var interfaceSpeed int64 = 0
	var errors []string

	var networkInterfaces []string
	if output, err := c.executeCommand("ls", "/sys/class/net"); err == nil {
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		for _, line := range lines {
			if line != "lo" && !strings.HasPrefix(line, "docker") && !strings.HasPrefix(line, "veth") {
				networkInterfaces = append(networkInterfaces, line)
			}
		}
	} else {
		errors = append(errors, fmt.Sprintf("failed to list network interfaces: %v", err))
	}

	for _, iface := range networkInterfaces {
		speedPath := fmt.Sprintf("/sys/class/net/%s/speed", iface)
		if output, err := c.executeCommand("cat", speedPath); err == nil {
			if speed, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64); err == nil {
				if speed > interfaceSpeed {
					interfaceSpeed = speed
				}
			}
		}

		if output, err := c.executeCommand("cat", fmt.Sprintf("/sys/class/net/%s/operstate", iface)); err == nil {
			if strings.TrimSpace(string(output)) == "up" {
				c.capabilities["active-interface"] = iface
			}
		}
	}

	// Calculate network score based on speed
	if interfaceSpeed > 0 {
		networkScore = calculateNetworkScore(interfaceSpeed)
	} else {
		for _, iface := range networkInterfaces {
			if strings.HasPrefix(iface, "en") || strings.HasPrefix(iface, "eth") {
				// Ethernet interface - decent score
				networkScore = 60
				break
			} else if strings.HasPrefix(iface, "wl") {
				// Wireless interface - lower score
				networkScore = 40
				break
			}
		}
	}

	c.capabilities["network-score"] = strconv.Itoa(networkScore)

	if interfaceSpeed > 0 {
		c.capabilities["network-speed-mbps"] = strconv.FormatInt(interfaceSpeed, 10)
	}

	if len(networkInterfaces) > 0 {
		c.capabilities["primary-interface"] = networkInterfaces[0]
	}

	if networkScore >= 80 {
		c.capabilities["high-bandwidth-network"] = "true"
	}

	if len(errors) > 0 {
		return fmt.Errorf("network capability collection had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	return nil
}

// calculateNetworkScore determines a score based on network speed
func calculateNetworkScore(speedMbps int64) int {
	var score int
	switch {
	case speedMbps >= 10000:
		score = 95 // 10 Gbps or higher
	case speedMbps >= 1000:
		score = 80 // 1 Gbps
	case speedMbps >= 100:
		score = 50 // 100 Mbps
	default:
		score = 30 // Less than 100 Mbps
	}

	if score > MaxScore {
		score = MaxScore
	}

	return score
}

// detectSpecializedHardware checks for specialized hardware like GPUs
func (c *NodeCapabilityCollector) detectSpecializedHardware() {
	hasNvidia := false
	if output, err := c.executeCommand("nvidia-smi", "--query-gpu=name", "--format=csv,noheader"); err == nil && len(output) > 0 {
		hasNvidia = true
		gpuModel := strings.TrimSpace(string(output))
		c.capabilities["gpu-nvidia"] = "true"
		c.capabilities["gpu-model"] = sanitizeValue(gpuModel)
		c.capabilities["gpu-accelerated"] = "true"
	}

	if !hasNvidia {
		if output, err := c.executeCommand("lspci"); err == nil {
			outputStr := strings.ToLower(string(output))

			if strings.Contains(outputStr, "nvidia") {
				c.capabilities["gpu-nvidia"] = "true"
				c.capabilities["gpu-accelerated"] = "true"
			} else if strings.Contains(outputStr, "amd") && (strings.Contains(outputStr, "graphics") || strings.Contains(outputStr, "display")) {
				c.capabilities["gpu-amd"] = "true"
				c.capabilities["gpu-accelerated"] = "true"
			} else if strings.Contains(outputStr, "intel") && (strings.Contains(outputStr, "graphics") || strings.Contains(outputStr, "display")) {
				c.capabilities["gpu-intel"] = "true"
			}
		}
	}

	// Check for TPUs (Google Cloud specific)
	if output, err := c.executeCommand("ls", "/dev"); err == nil {
		if strings.Contains(string(output), "accel") {
			c.capabilities["tpu-accelerated"] = "true"
		}
	}

	// Check for other accelerators
	if output, err := c.executeCommand("lspci"); err == nil {
		outputStr := strings.ToLower(string(output))

		// Check for FPGA
		if strings.Contains(outputStr, "fpga") {
			c.capabilities["fpga-accelerated"] = "true"
		}

		// Check for Infiniband
		if strings.Contains(outputStr, "infiniband") {
			c.capabilities["infiniband"] = "true"
			c.capabilities["high-bandwidth-network"] = "true"
		}
	}
}

// updateNodeLabels updates the node labels with collected capabilities
func (c *NodeCapabilityCollector) updateNodeLabels(ctx context.Context) error {
	node, err := c.clientset.CoreV1().Nodes().Get(ctx, c.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", c.nodeName, err)
	}

	updatedNode := node.DeepCopy()

	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	updated := false
	for key, value := range c.capabilities {
		labelKey := fmt.Sprintf("%s/%s", LabelPrefix, key)

		if !isValidLabelKey(labelKey) || !isValidLabelValue(value) {
			klog.Warning(nil, "Invalid label key or value, skipping", "key", labelKey, "value", value)
			continue
		}

		if updatedNode.Labels[labelKey] != value {
			updatedNode.Labels[labelKey] = value
			updated = true
		}
	}

	if updated {
		_, err = c.clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update node labels: %w", err)
		}
		klog.InfoS("Updated node capability labels", "node", c.nodeName, "labelCount", len(c.capabilities))
	} else {
		klog.V(4).InfoS("No changes to node capability labels", "node", c.nodeName)
	}

	return nil
}

// Helper method to execute system commands safely
func (c *NodeCapabilityCollector) executeCommand(command string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, command, args...)
	return cmd.Output()
}

// Helper method to execute commands with sudo if available
func (c *NodeCapabilityCollector) executeCommandWithSudo(command string, args ...string) ([]byte, error) {
	if _, err := exec.LookPath("sudo"); err == nil {
		sudoArgs := append([]string{command}, args...)
		return c.executeCommand("sudo", sudoArgs...)
	}

	return c.executeCommand(command, args...)
}

// Sanitize the label value to ensure it's valid for Kubernetes
// func sanitizeValue(value string) string {
// 	re := regexp.MustCompile(`[^a-zA-Z0-9._-]`)
// 	sanitized := re.ReplaceAllString(value, "-")

// 	if len(sanitized) > 63 {
// 		sanitized = sanitized[:63]
// 	}

// 	if len(sanitized) > 0 {
// 		if !isAlphaNumeric(sanitized[0]) {
// 			sanitized = "x" + sanitized[1:]
// 		}
// 		if !isAlphaNumeric(sanitized[len(sanitized)-1]) {
// 			sanitized = sanitized[:len(sanitized)-1] + "x"
// 		}
// 	} else {
// 		sanitized = "unknown"
// 	}

// 	return sanitized
// }

// Check if a character is alphanumeric
func isAlphaNumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// Check if a label key is valid for Kubernetes
func isValidLabelKey(key string) bool {
	// Label keys have two segments: an optional prefix and name, separated by a slash (/)
	// The prefix must be a DNS subdomain, and the name must be 63 characters or less
	parts := strings.Split(key, "/")
	if len(parts) > 2 {
		return false
	}

	name := parts[len(parts)-1]
	if len(name) > 63 || len(name) == 0 {
		return false
	}

	if len(parts) > 1 && len(parts[0]) > 0 {
		prefix := parts[0]
		if len(prefix) > 253 {
			return false
		}

		prefixParts := strings.Split(prefix, ".")
		for _, part := range prefixParts {
			if len(part) == 0 || len(part) > 63 {
				return false
			}
		}
	}

	return true
}

// Check if a label value is valid for Kubernetes
func isValidLabelValue(value string) bool {
	if len(value) > 63 {
		return false
	}

	if len(value) == 0 {
		return true
	}

	if !isAlphaNumeric(value[0]) || !isAlphaNumeric(value[len(value)-1]) {
		return false
	}

	for i := 0; i < len(value); i++ {
		c := value[i]
		if !isAlphaNumeric(c) && c != '-' && c != '_' && c != '.' {
			return false
		}
	}

	return true
}

// GetCapabilities returns the current capability values
func (c *NodeCapabilityCollector) GetCapabilities() map[string]string {
	return c.capabilities
}
