package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/daemon"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var (
	version   = "0.0.3"
	buildTime = "2025-03-06"
)

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var nodeName string
	var collectionInterval int
	var healthServerPort int
	var showVersion bool
	var enableDataLocality bool
	var edgeNode bool
	var region string
	var zone string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&nodeName, "node-name", "", "Name of the node this daemon is running on")
	flag.IntVar(&collectionInterval, "collection-interval", 60, "Interval between capability collections in seconds")
	flag.IntVar(&healthServerPort, "health-port", 8080, "Port to serve health and metrics endpoints")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.BoolVar(&enableDataLocality, "enable-data-locality", true, "Enable data locality detection")
	flag.BoolVar(&edgeNode, "edge-node", false, "Mark this node as an edge node")
	flag.StringVar(&region, "region", "", "Region this node belongs to")
	flag.StringVar(&zone, "zone", "", "Zone this node belongs to")

	flag.Parse()

	if showVersion {
		fmt.Printf("Node Capability Daemon v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	if nodeName == "" {
		nodeName = os.Getenv("NODE_NAME")
		if nodeName == "" {
			klog.Fatal("Node name must be specified either via --node-name flag or NODE_NAME environment variable")
		}
	}

	if region == "" {
		region = os.Getenv("NODE_REGION")
	}

	if zone == "" {
		zone = os.Getenv("NODE_ZONE")
	}

	var config *rest.Config
	var err error

	if kubeconfig == "" {
		klog.Info("Using in-cluster configuration")
		config, err = rest.InClusterConfig()
	} else {
		klog.Infof("Using kubeconfig: %s", kubeconfig)
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	collector := daemon.NewNodeCapabilityCollector(nodeName, clientset)

	var dataCollector *daemon.DataLocalityCollector
	if enableDataLocality {
		dataCollector = daemon.NewDataLocalityCollector(nodeName, clientset)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		klog.Infof("Received signal %v, shutting down", sig)
		cancel()
	}()

	if edgeNode || region != "" || zone != "" {
		updateTopologyLabels(ctx, clientset, nodeName, edgeNode, region, zone)
	}

	go startHealthServer(healthServerPort, collector)

	klog.Info("Performing initial capability collection")
	if err := collector.CollectAndUpdateCapabilities(ctx); err != nil {
		klog.Errorf("Error collecting node capabilities: %v", err)
	}

	if enableDataLocality {
		klog.Info("Collecting data locality information")
		storageLabels, err := dataCollector.CollectStorageCapabilities(ctx)
		if err != nil {
			klog.Warningf("Error collecting data locality information: %v", err)
		} else if len(storageLabels) > 0 {
			updateNodeLabels(ctx, clientset, nodeName, storageLabels)
		}
	}

	klog.Infof("Node Capability Daemon v%s started on node %s", version, nodeName)
	klog.Infof("Collection interval: %d seconds", collectionInterval)
	klog.Infof("Data locality detection: %v", enableDataLocality)

	if edgeNode {
		klog.Info("This node is marked as an edge node")
	}
	if region != "" {
		klog.Infof("Region: %s", region)
	}
	if zone != "" {
		klog.Infof("Zone: %s", zone)
	}

	ticker := time.NewTicker(time.Duration(collectionInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := collector.CollectAndUpdateCapabilities(ctx); err != nil {
				klog.Errorf("Error collecting node capabilities: %v", err)
			}

			if enableDataLocality {
				storageLabels, err := dataCollector.CollectStorageCapabilities(ctx)
				if err != nil {
					klog.Warningf("Error collecting data locality information: %v", err)
				} else if len(storageLabels) > 0 {
					updateNodeLabels(ctx, clientset, nodeName, storageLabels)
				}
			}

		case <-ctx.Done():
			klog.Info("Node capability daemon shutting down")
			return
		}
	}
}

// updateTopologyLabels updates node topology labels
func updateTopologyLabels(ctx context.Context, clientset kubernetes.Interface, nodeName string, edgeNode bool, region, zone string) {
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get node %s: %v", nodeName, err)
		return
	}

	updatedNode := node.DeepCopy()
	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	updated := false

	// edge/cloud label
	if edgeNode {
		if updatedNode.Labels["node-capability/node-type"] != "edge" {
			updatedNode.Labels["node-capability/node-type"] = "edge"
			updated = true
		}
	} else {
		if updatedNode.Labels["node-capability/node-type"] != "cloud" {
			updatedNode.Labels["node-capability/node-type"] = "cloud"
			updated = true
		}
	}

	// region label
	if region != "" {
		if updatedNode.Labels["topology.kubernetes.io/region"] != region {
			updatedNode.Labels["topology.kubernetes.io/region"] = region
			updated = true
		}
	}

	// zone label
	if zone != "" {
		if updatedNode.Labels["topology.kubernetes.io/zone"] != zone {
			updatedNode.Labels["topology.kubernetes.io/zone"] = zone
			updated = true
		}
	}

	if updated {
		_, err = clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update node topology labels: %v", err)
		} else {
			klog.Info("Updated node topology labels")
		}
	}
}

// updateNodeLabels updates storage and bandwidth labels
func updateNodeLabels(ctx context.Context, clientset kubernetes.Interface, nodeName string, labels map[string]string) {
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get node %s: %v", nodeName, err)
		return
	}

	updatedNode := node.DeepCopy()
	if updatedNode.Labels == nil {
		updatedNode.Labels = make(map[string]string)
	}

	updated := false
	for key, value := range labels {
		if value == "" {
			if _, exists := updatedNode.Labels[key]; exists {
				delete(updatedNode.Labels, key)
				updated = true
			}
		} else if updatedNode.Labels[key] != value {
			updatedNode.Labels[key] = value
			updated = true
		}
	}

	if !updated {
		klog.V(4).Info("No label changes detected, skipping update")
		return
	}

	_, err = clientset.CoreV1().Nodes().Update(ctx, updatedNode, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update node labels: %v", err)
	} else {
		klog.Infof("Updated %d node labels", len(labels))
	}
}

// startHealthServer starts an HTTP server for health checks
func startHealthServer(port int, collector *daemon.NodeCapabilityCollector) {
	mux := http.NewServeMux()

	// Basic Health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Capabilities endpoint
	mux.HandleFunc("/capabilities", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		capabilities := collector.GetCapabilities()

		w.Write([]byte("{\n"))
		i := 0
		for key, value := range capabilities {
			comma := ""
			if i < len(capabilities)-1 {
				comma = ","
			}

			value = strings.Replace(value, "\"", "\\\"", -1)
			w.Write([]byte(fmt.Sprintf("  \"%s\": \"%s\"%s\n", key, value, comma)))
			i++
		}
		w.Write([]byte("}\n"))
	})

	// Version endpoint
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf("{\n  \"version\": \"%s\",\n  \"buildTime\": \"%s\"\n}\n",
			version, buildTime)))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	klog.Infof("Starting health server on :%d", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		klog.Errorf("Health server failed: %v", err)
	}
}
