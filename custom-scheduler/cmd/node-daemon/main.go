package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/daemon"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var (
	version   = "1.0.0"
	buildTime = "unknown"
)

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var nodeName string
	var collectionInterval int
	var healthServerPort int
	var showVersion bool

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&nodeName, "node-name", "", "Name of the node this daemon is running on")
	flag.IntVar(&collectionInterval, "collection-interval", 30, "Interval between capability collections in seconds")
	flag.IntVar(&healthServerPort, "health-port", 8080, "Port to serve health and metrics endpoints")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")

	flag.Parse()

	if showVersion {
		fmt.Printf("Node Capability Daemon v%s (built %s)\n", version, buildTime)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	if nodeName == "" {
		nodeName = os.Getenv("NODE_NAME")
		if nodeName == "" {
			klog.Fatal("Node name must be specified either via --node-name flag or NODE_NAME environment variable")
		}
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		klog.Infof("Received signal %v, shutting down", sig)
		cancel()
	}()

	go startHealthServer(healthServerPort, collector)

	ticker := time.NewTicker(time.Duration(collectionInterval) * time.Second)
	defer ticker.Stop()

	klog.Info("Performing initial capability collection")
	if err := collector.CollectAndUpdateCapabilities(ctx); err != nil {
		klog.Errorf("Error collecting node capabilities: %v", err)
	}

	klog.Infof("Node Capability Daemon v%s started on node %s", version, nodeName)
	klog.Infof("Collection interval: %d seconds", collectionInterval)

	for {
		select {
		case <-ticker.C:
			if err := collector.CollectAndUpdateCapabilities(ctx); err != nil {
				klog.Errorf("Error collecting node capabilities: %v", err)
			}
		case <-ctx.Done():
			klog.Info("Node capability collector shutting down")
			return
		}
	}
}

// startHealthServer starts an HTTP server for health checks and metrics
func startHealthServer(port int, collector *daemon.NodeCapabilityCollector) {
	// Define the health check handler
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/capabilities", func(w http.ResponseWriter, r *http.Request) {
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

	listenAddr := fmt.Sprintf(":%d", port)
	klog.Infof("Starting health server on %s", listenAddr)

	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		klog.Errorf("Health server failed: %v", err)
	}
}
