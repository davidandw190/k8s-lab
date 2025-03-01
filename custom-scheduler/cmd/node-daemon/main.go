package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/daemon"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var nodeName string
	var collectionInterval int

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&nodeName, "node-name", "", "Name of the node this daemon is running on")
	flag.IntVar(&collectionInterval, "collection-interval", 30, "Interval between capability collections in seconds")

	flag.Parse()

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
		<-sigCh
		klog.Info("Received termination signal, shutting down")
		cancel()
	}()

	ticker := time.NewTicker(time.Duration(collectionInterval) * time.Second)
	defer ticker.Stop()

	// Collect initially
	if err := collector.CollectAndUpdateCapabilities(ctx); err != nil {
		klog.Errorf("Error collecting node capabilities: %v", err)
	}

	// Collect periodically
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
