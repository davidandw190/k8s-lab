package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/scheduler"
	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/storage"
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
	var master string
	var schedulerName string
	var dataLocalityEnabled bool
	var showVersion bool
	var localBandwidthMBps float64
	var sameZoneBandwidthMBps float64
	var sameRegionBandwidthMBps float64
	var edgeToCloudBandwidthMBps float64

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&master, "master", "", "The address of the Kubernetes API server")
	flag.StringVar(&schedulerName, "scheduler-name", "data-locality-scheduler", "Name of the scheduler")
	flag.BoolVar(&dataLocalityEnabled, "enable-data-locality", true, "Enable data locality aware scheduling")
	flag.BoolVar(&showVersion, "version", false, "Show version information and exit")
	flag.Float64Var(&localBandwidthMBps, "local-bandwidth-mbps", 1000.0, "Bandwidth for local (same-node) transfers in MB/s")
	flag.Float64Var(&sameZoneBandwidthMBps, "same-zone-bandwidth-mbps", 500.0, "Bandwidth for same-zone transfers in MB/s")
	flag.Float64Var(&sameRegionBandwidthMBps, "same-region-bandwidth-mbps", 200.0, "Bandwidth for same-region transfers in MB/s")
	flag.Float64Var(&edgeToCloudBandwidthMBps, "edge-cloud-bandwidth-mbps", 25.0, "Bandwidth for edge-to-cloud transfers in MB/s")

	flag.Parse()

	if showVersion {
		fmt.Printf("Data Locality Scheduler v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		klog.Info("Received termination signal, shutting down")
		cancel()
	}()

	var config *rest.Config
	var err error

	if kubeconfig == "" {
		klog.Info("Using in-cluster configuration")
		config, err = rest.InClusterConfig()
	} else {
		klog.Infof("Using kubeconfig: %s", kubeconfig)
		config, err = clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}

	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error creating Kubernetes client: %s", err.Error())
	}

	sched := scheduler.NewScheduler(clientset, schedulerName)

	if dataLocalityEnabled {
		localBandwidth := localBandwidthMBps * 1024 * 1024
		sameZoneBandwidth := sameZoneBandwidthMBps * 1024 * 1024
		sameRegionBandwidth := sameRegionBandwidthMBps * 1024 * 1024
		edgeToCloudBandwidth := edgeToCloudBandwidthMBps * 1024 * 1024
		bandwidthGraph := storage.NewBandwidthGraph(10 * 1024 * 1024) // 10 MB/s default for unknown connections

		storageIndex := storage.NewStorageIndex()

		bandwidthGraph.SetTopologyDefaults(
			localBandwidth, 0.1,
			sameZoneBandwidth, 1.0,
			sameRegionBandwidth, 5.0,
			edgeToCloudBandwidth, 20.0,
		)

		sched.SetStorageIndex(storageIndex)
		sched.SetBandwidthGraph(bandwidthGraph)
	}

	klog.Infof("Starting scheduler: %s (version %s)", schedulerName, version)
	klog.Infof("Data locality awareness: %v", dataLocalityEnabled)

	if err := sched.Run(ctx); err != nil {
		klog.Fatalf("Error running scheduler: %s", err.Error())
	}
}
