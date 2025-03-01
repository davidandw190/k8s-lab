package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/davidandw190/k8s-lab/custom-scheduler/pkg/scheduler"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var master string
	var schedulerName string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.StringVar(&master, "master", "", "The address of the Kubernetes API server")
	flag.StringVar(&schedulerName, "scheduler-name", "custom-scheduler", "Name of the scheduler")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
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

	klog.Infof("Starting custom scheduler: %s", schedulerName)
	if err := sched.Run(ctx); err != nil {
		klog.Fatalf("Error running scheduler: %s", err.Error())
	}
}
