package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/internal/operator"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig file (optional, for out-of-cluster)")
	flag.Parse()

	cfg, err := buildConfig(*kubeconfig)
	if err != nil {
		log.Fatalf("failed to build kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("failed to create kubernetes client: %v", err)
	}

	op, err := operator.New(cfg, clientset, operator.LoadConfigFromEnv())
	if err != nil {
		log.Fatalf("failed to create operator: %v", err)
	}

	log.Println("benchmark-operator starting")

	stopCh := make(chan struct{})
	defer close(stopCh)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		<-sigCh
		close(stopCh)
	}()

	if err := op.Run(stopCh); err != nil {
		log.Fatalf("operator stopped with error: %v", err)
	}

	log.Println("operator stopped")
	time.Sleep(250 * time.Millisecond)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}

	if env := os.Getenv("KUBECONFIG"); env != "" {
		return clientcmd.BuildConfigFromFlags("", env)
	}

	return clientcmd.BuildConfigFromFlags("", "")
}
