package logs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
)

type logsFlags struct {
	pod    int
	all    bool
	follow bool
	tail   int64
	since  string
}

// NewCommand returns the "logs" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f logsFlags

	cmd := &cobra.Command{
		Use:     "logs [name]",
		Aliases: []string{"log"},
		Short:   "Stream logs from a Cluster deployment",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogs(cmd, opts, &f, args)
		},
	}

	cmd.Flags().IntVar(&f.pod, "pod", -1, "Pod ordinal to stream logs from (default: 0)")
	cmd.Flags().BoolVar(&f.all, "all", false, "Stream logs from all pods")
	cmd.Flags().BoolVarP(&f.follow, "follow", "f", false, "Follow log output")
	cmd.Flags().Int64Var(&f.tail, "tail", 100, "Number of lines to show from the end")
	cmd.Flags().StringVar(&f.since, "since", "", "Show logs since duration (e.g. 5m, 1h)")

	return cmd
}

func runLogs(cmd *cobra.Command, opts *cmdutil.Options, f *logsFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveClusterName(ctx, opts, args)
	if err != nil {
		return err
	}

	cs, err := opts.Clientset()
	if err != nil {
		return fmt.Errorf("creating clientset: %w", err)
	}

	logOpts := &corev1.PodLogOptions{
		Follow: f.follow,
	}
	if f.tail > 0 {
		logOpts.TailLines = &f.tail
	}
	if f.since != "" {
		d, err := time.ParseDuration(f.since)
		if err != nil {
			return fmt.Errorf("invalid --since %q: %w", f.since, err)
		}
		seconds := int64(d.Seconds())
		logOpts.SinceSeconds = &seconds
	}

	// Pod names derive from the Cluster base name, which spec.resourceName may
	// override (and another Cluster may share), so the pods are resolved from
	// the Cluster object and must be controlled by its StatefulSet.
	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}
	ledger, err := cmdutil.GetCluster(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	if f.all {
		return streamAllPods(ctx, cs, ledger, logOpts)
	}

	pod, err := cmdutil.ClusterPod(ctx, cs, ledger, max(f.pod, 0))
	if err != nil {
		return err
	}
	pterm.Info.Printfln("Streaming logs from %s", pterm.Cyan(pod.Name))

	return streamPodLogs(ctx, cs, ns, pod.Name, "", logOpts)
}

func streamAllPods(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster, logOpts *corev1.PodLogOptions) error {
	pods, err := cmdutil.ClusterPods(ctx, cs, ledger)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(pods) == 0 {
		pterm.Error.Printfln("No pods found for Cluster %s", pterm.Cyan(ledger.Name))

		return fmt.Errorf("no pods found for ledger %q", ledger.Name)
	}

	pterm.Info.Printfln("Streaming logs from %d pods", len(pods))

	ns := ledger.Namespace
	var wg sync.WaitGroup
	for i := range pods {
		podName := pods[i].Name
		wg.Go(func() {
			if err := streamPodLogs(ctx, cs, ns, podName, podName, logOpts); err != nil {
				pterm.Error.Printfln("[%s] %v", podName, err)
			}
		})
	}
	wg.Wait()

	return nil
}

func streamPodLogs(ctx context.Context, cs kubernetes.Interface, ns, podName, prefix string, logOpts *corev1.PodLogOptions) error {
	req := cs.CoreV1().Pods(ns).GetLogs(podName, logOpts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("opening log stream for %q: %w", podName, err)
	}
	defer func() { _ = stream.Close() }()

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if prefix != "" {
			pterm.Printf("[%s] %s\n", pterm.Cyan(prefix), scanner.Text())
		} else {
			pterm.Println(scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading logs from %q: %w", podName, err)
	}

	return nil
}
