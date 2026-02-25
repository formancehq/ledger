package logs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
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
		Use:   "logs <name>",
		Short: "Stream logs from a Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLogs(cmd, opts, &f, args[0])
		},
	}

	cmd.Flags().IntVar(&f.pod, "pod", -1, "Pod ordinal to stream logs from (default: 0)")
	cmd.Flags().BoolVar(&f.all, "all", false, "Stream logs from all pods")
	cmd.Flags().BoolVarP(&f.follow, "follow", "f", false, "Follow log output")
	cmd.Flags().Int64Var(&f.tail, "tail", 100, "Number of lines to show from the end")
	cmd.Flags().StringVar(&f.since, "since", "", "Show logs since duration (e.g. 5m, 1h)")

	return cmd
}

func runLogs(cmd *cobra.Command, opts *cmdutil.Options, f *logsFlags, name string) error {
	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
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

	if f.all {
		return streamAllPods(ctx, cs, ns, name, logOpts)
	}

	ordinal := 0
	if f.pod >= 0 {
		ordinal = f.pod
	}
	podName := fmt.Sprintf("%s-%d", name, ordinal)
	return streamPodLogs(ctx, cs, ns, podName, "", logOpts)
}

func streamAllPods(ctx context.Context, cs kubernetes.Interface, ns, name string, logOpts *corev1.PodLogOptions) error {
	pods, err := cmdutil.LedgerPods(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return fmt.Errorf("no pods found for ledger %q", name)
	}

	var wg sync.WaitGroup
	for i := range pods.Items {
		podName := pods.Items[i].Name
		wg.Add(1)
		go func() {
			defer wg.Done()
			// best-effort: errors logged inline
			if err := streamPodLogs(ctx, cs, ns, podName, podName, logOpts); err != nil {
				fmt.Printf("[%s] error: %v\n", podName, err)
			}
		}()
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
	// Increase scanner buffer for long log lines.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if prefix != "" {
			fmt.Printf("[%s] %s\n", prefix, scanner.Text())
		} else {
			fmt.Println(scanner.Text())
		}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("reading logs from %q: %w", podName, err)
	}
	return nil
}
