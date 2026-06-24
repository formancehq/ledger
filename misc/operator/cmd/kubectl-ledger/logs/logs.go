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

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
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
		Short:   "Stream logs from a LedgerService deployment",
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

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
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

	if f.all {
		return streamAllPods(ctx, cs, ns, name, logOpts)
	}

	ordinal := max(f.pod, 0)
	podName := cmdutil.LedgerServicePodName(name, ordinal)
	pterm.Info.Printfln("Streaming logs from %s", pterm.Cyan(podName))

	return streamPodLogs(ctx, cs, ns, podName, "", logOpts)
}

func streamAllPods(ctx context.Context, cs kubernetes.Interface, ns, name string, logOpts *corev1.PodLogOptions) error {
	pods, err := cmdutil.LedgerServicePods(ctx, cs, ns, name)
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	if len(pods.Items) == 0 {
		pterm.Error.Printfln("No pods found for LedgerService %s", pterm.Cyan(name))

		return fmt.Errorf("no pods found for ledger %q", name)
	}

	pterm.Info.Printfln("Streaming logs from %d pods", len(pods.Items))

	var wg sync.WaitGroup
	for i := range pods.Items {
		podName := pods.Items[i].Name
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
