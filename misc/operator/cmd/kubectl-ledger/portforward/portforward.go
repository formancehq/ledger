package portforward

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type portForwardFlags struct {
	grpc      bool
	pod       int
	localPort int32
}

// NewCommand returns the "port-forward" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f portForwardFlags

	cmd := &cobra.Command{
		Use:     "port-forward [name]",
		Aliases: []string{"pf"},
		Short:   "Port-forward to a LedgerService deployment",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPortForward(cmd, opts, &f, args)
		},
	}

	cmd.Flags().BoolVar(&f.grpc, "grpc", false, "Forward gRPC port instead of HTTP")
	cmd.Flags().IntVar(&f.pod, "pod", 0, "Pod ordinal to forward to")
	cmd.Flags().Int32Var(&f.localPort, "local-port", 0, "Local port (defaults to remote port)")

	return cmd
}

func runPortForward(cmd *cobra.Command, opts *cmdutil.Options, f *portForwardFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating CRD client: %w", err)
	}

	restConfig, err := opts.RESTConfig()
	if err != nil {
		return fmt.Errorf("getting REST config: %w", err)
	}

	cs, err := opts.Clientset()
	if err != nil {
		return fmt.Errorf("creating clientset: %w", err)
	}

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	// Determine remote port
	var remotePort int32
	proto := "HTTP"
	if f.grpc {
		proto = "gRPC"
		remotePort = ledger.Spec.Config.GrpcPort
		if remotePort == 0 {
			remotePort = 8888
		}
	} else {
		remotePort = ledger.Spec.Config.HttpPort
		if remotePort == 0 {
			remotePort = 9000
		}
	}

	localPort := f.localPort
	if localPort == 0 {
		localPort = remotePort
	}

	podName := fmt.Sprintf("%s-%d", name, f.pod)
	portSpec := fmt.Sprintf("%d:%d", localPort, remotePort)

	// Build the port-forward URL
	url := cs.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(ns).
		Name(podName).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return fmt.Errorf("creating SPDY transport: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)

	stopCh := make(chan struct{}, 1)
	readyCh := make(chan struct{})

	fw, err := portforward.New(dialer, []string{portSpec}, stopCh, readyCh, os.Stdout, os.Stderr)
	if err != nil {
		return fmt.Errorf("creating port-forward: %w", err)
	}

	// Handle interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		close(stopCh)
	}()

	pterm.Info.Printfln("Forwarding %s from %s -> %s:%d",
		pterm.Cyan(proto),
		pterm.Green(fmt.Sprintf("127.0.0.1:%d", localPort)),
		podName, remotePort,
	)
	pterm.Info.Println("Press Ctrl+C to stop")

	return fw.ForwardPorts()
}
