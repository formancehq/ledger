package config

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newViewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:     "view [name]",
		Aliases: []string{"show"},
		Short:   "Pretty-print LedgerService configuration",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runView(cmd, opts, args)
		},
	}
}

func runView(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerServiceName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedgerService(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(ledger.Spec.Config)
	case "yaml":
		return cmdutil.OutputYAML(ledger.Spec.Config)
	}

	cfg := &ledger.Spec.Config

	pterm.Println()
	pterm.Printf("Configuration for LedgerService %s\n", pterm.Bold.Sprint(pterm.Cyan(name)))
	cmdutil.Separator()

	// Core
	pterm.DefaultSection.Println("Core")
	cmdutil.RenderTable([]string{"KEY", "VALUE"}, coreRows(cfg))

	// Raft
	if cfg.Raft != nil {
		pterm.DefaultSection.Println("Raft")
		cmdutil.RenderTable([]string{"KEY", "VALUE"}, raftRows(cfg.Raft))
	}

	// Pebble
	if cfg.Pebble != nil {
		pterm.DefaultSection.Println("Pebble")
		cmdutil.RenderTable([]string{"KEY", "VALUE"}, pebbleRows(cfg.Pebble))
	}

	// Monitoring
	if cfg.Monitoring != nil {
		pterm.DefaultSection.Println("Monitoring")
		cmdutil.RenderTable([]string{"KEY", "VALUE"}, monitoringRows(cfg.Monitoring))
	}

	// Other
	other := otherRows(cfg)
	if len(other) > 0 {
		pterm.DefaultSection.Println("Other")
		cmdutil.RenderTable([]string{"KEY", "VALUE"}, other)
	}

	return nil
}

func coreRows(cfg *ledgerv1alpha1.LedgerServiceConfig) [][]string {
	debug := "false"
	if cfg.Debug {
		debug = pterm.Yellow("true")
	}

	return [][]string{
		{"clusterID", cfg.ClusterID},
		{"bindAddr", cfg.BindAddr},
		{"httpPort", strconv.Itoa(int(cfg.HttpPort))},
		{"grpcPort", strconv.Itoa(int(cfg.GrpcPort))},
		{"walDir", cfg.WalDir},
		{"dataDir", cfg.DataDir},
		{"debug", debug},
		{"restore", strconv.FormatBool(cfg.Restore)},
	}
}

func raftRows(r *ledgerv1alpha1.RaftConfig) [][]string {
	var rows [][]string
	if r.SnapshotThreshold != nil {
		rows = append(rows, []string{"snapshotThreshold", strconv.Itoa(int(*r.SnapshotThreshold))})
	}
	if r.CompactionMargin != nil {
		rows = append(rows, []string{"compactionMargin", strconv.Itoa(int(*r.CompactionMargin))})
	}
	if r.SnapshotInterval != "" {
		rows = append(rows, []string{"snapshotInterval", r.SnapshotInterval})
	}
	if r.ElectionTick != nil {
		rows = append(rows, []string{"electionTick", strconv.Itoa(int(*r.ElectionTick))})
	}
	if r.HeartbeatTick != nil {
		rows = append(rows, []string{"heartbeatTick", strconv.Itoa(int(*r.HeartbeatTick))})
	}
	if r.TickInterval != "" {
		rows = append(rows, []string{"tickInterval", r.TickInterval})
	}
	if r.MaxSizePerMsg != nil {
		rows = append(rows, []string{"maxSizePerMsg", strconv.FormatInt(*r.MaxSizePerMsg, 10)})
	}
	if r.MaxInflightMsgs != nil {
		rows = append(rows, []string{"maxInflightMsgs", strconv.Itoa(int(*r.MaxInflightMsgs))})
	}
	if r.ProposeQueueCapacity != nil {
		rows = append(rows, []string{"proposeQueueCapacity", strconv.Itoa(int(*r.ProposeQueueCapacity))})
	}
	if r.LearnerPromotionThreshold != nil {
		rows = append(rows, []string{"learnerPromotionThreshold", strconv.Itoa(int(*r.LearnerPromotionThreshold))})
	}

	return rows
}

func pebbleRows(p *ledgerv1alpha1.PebbleConfig) [][]string {
	var rows [][]string
	if p.MemTableSize != nil {
		rows = append(rows, []string{"memTableSize", strconv.FormatInt(*p.MemTableSize, 10)})
	}
	if p.MemTableStopWritesThreshold != nil {
		rows = append(rows, []string{"memTableStopWritesThreshold", strconv.Itoa(int(*p.MemTableStopWritesThreshold))})
	}
	if p.L0CompactionThreshold != nil {
		rows = append(rows, []string{"l0CompactionThreshold", strconv.Itoa(int(*p.L0CompactionThreshold))})
	}
	if p.L0StopWritesThreshold != nil {
		rows = append(rows, []string{"l0StopWritesThreshold", strconv.Itoa(int(*p.L0StopWritesThreshold))})
	}
	if p.LBaseMaxBytes != nil {
		rows = append(rows, []string{"lBaseMaxBytes", strconv.FormatInt(*p.LBaseMaxBytes, 10)})
	}
	if p.CacheSize != nil {
		rows = append(rows, []string{"cacheSize", strconv.FormatInt(*p.CacheSize, 10)})
	}
	if p.TargetFileSize != nil {
		rows = append(rows, []string{"targetFileSize", strconv.FormatInt(*p.TargetFileSize, 10)})
	}
	if p.MaxConcurrentCompactions != nil {
		rows = append(rows, []string{"maxConcurrentCompactions", strconv.Itoa(int(*p.MaxConcurrentCompactions))})
	}
	if p.DisableWAL != nil {
		rows = append(rows, []string{"disableWAL", strconv.FormatBool(*p.DisableWAL)})
	}

	return rows
}

func monitoringRows(m *ledgerv1alpha1.MonitoringConfig) [][]string {
	var rows [][]string
	rows = append(rows, []string{"serviceName", m.ServiceName})
	if m.Traces != nil && m.Traces.Enabled != nil {
		rows = append(rows, []string{"traces.enabled", strconv.FormatBool(*m.Traces.Enabled)})
		if m.Traces.Exporter != "" {
			rows = append(rows, []string{"traces.exporter", m.Traces.Exporter})
		}
		if m.Traces.Endpoint != "" {
			rows = append(rows, []string{"traces.endpoint", m.Traces.Endpoint})
		}
	}
	if m.Metrics != nil && m.Metrics.Enabled != nil {
		rows = append(rows, []string{"metrics.enabled", strconv.FormatBool(*m.Metrics.Enabled)})
		if m.Metrics.Exporter != "" {
			rows = append(rows, []string{"metrics.exporter", m.Metrics.Exporter})
		}
	}
	if m.Logs != nil && m.Logs.Enabled != nil {
		rows = append(rows, []string{"logs.enabled", strconv.FormatBool(*m.Logs.Enabled)})
		if m.Logs.Level != "" {
			rows = append(rows, []string{"logs.level", m.Logs.Level})
		}
	}
	if m.Pyroscope != nil && m.Pyroscope.Enabled {
		rows = append(rows, []string{"pyroscope.enabled", "true"})
		if m.Pyroscope.ServerAddress != "" {
			rows = append(rows, []string{"pyroscope.serverAddress", m.Pyroscope.ServerAddress})
		}
	}

	return rows
}

func otherRows(cfg *ledgerv1alpha1.LedgerServiceConfig) [][]string {
	var rows [][]string
	if cfg.Health != nil {
		if cfg.Health.Interval != "" {
			rows = append(rows, []string{"health.interval", cfg.Health.Interval})
		}
		if cfg.Health.WalThreshold != "" {
			rows = append(rows, []string{"health.walThreshold", cfg.Health.WalThreshold})
		}
		if cfg.Health.DataThreshold != "" {
			rows = append(rows, []string{"health.dataThreshold", cfg.Health.DataThreshold})
		}
	}
	if cfg.Audit != nil && cfg.Audit.Enabled != nil {
		rows = append(rows, []string{"audit.enabled", strconv.FormatBool(*cfg.Audit.Enabled)})
	}
	if cfg.ColdStorage != nil && cfg.ColdStorage.Driver != "" {
		rows = append(rows, []string{"coldStorage.driver", cfg.ColdStorage.Driver})
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		rows = append(rows, []string{"tls.enabled", "true"})
		rows = append(rows, []string{"tls.secretName", cfg.TLS.SecretName})
	}
	if cfg.ResponseSigning != nil && cfg.ResponseSigning.Enabled {
		rows = append(rows, []string{"responseSigning.enabled", "true"})
	}
	if cfg.Cache != nil && cfg.Cache.RotationThreshold != nil {
		rows = append(rows, []string{"cache.rotationThreshold", strconv.Itoa(int(*cfg.Cache.RotationThreshold))})
	}
	if cfg.AdmissionMetrics != nil {
		rows = append(rows, []string{"admissionMetrics", strconv.FormatBool(*cfg.AdmissionMetrics)})
	}

	return rows
}
