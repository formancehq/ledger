package config

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

func newViewCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "view <name>",
		Short: "Pretty-print Ledger configuration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runView(cmd, opts, args[0])
		},
	}
}

func runView(cmd *cobra.Command, opts *cmdutil.Options, name string) error {
	ctx := cmd.Context()

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
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

func coreRows(cfg *ledgerv1alpha1.LedgerConfig) [][]string {
	return [][]string{
		{"clusterID", cfg.ClusterID},
		{"bindAddr", cfg.BindAddr},
		{"httpPort", fmt.Sprintf("%d", cfg.HttpPort)},
		{"grpcPort", fmt.Sprintf("%d", cfg.GrpcPort)},
		{"walDir", cfg.WalDir},
		{"dataDir", cfg.DataDir},
		{"debug", fmt.Sprintf("%t", cfg.Debug)},
		{"restore", fmt.Sprintf("%t", cfg.Restore)},
	}
}

func raftRows(r *ledgerv1alpha1.RaftConfig) [][]string {
	var rows [][]string
	if r.SnapshotThreshold != nil {
		rows = append(rows, []string{"snapshotThreshold", fmt.Sprintf("%d", *r.SnapshotThreshold)})
	}
	if r.CompactionMargin != nil {
		rows = append(rows, []string{"compactionMargin", fmt.Sprintf("%d", *r.CompactionMargin)})
	}
	if r.SnapshotInterval != "" {
		rows = append(rows, []string{"snapshotInterval", r.SnapshotInterval})
	}
	if r.ElectionTick != nil {
		rows = append(rows, []string{"electionTick", fmt.Sprintf("%d", *r.ElectionTick)})
	}
	if r.HeartbeatTick != nil {
		rows = append(rows, []string{"heartbeatTick", fmt.Sprintf("%d", *r.HeartbeatTick)})
	}
	if r.TickInterval != "" {
		rows = append(rows, []string{"tickInterval", r.TickInterval})
	}
	if r.MaxSizePerMsg != nil {
		rows = append(rows, []string{"maxSizePerMsg", fmt.Sprintf("%d", *r.MaxSizePerMsg)})
	}
	if r.MaxInflightMsgs != nil {
		rows = append(rows, []string{"maxInflightMsgs", fmt.Sprintf("%d", *r.MaxInflightMsgs)})
	}
	if r.ProposeQueueCapacity != nil {
		rows = append(rows, []string{"proposeQueueCapacity", fmt.Sprintf("%d", *r.ProposeQueueCapacity)})
	}
	if r.LearnerPromotionThreshold != nil {
		rows = append(rows, []string{"learnerPromotionThreshold", fmt.Sprintf("%d", *r.LearnerPromotionThreshold)})
	}
	return rows
}

func pebbleRows(p *ledgerv1alpha1.PebbleConfig) [][]string {
	var rows [][]string
	if p.MemTableSize != nil {
		rows = append(rows, []string{"memTableSize", fmt.Sprintf("%d", *p.MemTableSize)})
	}
	if p.MemTableStopWritesThreshold != nil {
		rows = append(rows, []string{"memTableStopWritesThreshold", fmt.Sprintf("%d", *p.MemTableStopWritesThreshold)})
	}
	if p.L0CompactionThreshold != nil {
		rows = append(rows, []string{"l0CompactionThreshold", fmt.Sprintf("%d", *p.L0CompactionThreshold)})
	}
	if p.L0StopWritesThreshold != nil {
		rows = append(rows, []string{"l0StopWritesThreshold", fmt.Sprintf("%d", *p.L0StopWritesThreshold)})
	}
	if p.LBaseMaxBytes != nil {
		rows = append(rows, []string{"lBaseMaxBytes", fmt.Sprintf("%d", *p.LBaseMaxBytes)})
	}
	if p.CacheSize != nil {
		rows = append(rows, []string{"cacheSize", fmt.Sprintf("%d", *p.CacheSize)})
	}
	if p.TargetFileSize != nil {
		rows = append(rows, []string{"targetFileSize", fmt.Sprintf("%d", *p.TargetFileSize)})
	}
	if p.MaxConcurrentCompactions != nil {
		rows = append(rows, []string{"maxConcurrentCompactions", fmt.Sprintf("%d", *p.MaxConcurrentCompactions)})
	}
	if p.DisableWAL != nil {
		rows = append(rows, []string{"disableWAL", fmt.Sprintf("%t", *p.DisableWAL)})
	}
	return rows
}

func monitoringRows(m *ledgerv1alpha1.MonitoringConfig) [][]string {
	var rows [][]string
	rows = append(rows, []string{"serviceName", m.ServiceName})
	if m.Traces != nil && m.Traces.Enabled != nil {
		rows = append(rows, []string{"traces.enabled", fmt.Sprintf("%t", *m.Traces.Enabled)})
		if m.Traces.Exporter != "" {
			rows = append(rows, []string{"traces.exporter", m.Traces.Exporter})
		}
		if m.Traces.Endpoint != "" {
			rows = append(rows, []string{"traces.endpoint", m.Traces.Endpoint})
		}
	}
	if m.Metrics != nil && m.Metrics.Enabled != nil {
		rows = append(rows, []string{"metrics.enabled", fmt.Sprintf("%t", *m.Metrics.Enabled)})
		if m.Metrics.Exporter != "" {
			rows = append(rows, []string{"metrics.exporter", m.Metrics.Exporter})
		}
	}
	if m.Logs != nil && m.Logs.Enabled != nil {
		rows = append(rows, []string{"logs.enabled", fmt.Sprintf("%t", *m.Logs.Enabled)})
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

func otherRows(cfg *ledgerv1alpha1.LedgerConfig) [][]string {
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
		rows = append(rows, []string{"audit.enabled", fmt.Sprintf("%t", *cfg.Audit.Enabled)})
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
		rows = append(rows, []string{"cache.rotationThreshold", fmt.Sprintf("%d", *cfg.Cache.RotationThreshold)})
	}
	if cfg.AdmissionMetrics != nil {
		rows = append(rows, []string{"admissionMetrics", fmt.Sprintf("%t", *cfg.AdmissionMetrics)})
	}
	return rows
}
