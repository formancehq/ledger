//go:build it

package provisionner

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/testserver"
)

func TestReconciler(t *testing.T) {
	t.Parallel()

	dockerPool := docker.NewPool(t, logging.Testing())
	pgServer := pgtesting.CreatePostgresServer(t, dockerPool)

	type step struct {
		cfg           Config
		expectedState State
	}

	type testCase struct {
		name  string
		steps []step
	}
	for _, tc := range []testCase{
		{
			name: "nominal",
			steps: []step{{
				cfg: Config{},
				expectedState: State{
					Ledgers:   map[string]*LedgerState{},
					Exporters: map[string]*ExporterState{},
				},
			}},
		},
		{
			name: "with a feature config",
			steps: []step{{
				cfg: Config{
					Ledgers: map[string]LedgerConfig{
						"ledger1": {
							LedgerCreateConfig: LedgerCreateConfig{
								Features: map[string]string{
									"HASH_LOGS": "DISABLED",
								},
							},
						},
					},
				},
				expectedState: State{
					Exporters: map[string]*ExporterState{},
					Ledgers: map[string]*LedgerState{
						"ledger1": {
							Exporters: map[string]string{},
							Config: LedgerConfig{
								LedgerCreateConfig: LedgerCreateConfig{
									Features: map[string]string{
										"HASH_LOGS": "DISABLED",
									},
								},
							},
						},
					},
				},
			}},
		},
		{
			name: "3 ledgers",
			steps: []step{{
				cfg: Config{
					Ledgers: map[string]LedgerConfig{
						"ledger1": {},
						"ledger2": {},
						"ledger3": {},
					},
				},
				expectedState: State{
					Exporters: map[string]*ExporterState{},
					Ledgers: map[string]*LedgerState{
						"ledger1": {
							Exporters: map[string]string{},
						},
						"ledger2": {
							Exporters: map[string]string{},
						},
						"ledger3": {
							Exporters: map[string]string{},
						},
					},
				},
			}},
		},
		{
			name: "2 exporters",
			steps: []step{{
				cfg: Config{
					Exporters: map[string]ExporterConfig{
						"clickhouse1": {
							Driver: "clickhouse",
							Config: map[string]any{
								"dsn": "clickhouse://srv1:8123",
							},
						},
						"clickhouse2": {
							Driver: "clickhouse",
							Config: map[string]any{
								"dsn": "clickhouse://srv2:8123",
							},
						},
					},
				},
				expectedState: State{
					Exporters: map[string]*ExporterState{
						"clickhouse1": {
							Config: ExporterConfig{
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						"clickhouse2": {
							Config: ExporterConfig{
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv2:8123",
								},
							},
						},
					},
					Ledgers: map[string]*LedgerState{},
				},
			}},
		},
		{
			name: "1 exporter and a ledger bounded to it",
			steps: []step{{
				cfg: Config{
					Exporters: map[string]ExporterConfig{
						"clickhouse1": {
							Driver: "clickhouse",
							Config: map[string]any{
								"dsn": "clickhouse://srv1:8123",
							},
						},
					},
					Ledgers: map[string]LedgerConfig{
						"ledger1": {
							Exporters: []string{"clickhouse1"},
						},
					},
				},
				expectedState: State{
					Exporters: map[string]*ExporterState{
						"clickhouse1": {
							Config: ExporterConfig{
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
					},
					Ledgers: map[string]*LedgerState{
						"ledger1": {
							Config: LedgerConfig{
								Exporters: []string{"clickhouse1"},
							},
							Exporters: map[string]string{
								"clickhouse1": "",
							},
						},
					},
				},
			}},
		},
		{
			name: "removing exporter binding",
			steps: []step{
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Exporters: []string{"clickhouse1"},
							},
						},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{
							"clickhouse1": {
								Config: ExporterConfig{
									Driver: "clickhouse",
									Config: map[string]any{
										"dsn": "clickhouse://srv1:8123",
									},
								},
							},
						},
						Ledgers: map[string]*LedgerState{
							"ledger1": {
								Config: LedgerConfig{
									Exporters: []string{"clickhouse1"},
								},
								Exporters: map[string]string{
									"clickhouse1": "",
								},
							},
						},
					},
				},
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {},
						},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{
							"clickhouse1": {
								Config: ExporterConfig{
									Driver: "clickhouse",
									Config: map[string]any{
										"dsn": "clickhouse://srv1:8123",
									},
								},
							},
						},
						Ledgers: map[string]*LedgerState{
							"ledger1": {
								Config: LedgerConfig{
									Exporters: []string{},
								},
								Exporters: map[string]string{},
							},
						},
					},
				},
			},
		},
		{
			name: "removing exporter without binding",
			steps: []step{
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{
							"clickhouse1": {
								Config: ExporterConfig{
									Driver: "clickhouse",
									Config: map[string]any{
										"dsn": "clickhouse://srv1:8123",
									},
								},
							},
						},
						Ledgers: map[string]*LedgerState{},
					},
				},
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{},
						Ledgers:   map[string]LedgerConfig{},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{},
						Ledgers:   map[string]*LedgerState{},
					},
				},
			},
		},
		{
			name: "removing exporter with binding",
			steps: []step{
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Exporters: []string{"clickhouse1"},
							},
						},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{
							"clickhouse1": {
								Config: ExporterConfig{
									Driver: "clickhouse",
									Config: map[string]any{
										"dsn": "clickhouse://srv1:8123",
									},
								},
							},
						},
						Ledgers: map[string]*LedgerState{
							"ledger1": {
								Config: LedgerConfig{
									Exporters: []string{"clickhouse1"},
								},
								Exporters: map[string]string{
									"clickhouse1": "",
								},
							},
						},
					},
				},
				{
					cfg: Config{
						Exporters: map[string]ExporterConfig{},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Exporters: []string{},
							},
						},
					},
					expectedState: State{
						Exporters: map[string]*ExporterState{},
						Ledgers: map[string]*LedgerState{
							"ledger1": {
								Config: LedgerConfig{
									Exporters: []string{},
								},
								Exporters: map[string]string{},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := pgServer.NewDatabase(t)

			srv := testserver.NewTestServer(deferred.FromValue(db.ConnectionOptions()),
				testservice.WithInstruments(
					testservice.DebugInstrumentation(os.Getenv("DEBUG") == "true"),
					testserver.ExperimentalExportersInstrumentation(),
					testserver.ExperimentalFeaturesInstrumentation(),
				),
			)

			store := NewInMemoryStore()
			r := NewReconciler(store, testserver.Client(srv))

			for _, step := range tc.steps {
				require.NoError(t, r.Reconcile(logging.TestingContext(), step.cfg))

				storedState := store.state
				expectedState := step.expectedState
				for exporter := range storedState.Exporters {
					if _, ok := expectedState.Exporters[exporter]; ok {
						expectedState.Exporters[exporter].ID = storedState.Exporters[exporter].ID
					}
				}

				for ledgerName, ledgerState := range storedState.Ledgers {
					for exporterName, pipelineID := range ledgerState.Exporters {
						if expectedLedgerState, ok := expectedState.Ledgers[ledgerName]; ok {
							if _, ok := expectedLedgerState.Exporters[exporterName]; ok {
								expectedLedgerState.Exporters[exporterName] = pipelineID
							}
						}
					}
				}

				require.EqualValues(t, expectedState, storedState)
			}
		})
	}
}
