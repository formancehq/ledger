//go:build it

package provisionner

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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
					Ledgers:    map[string]*LedgerState{},
					Connectors: map[string]*ConnectorState{},
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
					Connectors: map[string]*ConnectorState{},
					Ledgers: map[string]*LedgerState{
						"ledger1": {
							Connectors: map[string]string{},
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
					Connectors: map[string]*ConnectorState{},
					Ledgers: map[string]*LedgerState{
						"ledger1": {
							Connectors: map[string]string{},
						},
						"ledger2": {
							Connectors: map[string]string{},
						},
						"ledger3": {
							Connectors: map[string]string{},
						},
					},
				},
			}},
		},
		{
			name: "2 connectors",
			steps: []step{{
				cfg: Config{
					Connectors: map[string]ConnectorConfig{
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
					Connectors: map[string]*ConnectorState{
						"clickhouse1": {
							Config: ConnectorConfig{
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						"clickhouse2": {
							Config: ConnectorConfig{
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
			name: "1 connector and a ledger bounded to it",
			steps: []step{{
				cfg: Config{
					Connectors: map[string]ConnectorConfig{
						"clickhouse1": {
							Driver: "clickhouse",
							Config: map[string]any{
								"dsn": "clickhouse://srv1:8123",
							},
						},
					},
					Ledgers: map[string]LedgerConfig{
						"ledger1": {
							Connectors: []string{"clickhouse1"},
						},
					},
				},
				expectedState: State{
					Connectors: map[string]*ConnectorState{
						"clickhouse1": {
							Config: ConnectorConfig{
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
								Connectors: []string{"clickhouse1"},
							},
							Connectors: map[string]string{
								"clickhouse1": "",
							},
						},
					},
				},
			}},
		},
		{
			name: "removing connector binding",
			steps: []step{
				{
					cfg: Config{
						Connectors: map[string]ConnectorConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Connectors: []string{"clickhouse1"},
							},
						},
					},
					expectedState: State{
						Connectors: map[string]*ConnectorState{
							"clickhouse1": {
								Config: ConnectorConfig{
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
									Connectors: []string{"clickhouse1"},
								},
								Connectors: map[string]string{
									"clickhouse1": "",
								},
							},
						},
					},
				},
				{
					cfg: Config{
						Connectors: map[string]ConnectorConfig{
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
						Connectors: map[string]*ConnectorState{
							"clickhouse1": {
								Config: ConnectorConfig{
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
									Connectors: []string{},
								},
								Connectors: map[string]string{},
							},
						},
					},
				},
			},
		},
		{
			name: "removing connector without binding",
			steps: []step{
				{
					cfg: Config{
						Connectors: map[string]ConnectorConfig{
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
						Connectors: map[string]*ConnectorState{
							"clickhouse1": {
								Config: ConnectorConfig{
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
						Connectors: map[string]ConnectorConfig{},
						Ledgers:    map[string]LedgerConfig{},
					},
					expectedState: State{
						Connectors: map[string]*ConnectorState{},
						Ledgers:    map[string]*LedgerState{},
					},
				},
			},
		},
		{
			name: "removing connector with binding",
			steps: []step{
				{
					cfg: Config{
						Connectors: map[string]ConnectorConfig{
							"clickhouse1": {
								Driver: "clickhouse",
								Config: map[string]any{
									"dsn": "clickhouse://srv1:8123",
								},
							},
						},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Connectors: []string{"clickhouse1"},
							},
						},
					},
					expectedState: State{
						Connectors: map[string]*ConnectorState{
							"clickhouse1": {
								Config: ConnectorConfig{
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
									Connectors: []string{"clickhouse1"},
								},
								Connectors: map[string]string{
									"clickhouse1": "",
								},
							},
						},
					},
				},
				{
					cfg: Config{
						Connectors: map[string]ConnectorConfig{},
						Ledgers: map[string]LedgerConfig{
							"ledger1": {
								Connectors: []string{},
							},
						},
					},
					expectedState: State{
						Connectors: map[string]*ConnectorState{},
						Ledgers: map[string]*LedgerState{
							"ledger1": {
								Config: LedgerConfig{
									Connectors: []string{},
								},
								Connectors: map[string]string{},
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
					testserver.ExperimentalConnectorsInstrumentation(),
					testserver.ExperimentalFeaturesInstrumentation(),
				),
			)

			store := NewInMemoryStore()
			r := NewReconciler(store, testserver.Client(srv))

			for _, step := range tc.steps {
				require.NoError(t, r.Reconcile(logging.TestingContext(), step.cfg))

				storedState := store.state
				expectedState := step.expectedState
				for connector := range storedState.Connectors {
					if _, ok := expectedState.Connectors[connector]; ok {
						expectedState.Connectors[connector].ID = storedState.Connectors[connector].ID
					}
				}

				for ledgerName, ledgerState := range storedState.Ledgers {
					for connectorName, pipelineID := range ledgerState.Connectors {
						if expectedLedgerState, ok := expectedState.Ledgers[ledgerName]; ok {
							if _, ok := expectedLedgerState.Connectors[connectorName]; ok {
								expectedLedgerState.Connectors[connectorName] = pipelineID
							}
						}
					}
				}

				require.EqualValues(t, expectedState, storedState)
			}
		})
	}
}
