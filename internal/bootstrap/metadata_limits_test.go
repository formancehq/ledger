package bootstrap

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A mistyped or missing ceiling must fail the boot with the flag named, rather
// than starting a node that admits unbounded metadata.
func TestConfigValidate_MetadataLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:   "defaults are valid",
			mutate: func(*Config) {},
		},
		{
			name:    "zero entries",
			mutate:  func(c *Config) { c.MetadataMaxEntriesPerEntity = 0 },
			wantErr: "--metadata-max-entries",
		},
		{
			name:    "zero key bytes",
			mutate:  func(c *Config) { c.MetadataMaxKeyBytes = 0 },
			wantErr: "--metadata-max-key-bytes",
		},
		{
			name:    "zero value bytes",
			mutate:  func(c *Config) { c.MetadataMaxValueBytes = 0 },
			wantErr: "--metadata-max-value-bytes",
		},
		{
			name:    "zero entity bytes",
			mutate:  func(c *Config) { c.MetadataMaxEntityBytes = 0 },
			wantErr: "--metadata-max-entity-bytes",
		},
		{
			name:    "zero command bytes",
			mutate:  func(c *Config) { c.MetadataMaxCommandBytes = 0 },
			wantErr: "--metadata-max-command-bytes",
		},
		{
			name:    "key ceiling above the entity ceiling",
			mutate:  func(c *Config) { c.MetadataMaxKeyBytes = c.MetadataMaxEntityBytes + 1 },
			wantErr: "must satisfy",
		},
		{
			name:    "value ceiling above the entity ceiling",
			mutate:  func(c *Config) { c.MetadataMaxValueBytes = c.MetadataMaxEntityBytes + 1 },
			wantErr: "must satisfy",
		},
		{
			name:    "entity ceiling above the command ceiling",
			mutate:  func(c *Config) { c.MetadataMaxEntityBytes = c.MetadataMaxCommandBytes + 1 },
			wantErr: "must satisfy",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()

			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The flags are the desired policy this node would propose, so they map onto
// the domain contract one for one.
func TestConfigMetadataLimits(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig()
	require.Equal(t, domain.DefaultMetadataLimits, cfg.MetadataLimits())

	cfg.MetadataMaxValueBytes = 1234
	require.Equal(t, uint64(1234), cfg.MetadataLimits().MaxValueBytes)
}

func commitPolicy(t *testing.T, store *dal.Store, policy *commonpb.ClusterPolicy) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())
}

// The reconciler proposes a policy only when the desired revision exceeds the
// applied one. A policy committed without the metadata ceilings therefore cannot
// be repaired at the same revision, and every business write would be rejected;
// the node refuses to start and names the remedy instead.
func TestValidateCommittedMetadataLimits(t *testing.T) {
	t.Parallel()

	withLimits := func(revision uint64) *commonpb.ClusterPolicy {
		return &commonpb.ClusterPolicy{
			Revision:                    revision,
			QueryCheckpointLimit:        10,
			MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
			MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
			MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
			MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
			MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
		}
	}

	tests := []struct {
		name            string
		policy          *commonpb.ClusterPolicy
		desiredRevision uint64
		wantErr         bool
	}{
		{
			name:            "no policy committed yet",
			policy:          nil,
			desiredRevision: 1,
		},
		{
			name:            "committed policy carries the ceilings",
			policy:          withLimits(3),
			desiredRevision: 3,
		},
		{
			name:            "ceilings missing but a higher revision supersedes it",
			policy:          &commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 10},
			desiredRevision: 3,
		},
		{
			name:            "ceilings missing at the same revision",
			policy:          &commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 10},
			desiredRevision: 2,
			wantErr:         true,
		},
		{
			name:            "ceilings missing and the desired revision is lower",
			policy:          &commonpb.ClusterPolicy{Revision: 5, QueryCheckpointLimit: 10},
			desiredRevision: 2,
			wantErr:         true,
		},
		{
			name: "one ceiling missing is enough to refuse",
			policy: func() *commonpb.ClusterPolicy {
				p := withLimits(4)
				p.MetadataMaxCommandBytes = 0

				return p
			}(),
			desiredRevision: 4,
			wantErr:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			if tc.policy != nil {
				commitPolicy(t, store, tc.policy)
			}

			cfg := validBaseConfig()
			cfg.ClusterPolicyRevision = tc.desiredRevision

			err := validateCommittedMetadataLimits(store, cfg)

			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "--cluster-policy-revision",
				"the failure must name the remedy")
		})
	}
}

// Force may replace identity, but it must not bypass a committed policy that
// this node cannot supersede or persist the new identity on that failed boot.
func TestValidateOrPersistConfig_ForcedIdentityPreservesMetadataGuard(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"node", "cluster"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			for _, revision := range []uint64{1, 2} {
				t.Run(strconv.FormatUint(revision, 10), func(t *testing.T) {
					t.Parallel()
					store := newTestStore(t)
					logger := logging.Testing()
					cfg := validBaseConfig()
					require.NoError(t, ValidateOrPersistConfig(store, cfg, logger, false))
					original, err := LoadPersistedConfig(store)
					require.NoError(t, err)
					commitPolicy(t, store, &commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 10})
					if field == "node" {
						cfg.RaftConfig.NodeID++
					} else {
						cfg.ClusterID += "-changed"
					}
					cfg.ClusterPolicyRevision = revision
					err = ValidateOrPersistConfig(store, cfg, logger, true)
					require.ErrorContains(t, err, "--cluster-policy-revision")
					persisted, err := LoadPersistedConfig(store)
					require.NoError(t, err)
					require.Equal(t, original.GetNodeId(), persisted.GetNodeId())
					require.Equal(t, original.GetClusterId(), persisted.GetClusterId())

					// The same override succeeds once the desired policy can replace the
					// incomplete committed policy, and only then persists the new identity.
					cfg.ClusterPolicyRevision = 3
					require.NoError(t, ValidateOrPersistConfig(store, cfg, logger, true))
					persisted, err = LoadPersistedConfig(store)
					require.NoError(t, err)
					require.Equal(t, cfg.RaftConfig.NodeID, persisted.GetNodeId())
					require.Equal(t, cfg.ClusterID, persisted.GetClusterId())
				})
			}
		})
	}
}
