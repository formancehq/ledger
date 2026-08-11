package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/features"
)

func TestFeatures(t *testing.T) {
	f := features.MinimalFeatureSet.With(features.FeatureMovesHistory, "DISABLED")
	require.Equal(t, "DISABLED", f[features.FeatureMovesHistory])
	require.Equal(t, "AMH=DISABLED,HL=DISABLED,MH=DISABLED,MHPCEV=DISABLED,TMH=DISABLED", f.String())
}

func TestNewLedger(t *testing.T) {
	t.Run("valid name", func(t *testing.T) {
		l, err := New("my-ledger", NewDefaultConfiguration())
		require.NoError(t, err)
		require.Equal(t, "my-ledger", l.Name)
		require.Equal(t, StateInitializing, l.State)
		require.Equal(t, DefaultBucket, l.Bucket)
	})

	t.Run("invalid name", func(t *testing.T) {
		_, err := New("invalid ledger!", NewDefaultConfiguration())
		require.Error(t, err)
	})

	t.Run("reserved name", func(t *testing.T) {
		_, err := New("_info", NewDefaultConfiguration())
		require.Error(t, err)
	})
}

func TestConfiguration_SetDefaults(t *testing.T) {
	cfg := Configuration{}
	cfg.SetDefaults()
	require.Equal(t, DefaultBucket, cfg.Bucket)
	require.NotNil(t, cfg.Features)
	require.Equal(t, "ON", cfg.Features[features.FeatureMovesHistory])
}

func TestLedger_HasFeature(t *testing.T) {
	l := MustNewWithDefault("test")
	require.True(t, l.HasFeature(features.FeatureMovesHistory, "ON"))
	require.False(t, l.HasFeature(features.FeatureMovesHistory, "OFF"))
}

func TestLedger_WithMetadata(t *testing.T) {
	l := MustNewWithDefault("test")
	l2 := l.WithMetadata(map[string]string{"key": "value"})
	require.Equal(t, "value", l2.Metadata["key"])
}

func TestGetIndexedMetadataKeys(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{}}}
		require.Nil(t, l.GetIndexedMetadataKeys())
	})
	t.Run("single key", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{
			features.FeatureIndexedMetadataKeys: "source_wallet_id",
		}}}
		require.Equal(t, []string{"source_wallet_id"}, l.GetIndexedMetadataKeys())
	})
	t.Run("multiple keys", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{
			features.FeatureIndexedMetadataKeys: "source_wallet_id,destination_wallet_id",
		}}}
		require.Equal(t, []string{"source_wallet_id", "destination_wallet_id"}, l.GetIndexedMetadataKeys())
	})
}

func TestGetIndexedMetadataKeys_DropsInvalidKeys(t *testing.T) {
	// Feature values can reach the database without passing the API validation
	// (the operator guide documents a direct UPDATE on _system.ledgers), and these
	// keys are embedded as SQL literals — so they are re-checked on read.
	for _, tc := range []struct {
		name     string
		value    string
		expected []string
	}{
		{"all valid", "source_wallet_id,dest_id", []string{"source_wallet_id", "dest_id"}},
		{"drops invalid, keeps valid", "source_wallet_id,bad-key", []string{"source_wallet_id"}},
		{"drops injection attempt", "ok_key,x'); drop table transactions; --", []string{"ok_key"}},
		{"all invalid", "bad-key,another bad", nil},
		{"empty element", "source_wallet_id,", []string{"source_wallet_id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := Ledger{Configuration: Configuration{Features: features.FeatureSet{
				features.FeatureIndexedMetadataKeys: tc.value,
			}}}
			require.Equal(t, tc.expected, l.GetIndexedMetadataKeys())
		})
	}
}

func TestLedger_HasFeature_InvalidFeaturePanics(t *testing.T) {
	l := MustNewWithDefault("test")

	t.Run("unknown feature", func(t *testing.T) {
		require.Panics(t, func() {
			l.HasFeature("DOES_NOT_EXIST", "ON")
		})
	})

	t.Run("invalid value for known feature", func(t *testing.T) {
		require.Panics(t, func() {
			l.HasFeature(features.FeatureMovesHistory, "NOT_A_VALUE")
		})
	})

	t.Run("open-ended feature accepts any syntactically valid value", func(t *testing.T) {
		require.NotPanics(t, func() {
			l.HasFeature(features.FeatureIndexedMetadataKeys, "source_wallet_id")
		})
	})
}

func TestMustNewWithDefault_PanicsOnInvalidName(t *testing.T) {
	require.Panics(t, func() {
		MustNewWithDefault("invalid name!")
	})
}

func TestNewLedger_RejectsInvalidBucketName(t *testing.T) {
	cfg := NewDefaultConfiguration()
	cfg.Bucket = "not a valid bucket!"

	_, err := New("my-ledger", cfg)
	require.Error(t, err)
	require.ErrorAs(t, err, &ErrInvalidBucketName{})
}

func TestConfiguration_Validate(t *testing.T) {
	t.Run("valid features", func(t *testing.T) {
		cfg := NewDefaultConfiguration()
		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid feature value is rejected", func(t *testing.T) {
		cfg := Configuration{Features: features.FeatureSet{
			features.FeatureMovesHistory: "NOT_A_VALUE",
		}}
		require.Error(t, cfg.Validate())
	})

	t.Run("invalid indexed-metadata key is rejected", func(t *testing.T) {
		cfg := Configuration{Features: features.FeatureSet{
			features.FeatureIndexedMetadataKeys: "bad-key",
		}}
		require.Error(t, cfg.Validate())
	})

	t.Run("New propagates configuration validation failure", func(t *testing.T) {
		_, err := New("my-ledger", Configuration{
			Bucket:   DefaultBucket,
			Features: features.FeatureSet{features.FeatureHashLogs: "NOPE"},
		})
		require.Error(t, err)
	})
}

func TestSetDefaults_PreservesExplicitValues(t *testing.T) {
	cfg := Configuration{
		Bucket:   "custom",
		Features: features.FeatureSet{features.FeatureHashLogs: "DISABLED"},
	}
	cfg.SetDefaults()

	require.Equal(t, "custom", cfg.Bucket, "explicit bucket must be preserved")
	require.Equal(t, "DISABLED", cfg.Features[features.FeatureHashLogs],
		"explicit feature value must not be overwritten by the default")
	require.Equal(t, "ON", cfg.Features[features.FeatureMovesHistory],
		"unset features must be filled from DefaultFeatures")
}
