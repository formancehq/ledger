package ledger

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPostCommitVolumes_AddInput_WithExistingAccount verifies normal behavior
func TestPostCommitVolumes_AddInput_WithExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewVolumesInt64(100, 50),
		},
	}

	volumes.AddInput("existing_account", "USD", big.NewInt(100))

	require.Equal(t, big.NewInt(200), volumes["existing_account"]["USD"].Input)
	require.Equal(t, big.NewInt(50), volumes["existing_account"]["USD"].Output)
}

// TestPostCommitVolumes_AddInput_WithNonExistingAccount verifies that AddInput
// creates the account if it doesn't exist (fix for "assignment to entry in nil map")
func TestPostCommitVolumes_AddInput_WithNonExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewEmptyVolumes(),
		},
	}

	// Should NOT panic - account should be created automatically
	require.NotPanics(t, func() {
		volumes.AddInput("new_account", "USD", big.NewInt(100))
	})

	// Verify the account and asset were created with correct values
	require.Contains(t, volumes, "new_account")
	require.Contains(t, volumes["new_account"], "USD")
	require.Equal(t, big.NewInt(100), volumes["new_account"]["USD"].Input)
	require.Equal(t, big.NewInt(0), volumes["new_account"]["USD"].Output)
}

// TestPostCommitVolumes_AddOutput_WithExistingAccount verifies normal behavior
func TestPostCommitVolumes_AddOutput_WithExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewVolumesInt64(100, 50),
		},
	}

	volumes.AddOutput("existing_account", "USD", big.NewInt(25))

	require.Equal(t, big.NewInt(100), volumes["existing_account"]["USD"].Input)
	require.Equal(t, big.NewInt(75), volumes["existing_account"]["USD"].Output)
}

// TestPostCommitVolumes_AddOutput_WithNonExistingAccount verifies that AddOutput
// creates the account if it doesn't exist (fix for "assignment to entry in nil map")
func TestPostCommitVolumes_AddOutput_WithNonExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewEmptyVolumes(),
		},
	}

	// Should NOT panic - account should be created automatically
	require.NotPanics(t, func() {
		volumes.AddOutput("new_account", "USD", big.NewInt(100))
	})

	// Verify the account and asset were created with correct values
	require.Contains(t, volumes, "new_account")
	require.Contains(t, volumes["new_account"], "USD")
	require.Equal(t, big.NewInt(0), volumes["new_account"]["USD"].Input)
	require.Equal(t, big.NewInt(100), volumes["new_account"]["USD"].Output)
}

// TestPostCommitVolumes_AddInput_WithNonExistingAsset verifies that AddInput
// creates the asset if it doesn't exist for an existing account
func TestPostCommitVolumes_AddInput_WithNonExistingAsset(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewEmptyVolumes(),
		},
	}

	// Should NOT panic - asset should be created automatically
	require.NotPanics(t, func() {
		volumes.AddInput("existing_account", "EUR", big.NewInt(100))
	})

	// Verify the asset was created with correct values
	require.Contains(t, volumes["existing_account"], "EUR")
	require.Equal(t, big.NewInt(100), volumes["existing_account"]["EUR"].Input)
	require.Equal(t, big.NewInt(0), volumes["existing_account"]["EUR"].Output)
}

// TestPostCommitVolumes_AddOutput_WithNonExistingAsset verifies that AddOutput
// creates the asset if it doesn't exist for an existing account
func TestPostCommitVolumes_AddOutput_WithNonExistingAsset(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewEmptyVolumes(),
		},
	}

	// Should NOT panic - asset should be created automatically
	require.NotPanics(t, func() {
		volumes.AddOutput("existing_account", "EUR", big.NewInt(100))
	})

	// Verify the asset was created with correct values
	require.Contains(t, volumes["existing_account"], "EUR")
	require.Equal(t, big.NewInt(0), volumes["existing_account"]["EUR"].Input)
	require.Equal(t, big.NewInt(100), volumes["existing_account"]["EUR"].Output)
}

// TestPostCommitVolumes_AddInput_OnEmptyMap verifies that AddInput works on empty map
func TestPostCommitVolumes_AddInput_OnEmptyMap(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{}

	// Should NOT panic - account and asset should be created automatically
	require.NotPanics(t, func() {
		volumes.AddInput("any_account", "USD", big.NewInt(100))
	})

	// Verify the account and asset were created
	require.Contains(t, volumes, "any_account")
	require.Contains(t, volumes["any_account"], "USD")
	require.Equal(t, big.NewInt(100), volumes["any_account"]["USD"].Input)
}

// TestPostCommitVolumes_AddOutput_OnEmptyMap verifies that AddOutput works on empty map
func TestPostCommitVolumes_AddOutput_OnEmptyMap(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{}

	// Should NOT panic - account and asset should be created automatically
	require.NotPanics(t, func() {
		volumes.AddOutput("any_account", "USD", big.NewInt(100))
	})

	// Verify the account and asset were created
	require.Contains(t, volumes, "any_account")
	require.Contains(t, volumes["any_account"], "USD")
	require.Equal(t, big.NewInt(100), volumes["any_account"]["USD"].Output)
}

// TestPostCommitVolumes_Get_WithExistingAccount verifies Get returns existing volumes
func TestPostCommitVolumes_Get_WithExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewVolumesInt64(100, 50),
		},
	}

	result := volumes.Get("existing_account", "USD")

	require.Equal(t, big.NewInt(100), result.Input)
	require.Equal(t, big.NewInt(50), result.Output)
}

// TestPostCommitVolumes_Get_WithNonExistingAccount verifies Get creates empty volumes
func TestPostCommitVolumes_Get_WithNonExistingAccount(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{
		"existing_account": {
			"USD": NewEmptyVolumes(),
		},
	}

	// Should NOT panic - account should be created automatically
	var result Volumes
	require.NotPanics(t, func() {
		result = volumes.Get("new_account", "USD")
	})

	// Verify empty volumes are returned
	require.Equal(t, big.NewInt(0), result.Input)
	require.Equal(t, big.NewInt(0), result.Output)

	// Verify the account and asset were created in the map
	require.Contains(t, volumes, "new_account")
	require.Contains(t, volumes["new_account"], "USD")
}

// TestPostCommitVolumes_Get_OnEmptyMap verifies Get works on empty map
func TestPostCommitVolumes_Get_OnEmptyMap(t *testing.T) {
	t.Parallel()

	volumes := PostCommitVolumes{}

	// Should NOT panic - account and asset should be created automatically
	var result Volumes
	require.NotPanics(t, func() {
		result = volumes.Get("any_account", "USD")
	})

	// Verify empty volumes are returned
	require.Equal(t, big.NewInt(0), result.Input)
	require.Equal(t, big.NewInt(0), result.Output)

	// Verify the account and asset were created
	require.Contains(t, volumes, "any_account")
	require.Contains(t, volumes["any_account"], "USD")
}
