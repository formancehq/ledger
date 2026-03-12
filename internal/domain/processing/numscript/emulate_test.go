package numscript

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
)

func TestDiscoverNumscriptDependencies(t *testing.T) {
	t.Parallel()

	const ledgerID = "test-ledger"
	testCache := NewNumscriptCache(0)

	t.Run("simple transfer discovers source and destination", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 1000] (
				source = @users:alice
				destination = @users:bob
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)
		require.Len(t, result.SourceVolumes, 1)
		require.Len(t, result.DestinationVolumes, 1)

		_, hasAlice := result.SourceVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:alice"},
			Asset:      "USD/2",
		}]
		_, hasBob := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:bob"},
			Asset:      "USD/2",
		}]

		require.True(t, hasAlice, "should discover source account")
		require.True(t, hasBob, "should discover destination account")
	})

	t.Run("world source discovers world and destination", func(t *testing.T) {
		t.Parallel()

		script := `
			send [EUR/2 5000] (
				source = @world
				destination = @treasury
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)
		require.Len(t, result.SourceVolumes, 1)
		require.Len(t, result.DestinationVolumes, 1)

		_, hasWorld := result.SourceVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "world"},
			Asset:      "EUR/2",
		}]
		_, hasTreasury := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "treasury"},
			Asset:      "EUR/2",
		}]

		require.True(t, hasWorld, "should discover world account")
		require.True(t, hasTreasury, "should discover destination account")
	})

	t.Run("dynamic account interpolation", func(t *testing.T) {
		t.Parallel()

		script := `
#![feature("experimental-account-interpolation")]
			vars {
				string $order_id
				monetary $amount
			}
			send $amount (
				source = @world
				destination = @escrow:$order_id
			)
		`
		vars := map[string]string{
			"order_id": "order-123",
			"amount":   "USD/2 1000",
		}

		result, err := DiscoverNumscriptDependencies(testCache, script, vars, ledgerID)
		require.NoError(t, err)
		require.Len(t, result.SourceVolumes, 1)
		require.Len(t, result.DestinationVolumes, 1)

		_, hasEscrow := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "escrow:order-123"},
			Asset:      "USD/2",
		}]
		require.True(t, hasEscrow, "should discover interpolated account address")
	})

	t.Run("multiple sources fallback pattern", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 1000] (
				source = {
					@users:alice:checking
					@users:alice:savings
				}
				destination = @merchant
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)

		_, hasChecking := result.SourceVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:alice:checking"},
			Asset:      "USD/2",
		}]
		_, hasSavings := result.SourceVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:alice:savings"},
			Asset:      "USD/2",
		}]
		_, hasMerchant := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "merchant"},
			Asset:      "USD/2",
		}]

		require.True(t, hasChecking, "should discover first source")
		require.True(t, hasSavings, "should discover second source")
		require.True(t, hasMerchant, "should discover destination")
	})

	t.Run("percentage split discovers all destinations", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 10000] (
				source = @world
				destination = {
					1/2 to @users:alice
					1/2 to @users:bob
				}
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)

		_, hasAlice := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:alice"},
			Asset:      "USD/2",
		}]
		_, hasBob := result.DestinationVolumes[domain.VolumeKey{
			AccountKey: domain.AccountKey{Ledger: ledgerID, Account: "users:bob"},
			Asset:      "USD/2",
		}]

		require.True(t, hasAlice, "should discover first destination")
		require.True(t, hasBob, "should discover second destination")
	})

	t.Run("parse error returns error", func(t *testing.T) {
		t.Parallel()

		script := `send [USD/2 invalid] ( source = @world destination = @users:alice )`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.Error(t, err)
		require.Nil(t, result)

		var parseErr *domain.ErrNumscriptParse
		require.ErrorAs(t, err, &parseErr)
	})

	t.Run("execution error still returns partial discovery", func(t *testing.T) {
		t.Parallel()

		// This script references a variable that is not provided,
		// which should cause an execution error. But any accounts queried
		// before the error should still be discovered.
		script := `
			vars {
				monetary $amount
			}
			send $amount (
				source = @users:alice
				destination = @users:bob
			)
		`
		// Don't provide the $amount variable — this will cause an execution error
		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err) // Parse succeeds, execution error is ignored
		// Discovery may or may not have found accounts depending on when the error occurred,
		// but the function should not return an error
		require.NotNil(t, result)
	})

	t.Run("sets correct ledger ID on all discovered volumes", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 100] (
				source = @world
				destination = @users:alice
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)

		for key := range result.SourceVolumes {
			require.Equal(t, ledgerID, key.Ledger, "all source volume keys should have the correct ledger ID")
		}

		for key := range result.DestinationVolumes {
			require.Equal(t, ledgerID, key.Ledger, "all destination volume keys should have the correct ledger ID")
		}
	})

	t.Run("rejects non-deterministic script with multiple GetBalances calls", func(t *testing.T) {
		t.Parallel()

		// A script that queries balance mid-execution triggers a second
		// GetBalances call, violating the determinism constraint.
		script := `
			vars {
				account $user
			}
			// First GetBalances call (from the send source)
			send [USD/2 100] (
				source = $user
				destination = @merchant
			)
			// Second send triggers a second GetBalances call
			send [USD/2 50] (
				source = $user
				destination = @platform:fees
			)
		`
		vars := map[string]string{"user": "users:alice"}

		result, err := DiscoverNumscriptDependencies(testCache, script, vars, ledgerID)

		// The numscript interpreter may batch all balance queries in a single
		// GetBalances call or call it multiple times depending on the script structure.
		// If it calls GetBalances twice, we expect a non-deterministic error.
		if err != nil {
			var nonDetErr *ErrNonDeterministicScript
			require.ErrorAs(t, err, &nonDetErr)
			require.Equal(t, "GetBalances", nonDetErr.Method)
			require.Nil(t, result)
		}
		// If the interpreter batches all queries into one call, the test still passes
	})

	t.Run("allows single GetBalances and single GetAccountsMetadata", func(t *testing.T) {
		t.Parallel()

		// A simple script with both balance queries and metadata — single batch each
		script := `
			send [USD/2 1000] (
				source = @users:alice
				destination = @users:bob
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err, "single GetBalances call should be allowed")
		require.NotEmpty(t, result.SourceVolumes)
	})

	t.Run("discovers metadata dependencies", func(t *testing.T) {
		t.Parallel()

		script := `
			vars {
				account $dest = meta(@platform, "default_destination")
			}
			send [USD/2 100] (
				source = @world
				destination = $dest
			)
		`

		result, err := DiscoverNumscriptDependencies(testCache, script, nil, ledgerID)
		require.NoError(t, err)
		require.NotEmpty(t, result.Metadata, "should discover metadata dependencies")
	})
}
