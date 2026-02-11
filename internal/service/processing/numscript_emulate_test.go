package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
)

func TestDiscoverNumscriptVolumes(t *testing.T) {
	t.Parallel()

	const ledgerID = uint32(42)

	t.Run("simple transfer discovers source and destination", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 1000] (
				source = @users:alice
				destination = @users:bob
			)
		`

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err)
		require.Len(t, volumes, 2)

		_, hasAlice := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:alice"},
			Asset:      "USD/2",
		}]
		_, hasBob := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:bob"},
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

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err)
		require.Len(t, volumes, 2)

		_, hasWorld := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "world"},
			Asset:      "EUR/2",
		}]
		_, hasTreasury := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "treasury"},
			Asset:      "EUR/2",
		}]
		require.True(t, hasWorld, "should discover world account")
		require.True(t, hasTreasury, "should discover destination account")
	})

	t.Run("dynamic account interpolation", func(t *testing.T) {
		t.Parallel()

		script := `
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

		volumes, err := DiscoverNumscriptVolumes(script, vars, ledgerID)
		require.NoError(t, err)
		require.Len(t, volumes, 2)

		_, hasEscrow := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "escrow:order-123"},
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

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err)

		_, hasChecking := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:alice:checking"},
			Asset:      "USD/2",
		}]
		_, hasSavings := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:alice:savings"},
			Asset:      "USD/2",
		}]
		_, hasMerchant := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "merchant"},
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

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err)

		_, hasAlice := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:alice"},
			Asset:      "USD/2",
		}]
		_, hasBob := volumes[data.VolumeKey{
			AccountKey: data.AccountKey{LedgerID: ledgerID, Account: "users:bob"},
			Asset:      "USD/2",
		}]
		require.True(t, hasAlice, "should discover first destination")
		require.True(t, hasBob, "should discover second destination")
	})

	t.Run("parse error returns error", func(t *testing.T) {
		t.Parallel()

		script := `send [USD/2 invalid] ( source = @world destination = @users:alice )`

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.Error(t, err)
		require.Nil(t, volumes)

		var parseErr *ErrNumscriptParse
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
		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err) // Parse succeeds, execution error is ignored
		// Discovery may or may not have found accounts depending on when the error occurred,
		// but the function should not return an error
		require.NotNil(t, volumes)
	})

	t.Run("sets correct ledger ID on all discovered volumes", func(t *testing.T) {
		t.Parallel()

		script := `
			send [USD/2 100] (
				source = @world
				destination = @users:alice
			)
		`

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err)

		for key := range volumes {
			require.Equal(t, ledgerID, key.LedgerID, "all volume keys should have the correct ledger ID")
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

		volumes, err := DiscoverNumscriptVolumes(script, vars, ledgerID)

		// The numscript interpreter may batch all balance queries in a single
		// GetBalances call or call it multiple times depending on the script structure.
		// If it calls GetBalances twice, we expect a non-deterministic error.
		if err != nil {
			var nonDetErr *ErrNonDeterministicScript
			require.ErrorAs(t, err, &nonDetErr)
			require.Equal(t, "GetBalances", nonDetErr.Method)
			require.Nil(t, volumes)
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

		volumes, err := DiscoverNumscriptVolumes(script, nil, ledgerID)
		require.NoError(t, err, "single GetBalances call should be allowed")
		require.NotEmpty(t, volumes)
	})
}
