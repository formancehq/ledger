package internal

import (
	"context"
	"io"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// restrictedPrefixes lists ledger name prefixes created by specialized drivers
// that set account type restrictions or have specific balance assumptions.
// Generic drivers (via RunDriver) must not pick these ledgers.
var restrictedPrefixes = []string{"transient-", "insuf-", "deltest-"}

// CreateLedger creates a ledger via the Apply RPC and verifies it can be read back.
func CreateLedger(ctx context.Context, client servicepb.BucketServiceClient, name string) error {
	details := Details{"ledger": name}

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: name},
			},
		}},
	})
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to create ledger", details.With(Details{"error": err}))
	if err != nil {
		return err
	}

	// Verify it's readable
	_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	assert.Sometimes(err == nil || IsUnavailable(err), "should always be able to get created ledger", details.With(Details{"error": err}))
	return nil
}

// ListLedgers returns the names of all ledgers.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) ([]string, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}
	var names []string
	for {
		ledger, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, ledger.Name)
	}
	return names, nil
}

// GetRandomLedger returns a random unrestricted ledger name. Ledgers created
// by specialized drivers (transient-, insuf-, deltest-) are filtered out to
// prevent cross-driver interference (e.g. account type violations).
func GetRandomLedger(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	ledgers, err := ListLedgers(ctx, client)
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to get a random ledger", Details{"error": err})
	if err != nil {
		return "", err
	}

	filtered := ledgers[:0]
	for _, name := range ledgers {
		restricted := false
		for _, prefix := range restrictedPrefixes {
			if strings.HasPrefix(name, prefix) {
				restricted = true
				break
			}
		}
		if !restricted {
			filtered = append(filtered, name)
		}
	}

	if len(filtered) == 0 {
		return "", io.EOF
	}

	return filtered[Rand().Uint64()%uint64(len(filtered))], nil
}
