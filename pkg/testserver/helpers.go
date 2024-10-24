package testserver

import (
	"github.com/formancehq/go-libs/v2/collectionutils"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func NewTestServer(configurationProvider func() Configuration) *Deferred[*Server] {
	d := NewDeferred[*Server]()
	BeforeEach(func() {
		d.Reset()
		d.SetValue(New(GinkgoT(), configurationProvider()))
	})
	return d
}

func ConvertSDKTxToCoreTX(tx *components.V2Transaction) ledger.Transaction {
	return ledger.Transaction{
		TransactionData: ledger.TransactionData{
			Postings:   collectionutils.Map(tx.Postings, ConvertSDKPostingToCorePosting),
			Timestamp:  time.New(tx.Timestamp),
			InsertedAt: time.New(tx.InsertedAt),
			Metadata:   tx.Metadata,
			Reference: func() string {
				if tx.Reference == nil {
					return ""
				}
				return *tx.Reference
			}(),
		},
		ID:                         int(tx.ID.Int64()),
		PostCommitVolumes:          ConvertSDKPostCommitVolumesToCorePostCommitVolumes(tx.PostCommitVolumes),
		PostCommitEffectiveVolumes: ConvertSDKPostCommitVolumesToCorePostCommitVolumes(tx.PostCommitEffectiveVolumes),
	}
}

func ConvertSDKPostCommitVolumesToCorePostCommitVolumes(volumes map[string]map[string]components.V2Volume) ledger.PostCommitVolumes {
	ret := ledger.PostCommitVolumes{}
	for account, volumesByAsset := range volumes {
		for asset, volumes := range volumesByAsset {
			ret.Merge(ledger.PostCommitVolumes{
				account: {
					asset: ledger.Volumes{
						Input:  volumes.Input,
						Output: volumes.Output,
					},
				},
			})
		}
	}
	return ret
}

func ConvertSDKPostingToCorePosting(p components.V2Posting) ledger.Posting {
	return ledger.Posting{
		Source:      p.Source,
		Destination: p.Destination,
		Asset:       p.Asset,
		Amount:      p.Amount,
	}
}

func Subscribe(t T, testServer *Server) chan *nats.Msg {
	subscription, ch, err := testServer.Subscribe()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, subscription.Unsubscribe())
	})

	return ch
}

func ConnectToDatabase(t T, testServer *Server) *bun.DB {
	db, err := testServer.Database()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}
