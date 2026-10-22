package cache

import (
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
)

func TestKeyStoreForcedCollision(t *testing.T) {
	t.Parallel()
	for _, generation := range []string{"gen0", "gen1"} {
		for _, deleted := range []bool{false, true} {
			t.Run(generation+map[bool]string{false: "/live", true: "/tombstone"}[deleted], func(t *testing.T) {
				t.Parallel()
				c, err := New(10, nil)
				require.NoError(t, err)
				ac := c.LedgerMetadata
				store := attributes.NewKeyStore[domain.LedgerMetadataKey, *ledgerpb.MetadataValue](ac)
				canonical := []byte("forced-collision")
				id, tag := attributes.MakeKey(canonical)
				original := attributes.Entry[*ledgerpb.MetadataValue]{Tag: tag + 1, Data: ledgerpb.NewStringValue("original"), Deleted: deleted}
				if generation == "gen0" {
					ac.Gen0().Put(id, original)
				} else {
					ac.Gen1().Put(id, original)
				}
				_, _, err = store.Get(canonical)
				var collision *attributes.ErrCollisionDetected
				require.ErrorAs(t, err, &collision)
				_, _, err = store.Put(canonical, ledgerpb.NewStringValue("replacement"))
				require.ErrorAs(t, err, &collision)
				visible, ok := ac.Get(id)
				require.True(t, ok)
				require.Equal(t, original, visible)
				_, promoted := ac.Gen0().Get(id)
				require.Equal(t, generation == "gen0", promoted)
			})
		}
	}
}

func TestKeyStoreCollisionNeverPublishedToAdmission(t *testing.T) {
	t.Parallel()
	c, err := New(10, nil)
	require.NoError(t, err)
	ac := c.LedgerMetadata
	store := attributes.NewKeyStore[domain.LedgerMetadataKey, *ledgerpb.MetadataValue](ac)
	canonical := []byte("concurrent-collision")
	id, tag := attributes.MakeKey(canonical)
	ac.Gen1().Put(id, attributes.Entry[*ledgerpb.MetadataValue]{Tag: tag + 1, Data: ledgerpb.NewStringValue("original")})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				_, _, putErr := store.Put(canonical, ledgerpb.NewStringValue("wrong"))
				if _, ok := errors.AsType[*attributes.ErrCollisionDetected](putErr); !ok {
					t.Errorf("Put error = %v, want collision", putErr)

					return
				}
				runtime.Gosched()
			}
		})
		wg.Go(func() {
			for range 1000 {
				if entry, ok := ac.Get(id); !ok || entry.Tag != tag+1 {
					t.Errorf("admission observed colliding entry: %+v, present=%v", entry, ok)

					return
				}
				runtime.Gosched()
			}
		})
	}
	wg.Wait()
	require.Equal(t, CacheHit, ac.CheckCache(5, id))
}

func TestKeyStoreMatchingTagAcrossGenerations(t *testing.T) {
	t.Parallel()
	for _, generation := range []string{"gen0", "gen1"} {
		for _, deleted := range []bool{false, true} {
			t.Run(generation+map[bool]string{false: "/live", true: "/tombstone"}[deleted], func(t *testing.T) {
				t.Parallel()
				c, err := New(10, nil)
				require.NoError(t, err)
				ac := c.LedgerMetadata
				store := attributes.NewKeyStore[domain.LedgerMetadataKey, *ledgerpb.MetadataValue](ac)
				canonical := []byte("matching-tag")
				id, tag := attributes.MakeKey(canonical)
				old := attributes.Entry[*ledgerpb.MetadataValue]{Tag: tag, Data: ledgerpb.NewStringValue("old"), Deleted: deleted}
				if generation == "gen0" {
					ac.Gen0().Put(id, old)
				} else {
					ac.Gen1().Put(id, old)
				}
				_, _, readErr := store.Get(canonical)
				if deleted {
					require.ErrorIs(t, readErr, domain.ErrNotFound)
				} else {
					require.NoError(t, readErr)
				}
				previous, gotID, err := store.Put(canonical, ledgerpb.NewStringValue("new"))
				require.NoError(t, err)
				require.Equal(t, id, gotID.ID)
				require.Equal(t, tag, gotID.Tag)
				require.Equal(t, !deleted, previous.IsDefined())
				if !deleted {
					require.Equal(t, old.Data, previous.Value())
				}
				entry, ok := ac.Gen0().Get(id)
				require.True(t, ok)
				require.False(t, entry.Deleted)
				require.Equal(t, ledgerpb.NewStringValue("new"), entry.Data)
			})
		}
	}
}
