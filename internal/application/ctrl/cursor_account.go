package ctrl

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// accountCursor iterates over attribute entries for a ledger using a single
// forward Pebble iterator, discovering accounts from Volume/Metadata entries
// and collecting volumes and metadata in one pass. It holds a ReadHandle for
// point-in-time consistency and closes it when the cursor is closed.
type accountCursor struct {
	handle   *dal.ReadHandle
	iter     *pebble.Iterator
	volAcc   *attributes.Accumulator[*raftcmdpb.VolumePair]
	metaAcc  *attributes.Accumulator[*commonpb.MetadataValue]
	schema   *commonpb.MetadataSchema // lazy read-path conversion
	pageSize uint32
	count    uint32
	started  bool
	done     bool

	// Current account being built
	currentAccount string
	hasAccount     bool

	// Pending entry saved when a boundary is detected
	pendingKey   []byte
	pendingValue []byte
	hasPending   bool
}

func (c *accountCursor) Next() (*commonpb.Account, error) {
	if c.pageSize > 0 && c.count >= c.pageSize {
		return nil, io.EOF
	}
	if c.done {
		return nil, io.EOF
	}

	for {
		var key, value []byte

		if c.hasPending {
			key = c.pendingKey
			value = c.pendingValue
			c.hasPending = false
		} else {
			var valid bool
			if !c.started {
				c.started = true
				valid = c.iter.First()
			} else {
				valid = c.iter.Next()
			}
			if !valid {
				if err := c.iter.Error(); err != nil {
					return nil, err
				}
				// End of iterator — return last collected account if any
				if c.hasAccount {
					c.done = true
					c.count++
					return c.buildAccount(), nil
				}
				return nil, io.EOF
			}
			key = c.iter.Key()
			var err error
			value, err = c.iter.ValueAndErr()
			if err != nil {
				return nil, err
			}
		}

		attrType, ok := attributes.AttrTypeFromKey(key)
		if !ok {
			continue
		}

		ck := attributes.CanonicalKeyFromPebbleKey(key)
		if ck == nil {
			continue
		}

		var account string
		switch attrType {
		case dal.AttributePrefixVolume:
			var vk domain.VolumeKey
			if err := vk.Unmarshal(ck); err != nil {
				continue
			}
			account = vk.Account
		case dal.AttributePrefixMetadata:
			var mk domain.MetadataKey
			if err := mk.Unmarshal(ck); err != nil {
				continue
			}
			account = mk.Account
		default:
			continue
		}

		if account == "" {
			continue
		}

		// Account boundary: flush current account and save this entry as pending
		if c.hasAccount && account != c.currentAccount {
			c.pendingKey = append(c.pendingKey[:0], key...)
			c.pendingValue = append(c.pendingValue[:0], value...)
			c.hasPending = true
			c.count++
			return c.buildAccount(), nil
		}

		if !c.hasAccount {
			c.currentAccount = account
			c.hasAccount = true
		}

		switch attrType {
		case dal.AttributePrefixVolume:
			if _, err := c.volAcc.Feed(key, value); err != nil {
				return nil, fmt.Errorf("feeding volume: %w", err)
			}
		case dal.AttributePrefixMetadata:
			if _, err := c.metaAcc.Feed(key, value); err != nil {
				return nil, fmt.Errorf("feeding metadata: %w", err)
			}
		}
	}
}

// buildAccount flushes accumulators, assembles the account, and resets state.
func (c *accountCursor) buildAccount() *commonpb.Account {
	account := assembleAccount(c.currentAccount, c.volAcc.Flush(), c.metaAcc.Flush(), c.schema)
	c.hasAccount = false
	c.currentAccount = ""
	return account
}

func (c *accountCursor) Close() error {
	err := c.iter.Close()
	if closeErr := c.handle.Close(); err == nil {
		err = closeErr
	}
	return err
}
