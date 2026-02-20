package ctrl

import (
	"fmt"
	"io"
	"math/big"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// accountCursor iterates over attribute entries for a ledger using a single
// forward Pebble iterator, discovering accounts from Volume/Metadata entries
// and collecting volumes and metadata in one pass. It holds a ReadHandle for
// point-in-time consistency and closes it when the cursor is closed.
type accountCursor struct {
	handle   *data.ReadHandle
	iter     *pebble.Iterator
	volAcc   *attributes.Accumulator[*raftcmdpb.VolumePair]
	metaAcc  *attributes.Accumulator[*commonpb.MetadataValue]
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
		case data.AttributePrefixVolume:
			var vk data.VolumeKey
			if err := vk.Unmarshal(ck); err != nil {
				continue
			}
			account = vk.Account
		case data.AttributePrefixMetadata:
			var mk data.MetadataKey
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
		case data.AttributePrefixVolume:
			if _, err := c.volAcc.Feed(key, value); err != nil {
				return nil, fmt.Errorf("feeding volume: %w", err)
			}
		case data.AttributePrefixMetadata:
			if _, err := c.metaAcc.Feed(key, value); err != nil {
				return nil, fmt.Errorf("feeding metadata: %w", err)
			}
		}
	}
}

// buildAccount assembles the current account with accumulated volumes and metadata, then resets state.
func (c *accountCursor) buildAccount() *commonpb.Account {
	account := &commonpb.Account{
		Address:  c.currentAccount,
		Metadata: &commonpb.MetadataSet{},
	}

	// Flush volumes
	volEntries := c.volAcc.Flush()
	if len(volEntries) > 0 {
		volumes := make(map[string]*commonpb.VolumesWithBalance, len(volEntries))
		for _, entry := range volEntries {
			var vk data.VolumeKey
			if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
				continue
			}
			input := big.NewInt(0)
			output := big.NewInt(0)
			if entry.Value != nil {
				if entry.Value.InputKnown != nil {
					input = entry.Value.InputKnown.ToBigInt()
				}
				if entry.Value.OutputKnown != nil {
					output = entry.Value.OutputKnown.ToBigInt()
				}
			}
			volumes[vk.Asset] = &commonpb.VolumesWithBalance{
				Input:   input.String(),
				Output:  output.String(),
				Balance: new(big.Int).Sub(input, output).String(),
			}
		}
		account.Volumes = volumes
	}

	// Flush metadata
	metaEntries := c.metaAcc.Flush()
	if len(metaEntries) > 0 {
		md := make(metadata.Metadata, len(metaEntries))
		for _, entry := range metaEntries {
			var mk data.MetadataKey
			if err := mk.Unmarshal(entry.CanonicalKey); err == nil && entry.Value != nil {
				md[mk.Key] = entry.Value.Value
			}
		}
		if len(md) > 0 {
			account.Metadata = commonpb.MetadataSetFromMap(md)
		}
	}

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
