package replication

import (
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strings"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/accounts"
)

// AddressRewriter applies a pipeline's address rewrite rules to the account
// addresses carried by a log before it is pushed to an exporter. It is a pure
// projection: the source ledger and its stored logs are never modified.
//
// Every address-bearing field must be rewritten together — postings, the
// post-commit volume maps (keyed by account address) and account metadata keys —
// otherwise Transaction.MarshalJSON, which derives preCommitVolumes by looking up
// posting addresses in the volume maps, would index rewritten addresses into stale
// keys and panic during export.
type AddressRewriter struct {
	rules []compiledRule
}

type compiledRule struct {
	re          *regexp.Regexp
	replacement string
}

// NewAddressRewriter compiles the given rules. It returns a nil rewriter when
// there are no rules; a nil *AddressRewriter is a valid pass-through (see Apply).
func NewAddressRewriter(rules []ledger.AddressRewriteRule) (*AddressRewriter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	compiled := make([]compiledRule, 0, len(rules))
	for _, rule := range rules {
		re, err := regexp.Compile(rule.Pattern)
		if err != nil {
			return nil, fmt.Errorf("compiling address rewrite pattern %q: %w", rule.Pattern, err)
		}
		compiled = append(compiled, compiledRule{re: re, replacement: rule.Replacement})
	}
	return &AddressRewriter{rules: compiled}, nil
}

// Apply returns a copy of the log with every account address rewritten. A nil
// receiver returns the log unchanged. Address-bearing fields are deep-copied so
// the fetched log (which backs the pull cursor) is never mutated. An error is
// returned — rather than silently emitting a partially rewritten log — when a
// rule produces an address that is no longer valid.
func (r *AddressRewriter) Apply(log ledger.Log) (ledger.Log, error) {
	if r == nil {
		return log, nil
	}

	switch data := log.Data.(type) {
	case ledger.CreatedTransaction:
		tx, err := r.rewriteTransaction(data.Transaction)
		if err != nil {
			return ledger.Log{}, err
		}
		accountMetadata, err := r.rewriteAccountMetadata(data.AccountMetadata)
		if err != nil {
			return ledger.Log{}, err
		}
		data.Transaction = tx
		data.AccountMetadata = accountMetadata
		log.Data = data
	case ledger.RevertedTransaction:
		reverted, err := r.rewriteTransaction(data.RevertedTransaction)
		if err != nil {
			return ledger.Log{}, err
		}
		revert, err := r.rewriteTransaction(data.RevertTransaction)
		if err != nil {
			return ledger.Log{}, err
		}
		data.RevertedTransaction = reverted
		data.RevertTransaction = revert
		log.Data = data
	case ledger.SavedMetadata:
		newData, err := r.rewriteMetadataTarget(data.TargetType, data.TargetID)
		if err != nil {
			return ledger.Log{}, err
		}
		data.TargetID = newData
		log.Data = data
	case ledger.DeletedMetadata:
		newData, err := r.rewriteMetadataTarget(data.TargetType, data.TargetID)
		if err != nil {
			return ledger.Log{}, err
		}
		data.TargetID = newData
		log.Data = data
	}

	return log, nil
}

// rewriteMetadataTarget rewrites a metadata target id when, and only when, it
// designates an account (for transactions the id is a uint64 and is left as-is).
func (r *AddressRewriter) rewriteMetadataTarget(targetType string, targetID any) (any, error) {
	if !strings.EqualFold(targetType, ledger.MetaTargetTypeAccount) {
		return targetID, nil
	}
	address, ok := targetID.(string)
	if !ok {
		return targetID, nil
	}
	return r.rewriteAddress(address)
}

func (r *AddressRewriter) rewriteAddress(address string) (string, error) {
	rewritten := address
	for _, rule := range r.rules {
		rewritten = rule.re.ReplaceAllString(rewritten, rule.replacement)
	}
	if rewritten == address {
		// unchanged: the address came from the ledger and is already valid.
		return address, nil
	}
	if !accounts.ValidateAddress(rewritten) {
		return "", fmt.Errorf("address rewrite of %q produced invalid address %q", address, rewritten)
	}
	return rewritten, nil
}

func (r *AddressRewriter) rewriteTransaction(tx ledger.Transaction) (ledger.Transaction, error) {
	postings, err := r.rewritePostings(tx.Postings)
	if err != nil {
		return ledger.Transaction{}, err
	}
	postCommitVolumes, err := r.rewritePostCommitVolumes(tx.PostCommitVolumes)
	if err != nil {
		return ledger.Transaction{}, err
	}
	postCommitEffectiveVolumes, err := r.rewritePostCommitVolumes(tx.PostCommitEffectiveVolumes)
	if err != nil {
		return ledger.Transaction{}, err
	}

	// tx is a copy (value parameter); reassign the address-bearing fields to the
	// freshly built collections so the original transaction is untouched. Non-address
	// fields (transaction metadata, timestamps) stay shared — they are never mutated.
	tx.Postings = postings
	tx.PostCommitVolumes = postCommitVolumes
	tx.PostCommitEffectiveVolumes = postCommitEffectiveVolumes
	return tx, nil
}

func (r *AddressRewriter) rewritePostings(postings ledger.Postings) (ledger.Postings, error) {
	if postings == nil {
		return nil, nil
	}
	rewritten := make(ledger.Postings, len(postings))
	for i, posting := range postings {
		source, err := r.rewriteAddress(posting.Source)
		if err != nil {
			return nil, err
		}
		destination, err := r.rewriteAddress(posting.Destination)
		if err != nil {
			return nil, err
		}
		rewritten[i] = ledger.Posting{
			Source:      source,
			Destination: destination,
			// Amount (*big.Int) is shared intentionally: only addresses are
			// rewritten and nothing on the export path mutates amounts.
			Amount: posting.Amount,
			Asset:  posting.Asset,
		}
	}
	return rewritten, nil
}

// rewriteAccountMetadata rebuilds the address-keyed metadata map under rewritten
// addresses. When two addresses collapse into one, their metadata is merged; on a
// conflicting key the value from the lexicographically-smallest original address
// wins (deterministic, since addresses are visited in sorted order).
func (r *AddressRewriter) rewriteAccountMetadata(accountMetadata ledger.AccountMetadata) (ledger.AccountMetadata, error) {
	if accountMetadata == nil {
		return nil, nil
	}
	rewritten := make(ledger.AccountMetadata, len(accountMetadata))
	for _, address := range sortedKeys(accountMetadata) {
		newAddress, err := r.rewriteAddress(address)
		if err != nil {
			return nil, err
		}
		existing, ok := rewritten[newAddress]
		if !ok {
			existing = metadata.Metadata{}
			rewritten[newAddress] = existing
		}
		for key, value := range accountMetadata[address] {
			if _, conflict := existing[key]; !conflict {
				existing[key] = value
			}
		}
	}
	return rewritten, nil
}

// rewritePostCommitVolumes rebuilds the address-keyed volume map under rewritten
// addresses. When two addresses collapse into one, their volumes are summed per
// asset.
func (r *AddressRewriter) rewritePostCommitVolumes(volumes ledger.PostCommitVolumes) (ledger.PostCommitVolumes, error) {
	if volumes == nil {
		return nil, nil
	}
	rewritten := ledger.PostCommitVolumes{}
	for _, address := range sortedKeys(volumes) {
		newAddress, err := r.rewriteAddress(address)
		if err != nil {
			return nil, err
		}
		byAsset, ok := rewritten[newAddress]
		if !ok {
			byAsset = ledger.VolumesByAssets{}
			rewritten[newAddress] = byAsset
		}
		for asset, volume := range volumes[address] {
			if existing, collision := byAsset[asset]; collision {
				byAsset[asset] = ledger.Volumes{
					Input:  new(big.Int).Add(existing.Input, volume.Input),
					Output: new(big.Int).Add(existing.Output, volume.Output),
				}
			} else {
				byAsset[asset] = volume.Copy()
			}
		}
	}
	return rewritten, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
