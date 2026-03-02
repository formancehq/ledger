package query

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	bolt "go.etcd.io/bbolt"
)

// Compile translates a QueryFilter proto into an EntityIterator tree.
// The params map resolves parameterized conditions at execution time.
// The schema map (optional) validates condition types against declared metadata field types.
// When profile is non-nil, each iterator is wrapped in a TrackedIterator and
// profile.Root is set to the root of the iterator stats tree.
func Compile(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	filter *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	ledger string,
	params map[string]string,
	schema map[string]*commonpb.MetadataFieldSchema,
	profile *QueryProfile,
) (readstore.EntityIterator, error) {
	if filter == nil {
		return compileUniverse(tx, kb, target, ledger, profile)
	}

	switch f := filter.Filter.(type) {
	case *commonpb.QueryFilter_Field:
		return compileFieldCondition(tx, kb, f.Field, target, ledger, params, schema, profile)
	case *commonpb.QueryFilter_Address:
		return compileAddressMatch(tx, kb, f.Address, target, ledger, params, profile)
	case *commonpb.QueryFilter_And:
		return compileAnd(tx, kb, f.And, target, ledger, params, schema, profile)
	case *commonpb.QueryFilter_Or:
		return compileOr(tx, kb, f.Or, target, ledger, params, schema, profile)
	case *commonpb.QueryFilter_Not:
		return compileNot(tx, kb, f.Not, target, ledger, params, schema, profile)
	default:
		return nil, fmt.Errorf("unknown filter type: %T", filter.Filter)
	}
}

// compileUniverse returns an iterator over ALL entities (no filter).
func compileUniverse(tx *bolt.Tx, kb *readstore.KeyBuilder, target commonpb.QueryTarget, ledger string, profile *QueryProfile) (readstore.EntityIterator, error) {
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return &SliceIterator{}, nil
	}
	ns, entityLen := targetNamespaceAndLen(target)
	prefix := readstore.ExistencePrefix(kb, ledger, ns)
	iter := readstore.NewPrefixIterator(b.Cursor(), prefix, len(prefix), entityLen)
	return trackIterator(iter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(exist:%s:%s:)", ledger, ns),
		Kind:   "Prefix",
		Bucket: "exist",
	}), nil
}

// compileAnd compiles an AND filter into a merge-intersect iterator.
func compileAnd(tx *bolt.Tx, kb *readstore.KeyBuilder, and *commonpb.AndFilter, target commonpb.QueryTarget, ledger string, params map[string]string, schema map[string]*commonpb.MetadataFieldSchema, profile *QueryProfile) (readstore.EntityIterator, error) {
	children := make([]readstore.EntityIterator, 0, len(and.Filters))
	var childStats []*IteratorStats
	for _, f := range and.Filters {
		child, err := Compile(tx, kb, f, target, ledger, params, schema, profile)
		if err != nil {
			closeAll(children)
			return nil, err
		}
		if profile != nil {
			childStats = append(childStats, profile.Root)
		}
		children = append(children, child)
	}
	if len(children) == 0 {
		return &SliceIterator{}, nil
	}
	if len(children) == 1 {
		return children[0], nil
	}
	andIter := readstore.NewAndIterator(children...)
	return trackIterator(andIter, profile, &IteratorStats{
		Label:    "AndIterator",
		Kind:     "And",
		Children: childStats,
	}), nil
}

// compileOr compiles an OR filter into a merge-union iterator.
func compileOr(tx *bolt.Tx, kb *readstore.KeyBuilder, or *commonpb.OrFilter, target commonpb.QueryTarget, ledger string, params map[string]string, schema map[string]*commonpb.MetadataFieldSchema, profile *QueryProfile) (readstore.EntityIterator, error) {
	children := make([]readstore.EntityIterator, 0, len(or.Filters))
	var childStats []*IteratorStats
	for _, f := range or.Filters {
		child, err := Compile(tx, kb, f, target, ledger, params, schema, profile)
		if err != nil {
			closeAll(children)
			return nil, err
		}
		if profile != nil {
			childStats = append(childStats, profile.Root)
		}
		children = append(children, child)
	}
	if len(children) == 0 {
		return &SliceIterator{}, nil
	}
	if len(children) == 1 {
		return children[0], nil
	}
	orIter := readstore.NewOrIterator(children...)
	return trackIterator(orIter, profile, &IteratorStats{
		Label:    "OrIterator",
		Kind:     "Or",
		Children: childStats,
	}), nil
}

// compileNot compiles a NOT filter into a merge-difference iterator.
func compileNot(tx *bolt.Tx, kb *readstore.KeyBuilder, not *commonpb.NotFilter, target commonpb.QueryTarget, ledger string, params map[string]string, schema map[string]*commonpb.MetadataFieldSchema, profile *QueryProfile) (readstore.EntityIterator, error) {
	universe, err := compileUniverse(tx, kb, target, ledger, profile)
	if err != nil {
		return nil, err
	}
	var universeStats *IteratorStats
	if profile != nil {
		universeStats = profile.Root
	}
	child, err := Compile(tx, kb, not.Filter, target, ledger, params, schema, profile)
	if err != nil {
		universe.Close()
		return nil, err
	}
	var childStats *IteratorStats
	if profile != nil {
		childStats = profile.Root
	}
	notIter := readstore.NewNotIterator(universe, child)
	return trackIterator(notIter, profile, &IteratorStats{
		Label:    "NotIterator",
		Kind:     "Not",
		Children: []*IteratorStats{universeStats, childStats},
	}), nil
}

// compileFieldCondition compiles a FieldCondition (metadata filter) into a leaf iterator.
func compileFieldCondition(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	fc *commonpb.FieldCondition,
	target commonpb.QueryTarget,
	ledger string,
	params map[string]string,
	schema map[string]*commonpb.MetadataFieldSchema,
	profile *QueryProfile,
) (readstore.EntityIterator, error) {
	if fc.Field == nil {
		return nil, fmt.Errorf("field condition has no field reference")
	}

	ns, entityLen := targetNamespaceAndLen(target)
	metaKey := fc.Field.GetMetadata()

	// Validate condition type against declared schema type (if schema is provided)
	if schema != nil {
		if fieldSchema, ok := schema[metaKey]; ok {
			var err error
			fc, err = validateAndCoerceCondition(fc, fieldSchema)
			if err != nil {
				return nil, err
			}
		}
	}

	b := tx.Bucket(readstore.BucketMetadataIndex)
	if b == nil {
		return &SliceIterator{}, nil
	}
	cursor := b.Cursor()

	prefix := readstore.MetadataIndexPrefix(kb, ledger, ns, metaKey)

	switch cond := fc.Condition.(type) {
	case *commonpb.FieldCondition_StringCond:
		return compileStringCondition(cursor, prefix, entityLen, cond.StringCond, params, ledger, ns, metaKey, profile)
	case *commonpb.FieldCondition_IntCond:
		return compileIntCondition(cursor, prefix, entityLen, cond.IntCond, params, ledger, ns, metaKey, profile)
	case *commonpb.FieldCondition_UintCond:
		return compileUintCondition(cursor, prefix, entityLen, cond.UintCond, params, ledger, ns, metaKey, profile)
	case *commonpb.FieldCondition_BoolCond:
		return compileBoolCondition(cursor, prefix, entityLen, cond.BoolCond, params, ledger, ns, metaKey, profile)
	case *commonpb.FieldCondition_ExistsCond:
		return compileExistsCondition(tx, kb, ledger, ns, metaKey, entityLen, cond.ExistsCond, profile)
	default:
		return nil, fmt.Errorf("unknown condition type: %T", fc.Condition)
	}
}

// compileStringCondition — point scan on exact string value.
// Entities are naturally sorted (same value prefix → entity suffix determines order).
func compileStringCondition(cursor *bolt.Cursor, prefix []byte, entityLen int, cond *commonpb.StringCondition, params map[string]string, ledger, ns, metaKey string, profile *QueryProfile) (readstore.EntityIterator, error) {
	value, err := resolveString(cond, params)
	if err != nil {
		return nil, err
	}
	fullPrefix := readstore.EncodeString(append([]byte{}, prefix...), value)
	iter := readstore.NewPrefixIterator(cursor, fullPrefix, len(fullPrefix), entityLen)
	return trackIterator(iter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=string)", ledger, ns, metaKey),
		Kind:   "Prefix",
		Bucket: "midx",
	}), nil
}

// resolvedIntBounds holds resolved min/max bounds for an int64 range condition.
// Values are already adjusted for exclusivity (min incremented if exclusive,
// max incremented if inclusive) so the range is [min, max).
type resolvedIntBounds struct {
	min    int64
	max    int64
	hasMin bool
	hasMax bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
// When max == min + 1, the range [min, min+1) matches only value min.
// Within a single value prefix, entities are naturally sorted by entity ID
// in the B+ tree, enabling streaming instead of materializing + sorting.
func (b resolvedIntBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
}

// resolveIntBounds resolves an IntCondition's bounds from hardcoded values or parameters,
// applying exclusivity adjustments. The returned bounds define a half-open range [min, max).
func resolveIntBounds(cond *commonpb.IntCondition, params map[string]string) (resolvedIntBounds, error) {
	var b resolvedIntBounds

	if cond.ParamMin != "" {
		v, err := resolveParamInt64(params, cond.ParamMin)
		if err != nil {
			return b, err
		}
		if cond.MinExclusive {
			v++
		}
		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := *cond.Min
		if cond.MinExclusive {
			v++
		}
		b.min = v
		b.hasMin = true
	}

	if cond.ParamMax != "" {
		v, err := resolveParamInt64(params, cond.ParamMax)
		if err != nil {
			return b, err
		}
		if !cond.MaxExclusive {
			v++
		}
		b.max = v
		b.hasMax = true
	} else if cond.Max != nil {
		v := *cond.Max
		if !cond.MaxExclusive {
			v++
		}
		b.max = v
		b.hasMax = true
	}

	return b, nil
}

// compileIntCondition — range scan on encoded int64 values.
// For equality conditions (single value), uses streaming PrefixIterator.
// For multi-value ranges, materializes + sorts because entities are not
// sorted by entity ID across different values.
func compileIntCondition(cursor *bolt.Cursor, prefix []byte, entityLen int, cond *commonpb.IntCondition, params map[string]string, ledger, ns, metaKey string, profile *QueryProfile) (readstore.EntityIterator, error) {
	bounds, err := resolveIntBounds(cond, params)
	if err != nil {
		return nil, err
	}

	// Equality optimization: single value range → entities are naturally sorted
	// within the value prefix, so we can stream instead of materializing.
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeInt64(append([]byte{}, prefix...), bounds.min)
		iter := readstore.NewPrefixIterator(cursor, fullPrefix, len(fullPrefix), entityLen)
		return trackIterator(iter, profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=int)", ledger, ns, metaKey),
			Kind:   "Prefix",
			Bucket: "midx",
		}), nil
	}

	// General range: materialize + sort
	lower := make([]byte, 0, len(prefix)+9)
	lower = append(lower, prefix...)
	upper := make([]byte, 0, len(prefix)+9)
	upper = append(upper, prefix...)

	if bounds.hasMin {
		lower = readstore.EncodeInt64(lower, bounds.min)
	} else {
		lower = append(lower, readstore.TypeTagInt)
	}
	if bounds.hasMax {
		upper = readstore.EncodeInt64(upper, bounds.max)
	} else {
		upper = append(upper, readstore.TypeTagInt+1)
	}

	entityOffset := len(prefix) + 1 + 8 // prefix + typeTag(1) + int64(8)
	iter := materializeRange(cursor, lower, upper, entityOffset, entityLen, profile)
	return trackIterator(iter, profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=int range)", ledger, ns, metaKey),
		Kind:   "Range",
		Bucket: "midx",
	}), nil
}

// resolvedUintBounds holds resolved min/max bounds for a uint64 range condition.
// Values are already adjusted for exclusivity so the range is [min, max).
type resolvedUintBounds struct {
	min    uint64
	max    uint64
	hasMin bool
	hasMax bool
}

// isEquality returns true if the resolved bounds cover exactly one value.
func (b resolvedUintBounds) isEquality() bool {
	return b.hasMin && b.hasMax && b.max == b.min+1
}

// resolveUintBounds resolves a UintCondition's bounds from hardcoded values or parameters,
// applying exclusivity adjustments. The returned bounds define a half-open range [min, max).
func resolveUintBounds(cond *commonpb.UintCondition, params map[string]string) (resolvedUintBounds, error) {
	var b resolvedUintBounds

	if cond.ParamMin != "" {
		v, err := resolveParamUint64(params, cond.ParamMin)
		if err != nil {
			return b, err
		}
		if cond.MinExclusive {
			v++
		}
		b.min = v
		b.hasMin = true
	} else if cond.Min != nil {
		v := *cond.Min
		if cond.MinExclusive {
			v++
		}
		b.min = v
		b.hasMin = true
	}

	if cond.ParamMax != "" {
		v, err := resolveParamUint64(params, cond.ParamMax)
		if err != nil {
			return b, err
		}
		if !cond.MaxExclusive {
			v++
		}
		b.max = v
		b.hasMax = true
	} else if cond.Max != nil {
		v := *cond.Max
		if !cond.MaxExclusive {
			v++
		}
		b.max = v
		b.hasMax = true
	}

	return b, nil
}

// compileUintCondition — range scan on encoded uint64 values.
// For equality conditions, uses streaming PrefixIterator (same optimization as int).
// For multi-value ranges, materializes + sorts.
func compileUintCondition(cursor *bolt.Cursor, prefix []byte, entityLen int, cond *commonpb.UintCondition, params map[string]string, ledger, ns, metaKey string, profile *QueryProfile) (readstore.EntityIterator, error) {
	bounds, err := resolveUintBounds(cond, params)
	if err != nil {
		return nil, err
	}

	// Equality optimization: single value range → streaming
	if bounds.isEquality() {
		fullPrefix := readstore.EncodeUint64(append([]byte{}, prefix...), bounds.min)
		iter := readstore.NewPrefixIterator(cursor, fullPrefix, len(fullPrefix), entityLen)
		return trackIterator(iter, profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=uint)", ledger, ns, metaKey),
			Kind:   "Prefix",
			Bucket: "midx",
		}), nil
	}

	// General range: materialize + sort
	lower := make([]byte, 0, len(prefix)+9)
	lower = append(lower, prefix...)
	upper := make([]byte, 0, len(prefix)+9)
	upper = append(upper, prefix...)

	if bounds.hasMin {
		lower = readstore.EncodeUint64(lower, bounds.min)
	} else {
		lower = append(lower, readstore.TypeTagUint)
	}
	if bounds.hasMax {
		upper = readstore.EncodeUint64(upper, bounds.max)
	} else {
		upper = append(upper, readstore.TypeTagUint+1)
	}

	entityOffset := len(prefix) + 1 + 8
	iter := materializeRange(cursor, lower, upper, entityOffset, entityLen, profile)
	return trackIterator(iter, profile, &IteratorStats{
		Label:  fmt.Sprintf("SliceIterator(midx:%s:%s:%s=uint range)", ledger, ns, metaKey),
		Kind:   "Range",
		Bucket: "midx",
	}), nil
}

// compileBoolCondition — point scan on exact bool value.
func compileBoolCondition(cursor *bolt.Cursor, prefix []byte, entityLen int, cond *commonpb.BoolCondition, params map[string]string, ledger, ns, metaKey string, profile *QueryProfile) (readstore.EntityIterator, error) {
	value, err := resolveBool(cond, params)
	if err != nil {
		return nil, err
	}
	fullPrefix := readstore.EncodeBool(append([]byte{}, prefix...), value)
	iter := readstore.NewPrefixIterator(cursor, fullPrefix, len(fullPrefix), entityLen)
	return trackIterator(iter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(midx:%s:%s:%s=bool)", ledger, ns, metaKey),
		Kind:   "Prefix",
		Bucket: "midx",
	}), nil
}

// compileExistsCondition — streaming scan on the entity-ordered existence index (eidx).
// Entities are stored in entity ID order, so no materialization or sorting is needed.
func compileExistsCondition(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	ledger, ns, metaKey string,
	entityLen int,
	cond *commonpb.ExistsCondition,
	profile *QueryProfile,
) (readstore.EntityIterator, error) {
	b := tx.Bucket(readstore.BucketEntityExists)
	if b == nil {
		return &SliceIterator{}, nil
	}

	nonNullPrefix := readstore.EntityExistsNonNullPrefix(kb, ledger, ns, metaKey)
	if !cond.IncludeNull {
		// Only non-null entries
		iter := readstore.NewPrefixIterator(b.Cursor(), nonNullPrefix, len(nonNullPrefix), entityLen)
		return trackIterator(iter, profile, &IteratorStats{
			Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ledger, ns, metaKey),
			Kind:   "Prefix",
			Bucket: "eidx",
		}), nil
	}

	// Both non-null and null entries: merge two prefix iterators
	nullPrefix := readstore.EntityExistsNullPrefix(kb, ledger, ns, metaKey)
	nonNullIter := readstore.NewPrefixIterator(b.Cursor(), nonNullPrefix, len(nonNullPrefix), entityLen)
	nullIter := readstore.NewPrefixIterator(b.Cursor(), nullPrefix, len(nullPrefix), entityLen)

	nonNullTracked := trackIterator(nonNullIter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s non-null)", ledger, ns, metaKey),
		Kind:   "Prefix",
		Bucket: "eidx",
	})
	var nonNullStats *IteratorStats
	if profile != nil {
		nonNullStats = profile.Root
	}

	nullTracked := trackIterator(nullIter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(eidx:%s:%s:%s null)", ledger, ns, metaKey),
		Kind:   "Prefix",
		Bucket: "eidx",
	})
	var nullStats *IteratorStats
	if profile != nil {
		nullStats = profile.Root
	}

	orIter := readstore.NewOrIterator(nonNullTracked, nullTracked)
	return trackIterator(orIter, profile, &IteratorStats{
		Label:    fmt.Sprintf("OrIterator(eidx:%s:%s:%s exists)", ledger, ns, metaKey),
		Kind:     "Or",
		Bucket:   "eidx",
		Children: []*IteratorStats{nonNullStats, nullStats},
	}), nil
}

// addressRoleBucket returns the bbolt bucket for the given address role.
func addressRoleBucket(role commonpb.AddressRole) []byte {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return readstore.BucketSourceAccountTx
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return readstore.BucketDestAccountTx
	default:
		return readstore.BucketAccountTx
	}
}

// compileAddressMatch compiles an address filter.
func compileAddressMatch(
	tx *bolt.Tx,
	kb *readstore.KeyBuilder,
	am *commonpb.AddressMatch,
	target commonpb.QueryTarget,
	ledger string,
	params map[string]string,
	profile *QueryProfile,
) (readstore.EntityIterator, error) {
	role := am.Role
	switch m := am.Match.(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		return compileAddressPrefix(tx, kb, m.HardcodedPrefix, target, ledger, role, profile)
	case *commonpb.AddressMatch_HardcodedExact:
		return compileAddressExact(tx, kb, m.HardcodedExact, target, ledger, role, profile)
	case *commonpb.AddressMatch_ParamPrefix:
		value, ok := params[m.ParamPrefix]
		if !ok {
			return nil, fmt.Errorf("parameter %q not provided", m.ParamPrefix)
		}
		return compileAddressPrefix(tx, kb, value, target, ledger, role, profile)
	case *commonpb.AddressMatch_ParamExact:
		value, ok := params[m.ParamExact]
		if !ok {
			return nil, fmt.Errorf("parameter %q not provided", m.ParamExact)
		}
		return compileAddressExact(tx, kb, value, target, ledger, role, profile)
	default:
		return nil, fmt.Errorf("unknown address match type: %T", am.Match)
	}
}

func compileAddressPrefix(tx *bolt.Tx, kb *readstore.KeyBuilder, addrPrefix string, target commonpb.QueryTarget, ledger string, role commonpb.AddressRole, profile *QueryProfile) (readstore.EntityIterator, error) {
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return &SliceIterator{}, nil
	}

	// Build existence prefix: [ledger\x00][a:][addressPrefix]
	prefix := readstore.ExistencePrefix(kb, ledger, readstore.NamespaceAccount)
	scanPrefix := append(append([]byte{}, prefix...), addrPrefix...)

	accountIter := readstore.NewPrefixIterator(b.Cursor(), scanPrefix, len(prefix), 0)
	trackedAccount := trackIterator(accountIter, profile, &IteratorStats{
		Label:  fmt.Sprintf("PrefixIterator(exist:%s:a:%s*)", ledger, addrPrefix),
		Kind:   "Prefix",
		Bucket: "exist",
	})

	if target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return trackedAccount, nil
	}
	// TRANSACTIONS target: translate matching accounts → transaction IDs
	var accountStats *IteratorStats
	if profile != nil {
		accountStats = profile.Root
	}
	addrTxIter := readstore.NewAddressTxIterator(tx, kb, ledger, trackedAccount, addressRoleBucket(role))
	return trackIterator(addrTxIter, profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ledger),
		Kind:     "AddressTx",
		Bucket:   "atx",
		Children: []*IteratorStats{accountStats},
	}), nil
}

func compileAddressExact(tx *bolt.Tx, kb *readstore.KeyBuilder, exactAddr string, target commonpb.QueryTarget, ledger string, role commonpb.AddressRole, profile *QueryProfile) (readstore.EntityIterator, error) {
	b := tx.Bucket(readstore.BucketExistence)
	if b == nil {
		return &SliceIterator{}, nil
	}

	// Check if the exact address exists
	key := readstore.ExistenceKey(kb, ledger, readstore.NamespaceAccount, []byte(exactAddr))
	k, _ := b.Cursor().Seek(key)
	if k == nil || !bytes.Equal(k, key) {
		return &SliceIterator{}, nil
	}

	if target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		iter := &SliceIterator{entities: [][]byte{[]byte(exactAddr)}}
		return trackIterator(iter, profile, &IteratorStats{
			Label: fmt.Sprintf("SliceIterator(exact:%s)", exactAddr),
			Kind:  "Slice",
		}), nil
	}
	// TRANSACTIONS target: wrap single account in AddressTxIterator
	singleIter := &SliceIterator{entities: [][]byte{[]byte(exactAddr)}}
	trackedSingle := trackIterator(singleIter, profile, &IteratorStats{
		Label: fmt.Sprintf("SliceIterator(exact:%s)", exactAddr),
		Kind:  "Slice",
	})
	var singleStats *IteratorStats
	if profile != nil {
		singleStats = profile.Root
	}
	addrTxIter := readstore.NewAddressTxIterator(tx, kb, ledger, trackedSingle, addressRoleBucket(role))
	return trackIterator(addrTxIter, profile, &IteratorStats{
		Label:    fmt.Sprintf("AddressTxIterator(%s)", ledger),
		Kind:     "AddressTx",
		Bucket:   "atx",
		Children: []*IteratorStats{singleStats},
	}), nil
}

// --- Helpers ---

// trackIterator wraps an iterator with a TrackedIterator when profiling is active.
// It also sets profile.Root to the new stats node.
func trackIterator(iter readstore.EntityIterator, profile *QueryProfile, stats *IteratorStats) readstore.EntityIterator {
	if profile == nil {
		return iter
	}
	profile.Root = stats
	return NewTrackedIterator(iter, stats)
}

// targetNamespaceAndLen returns the namespace and entity length for a query target.
func targetNamespaceAndLen(target commonpb.QueryTarget) (string, int) {
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return readstore.NamespaceTransaction, 8
	}
	return readstore.NamespaceAccount, 0
}

func resolveString(cond *commonpb.StringCondition, params map[string]string) (string, error) {
	switch v := cond.Value.(type) {
	case *commonpb.StringCondition_Hardcoded:
		return v.Hardcoded, nil
	case *commonpb.StringCondition_Param:
		val, ok := params[v.Param]
		if !ok {
			return "", fmt.Errorf("parameter %q not provided", v.Param)
		}
		return val, nil
	default:
		return "", fmt.Errorf("string condition has no value")
	}
}

func resolveBool(cond *commonpb.BoolCondition, params map[string]string) (bool, error) {
	switch v := cond.Value.(type) {
	case *commonpb.BoolCondition_Hardcoded:
		return v.Hardcoded, nil
	case *commonpb.BoolCondition_Param:
		val, ok := params[v.Param]
		if !ok {
			return false, fmt.Errorf("parameter %q not provided", v.Param)
		}
		b, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("parameter %q is not a valid boolean: %w", v.Param, err)
		}
		return b, nil
	default:
		return false, fmt.Errorf("bool condition has no value")
	}
}

func resolveParamInt64(params map[string]string, name string) (int64, error) {
	val, ok := params[name]
	if !ok {
		return 0, fmt.Errorf("parameter %q not provided", name)
	}
	return strconv.ParseInt(val, 10, 64)
}

func resolveParamUint64(params map[string]string, name string) (uint64, error) {
	val, ok := params[name]
	if !ok {
		return 0, fmt.Errorf("parameter %q not provided", name)
	}
	return strconv.ParseUint(val, 10, 64)
}

// materializeRange scans a bbolt range and collects sorted entity IDs.
// When profile is non-nil, it increments MaterializedRanges and MaterializedItems.
func materializeRange(cursor *bolt.Cursor, lower, upper []byte, entityOffset, entityLen int, profile *QueryProfile) *SliceIterator {
	if profile != nil {
		profile.MaterializedRanges++
	}
	var entities [][]byte
	for k, _ := cursor.Seek(lower); k != nil && bytes.Compare(k, upper) < 0; k, _ = cursor.Next() {
		entity := extractEntityAtOffset(k, entityOffset, entityLen)
		if entity != nil {
			cp := make([]byte, len(entity))
			copy(cp, entity)
			entities = append(entities, cp)
		}
	}
	if profile != nil {
		profile.MaterializedItems += len(entities)
	}
	sortEntities(entities)
	return &SliceIterator{entities: entities}
}

// extractEntityAtOffset extracts entity from a key at a known byte offset.
func extractEntityAtOffset(key []byte, entityOffset, entityLen int) []byte {
	if len(key) <= entityOffset {
		return nil
	}
	suffix := key[entityOffset:]
	if entityLen > 0 {
		if len(suffix) < entityLen {
			return nil
		}
		return suffix[:entityLen]
	}
	return suffix
}

func sortEntities(entities [][]byte) {
	sort.Slice(entities, func(i, j int) bool {
		return bytes.Compare(entities[i], entities[j]) < 0
	})
}

func closeAll(iters []readstore.EntityIterator) {
	for _, it := range iters {
		it.Close()
	}
}

// SchemaFieldsForTarget extracts the relevant metadata fields map from a schema
// based on the query target. Returns nil if schema is nil.
func SchemaFieldsForTarget(schema *commonpb.MetadataSchema, target commonpb.QueryTarget) map[string]*commonpb.MetadataFieldSchema {
	if schema == nil {
		return nil
	}
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return schema.TransactionFields
	}
	return schema.AccountFields
}

// validateAndCoerceCondition validates a field condition against the declared schema type.
// It returns the (possibly coerced) condition or an error for incompatible types.
// ExistsCondition is always valid regardless of schema type.
func validateAndCoerceCondition(fc *commonpb.FieldCondition, fieldSchema *commonpb.MetadataFieldSchema) (*commonpb.FieldCondition, error) {
	fieldName := fc.Field.GetMetadata()
	schemaType := fieldSchema.Type

	switch fc.Condition.(type) {
	case *commonpb.FieldCondition_ExistsCond:
		return fc, nil

	case *commonpb.FieldCondition_IntCond:
		if commonpb.IsSignedType(schemaType) {
			return fc, nil
		}
		if commonpb.IsUnsignedType(schemaType) {
			return coerceIntToUint(fc)
		}
		return nil, fmt.Errorf("field %q is declared as %s, cannot use integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_UintCond:
		if commonpb.IsUnsignedType(schemaType) {
			return fc, nil
		}
		return nil, fmt.Errorf("field %q is declared as %s, cannot use unsigned integer condition", fieldName, schemaType)

	case *commonpb.FieldCondition_StringCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_STRING {
			return fc, nil
		}
		return nil, fmt.Errorf("field %q is declared as %s, cannot use string condition", fieldName, schemaType)

	case *commonpb.FieldCondition_BoolCond:
		if schemaType == commonpb.MetadataType_METADATA_TYPE_BOOL {
			return fc, nil
		}
		return nil, fmt.Errorf("field %q is declared as %s, cannot use bool condition", fieldName, schemaType)

	default:
		return fc, nil
	}
}

// coerceIntToUint converts an IntCondition to a UintCondition for unsigned schema fields.
// Returns an error if any bound is negative.
func coerceIntToUint(fc *commonpb.FieldCondition) (*commonpb.FieldCondition, error) {
	fieldName := fc.Field.GetMetadata()
	intCond := fc.GetIntCond()

	uintCond := &commonpb.UintCondition{
		MinExclusive: intCond.MinExclusive,
		MaxExclusive: intCond.MaxExclusive,
		ParamMin:     intCond.ParamMin,
		ParamMax:     intCond.ParamMax,
	}

	if intCond.Min != nil {
		v := *intCond.Min
		if v < 0 {
			return nil, fmt.Errorf("field %q is unsigned, cannot use negative min bound %d", fieldName, v)
		}
		uv := uint64(v)
		uintCond.Min = &uv
	}
	if intCond.Max != nil {
		v := *intCond.Max
		if v < 0 {
			return nil, fmt.Errorf("field %q is unsigned, cannot use negative max bound %d", fieldName, v)
		}
		uv := uint64(v)
		uintCond.Max = &uv
	}

	return &commonpb.FieldCondition{
		Field:     fc.Field,
		Condition: &commonpb.FieldCondition_UintCond{UintCond: uintCond},
	}, nil
}

// SliceIterator wraps a pre-sorted slice of entity IDs as an EntityIterator.
type SliceIterator struct {
	entities [][]byte
	pos      int
	current  []byte
}

func (it *SliceIterator) Next() bool {
	it.pos++
	if it.pos > len(it.entities) {
		return false
	}
	// pos is 1-indexed after first Next() call (starts at 0, first Next → pos=1)
	idx := it.pos - 1
	if idx >= len(it.entities) {
		return false
	}
	it.current = it.entities[idx]
	return true
}

func (it *SliceIterator) Current() []byte {
	return it.current
}

func (it *SliceIterator) SeekGE(target []byte) bool {
	idx := sort.Search(len(it.entities), func(i int) bool {
		return bytes.Compare(it.entities[i], target) >= 0
	})
	if idx >= len(it.entities) {
		it.pos = len(it.entities) + 1
		return false
	}
	it.pos = idx + 1 // +1 because Next() increments before reading
	it.current = it.entities[idx]
	return true
}

func (it *SliceIterator) Close() {}

var _ readstore.EntityIterator = (*SliceIterator)(nil)
