package incarnationmigration

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"

	"github.com/cockroachdb/pebble/v2"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const definitionVersion = 1
const batchSize = 256

var markerKey = []byte{dal.ZoneGlobal, dal.SubGlobAccountIncarnationMigration}

// Marker describes the audit prefix reconstructed in this local Pebble store.
// A complete marker is copied with checkpoints. Its applied index identifies
// the exact frozen FSM state from which the local backfill was built.
type Marker struct {
	Cursor       uint64
	AppliedIndex uint64
	Complete     bool
}

func ReadMarker(reader dal.PebbleGetter) (Marker, error) {
	raw, closer, err := reader.Get(markerKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return Marker{}, nil
		}
		return Marker{}, fmt.Errorf("reading account incarnation migration marker: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(raw) != 18 || raw[0] != definitionVersion || raw[1] > 1 {
		return Marker{}, fmt.Errorf("invalid account incarnation migration marker")
	}
	return Marker{
		Complete:     raw[1] == 1,
		Cursor:       binary.BigEndian.Uint64(raw[2:10]),
		AppliedIndex: binary.BigEndian.Uint64(raw[10:18]),
	}, nil
}

// AdvertisedIDs includes data readiness only after this store has a completed
// migration. Software support alone is insufficient evidence for activation.
func AdvertisedIDs(reader dal.PebbleGetter) ([]uint32, error) {
	marker, err := ReadMarker(reader)
	if err != nil {
		return nil, err
	}
	ids := []uint32{uint32(capabilities.AccountIncarnationWritesV1)}
	if marker.Complete {
		ids = append(ids, uint32(capabilities.AccountIncarnationHistoryCompleteV1))
	}
	return ids, nil
}

func putMarker(batch *dal.WriteSession, marker Marker) error {
	var raw [18]byte
	raw[0] = definitionVersion
	if marker.Complete {
		raw[1] = 1
	}
	binary.BigEndian.PutUint64(raw[2:10], marker.Cursor)
	binary.BigEndian.PutUint64(raw[10:18], marker.AppliedIndex)
	return batch.SetBytes(markerKey, raw[:])
}

// Run converts the stopped node's durable account history in bounded commits.
// The caller must hold exclusive ownership of this Pebble store and must not
// start Raft, admission, or account reads until Run returns successfully.
func Run(ctx context.Context, store *dal.Store) error {
	policy, err := query.ReadClusterPolicy(store)
	if err != nil {
		return err
	}
	if !capabilities.IsActive(policy, capabilities.AccountIncarnationWritesV1) {
		if capabilities.IsActive(policy, capabilities.AccountIncarnationHistoryCompleteV1) {
			return errors.New("account history capability active without incarnation writes")
		}
		return nil
	}

	marker, err := ReadMarker(store)
	if err != nil {
		return err
	}
	if marker.Complete {
		return nil
	}

	reader, err := store.NewReadHandle()
	if err != nil {
		return fmt.Errorf("opening incarnation migration snapshot: %w", err)
	}
	defer func() { _ = reader.Close() }()
	lastAudit, err := query.ReadLastAuditSequence(reader)
	if err != nil {
		return err
	}
	applied, err := query.ReadLastAppliedIndex(reader)
	if err != nil {
		return err
	}
	if marker.Cursor > lastAudit || marker.AppliedIndex != 0 && marker.AppliedIndex != applied {
		return fmt.Errorf("incarnation migration cursor does not match frozen store")
	}

	cursor, err := query.ReadAuditEntries(ctx, reader, nil)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()

	fold := &folder{
		reader:  reader,
		current: store,
		attrs:   attributes.New(),
		types:   make(map[string]map[string]*commonpb.AccountType),
		rows:    make(map[domain.AccountKey]*commonpb.AccountIncarnation),
		deletes: make(map[string]struct{}),
	}
	var previous uint64
	var pending int
	for {
		entry, nextErr := cursor.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return fmt.Errorf("reading audit during incarnation migration: %w", nextErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.GetSequence() != previous+1 {
			return fmt.Errorf("account incarnation migration requires complete audit history: expected %d, found %d", previous+1, entry.GetSequence())
		}
		previous = entry.GetSequence()
		if previous > lastAudit {
			return errors.New("audit snapshot advanced beyond migration horizon")
		}
		if err := fold.process(ctx, entry, previous > marker.Cursor); err != nil {
			return fmt.Errorf("folding audit entry %d: %w", previous, err)
		}
		if previous <= marker.Cursor {
			continue
		}
		pending++
		if pending == batchSize {
			if err := fold.commit(store, Marker{Cursor: previous, AppliedIndex: applied}); err != nil {
				return err
			}
			pending = 0
		}
	}
	if previous != lastAudit {
		return fmt.Errorf("account incarnation migration ended at audit %d, expected %d", previous, lastAudit)
	}
	if pending > 0 {
		if err := fold.commit(store, Marker{Cursor: previous, AppliedIndex: applied}); err != nil {
			return err
		}
	}
	if err := clearIncarnationCacheAndSeal(store, Marker{Cursor: lastAudit, AppliedIndex: applied, Complete: true}); err != nil {
		return err
	}
	return nil
}

// Verify reconstructs the normal-account incarnation projection from the
// chain-checked audit history into a disposable bounded store, then compares
// both directions of the durable attribute range. Callers must first verify
// the audit chain itself.
func Verify(ctx context.Context, source dal.PebbleReader, logger logging.Logger) error {
	policy, err := query.ReadClusterPolicy(source)
	if err != nil {
		return err
	}
	marker, err := ReadMarker(source)
	if err != nil {
		return err
	}
	if !marker.Complete {
		if capabilities.IsActive(policy, capabilities.AccountIncarnationHistoryCompleteV1) {
			return errors.New("account history is active without a complete local migration")
		}
		return nil
	}
	dir, err := os.MkdirTemp("", "checker-incarnations-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	config := dal.DefaultConfig()
	config.DisableWAL = true
	expected, err := dal.NewStore(dir, logger, noop.NewMeterProvider().Meter("checker.incarnations"), config)
	if err != nil {
		return err
	}
	defer func() { _ = expected.Close() }()
	cursor, err := query.ReadAuditEntries(ctx, source, nil)
	if err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	fold := &folder{reader: source, current: expected, attrs: attributes.New(), types: make(map[string]map[string]*commonpb.AccountType), rows: make(map[domain.AccountKey]*commonpb.AccountIncarnation), deletes: make(map[string]struct{})}
	var sequence uint64
	var pending int
	for {
		entry, nextErr := cursor.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return nextErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.GetSequence() != sequence+1 {
			return fmt.Errorf("account history verification has an audit gap at %d", sequence+1)
		}
		sequence++
		if err := fold.process(ctx, entry, true); err != nil {
			return err
		}
		pending++
		if pending == batchSize {
			if err := fold.commit(expected, Marker{}); err != nil {
				return err
			}
			pending = 0
		}
	}
	if pending > 0 {
		if err := fold.commit(expected, Marker{}); err != nil {
			return err
		}
	}
	if sequence < marker.Cursor {
		return fmt.Errorf("account history verification ends at %d before migration cursor %d", sequence, marker.Cursor)
	}
	prefix := []byte{dal.ZoneAttributes, dal.SubAttrAccountIncarnation}
	options := &pebble.IterOptions{LowerBound: prefix, UpperBound: dal.PrefixUpperBound(prefix)}
	expectedReader, err := expected.NewReadHandle()
	if err != nil {
		return err
	}
	defer func() { _ = expectedReader.Close() }()
	want, err := expectedReader.NewIter(options)
	if err != nil {
		return err
	}
	defer func() { _ = want.Close() }()
	got, err := source.NewIter(options)
	if err != nil {
		return err
	}
	defer func() { _ = got.Close() }()
	for hasWant, hasGot := want.First(), got.First(); hasWant || hasGot; hasWant, hasGot = want.Next(), got.Next() {
		if !hasWant || !hasGot || !bytes.Equal(want.Key(), got.Key()) || !bytes.Equal(want.Value(), got.Value()) {
			return fmt.Errorf("account incarnation projection differs from audited history")
		}
	}
	if err := want.Error(); err != nil {
		return err
	}
	return got.Error()
}

type folder struct {
	reader  dal.PebbleReader
	current dal.PebbleGetter
	attrs   *attributes.Attributes
	types   map[string]map[string]*commonpb.AccountType
	rows    map[domain.AccountKey]*commonpb.AccountIncarnation
	deletes map[string]struct{}
}

func (f *folder) process(ctx context.Context, entry *auditpb.AuditEntry, write bool) error {
	success := entry.GetSuccess()
	if success == nil {
		return nil
	}
	items, err := query.ReadAuditItems(ctx, f.reader, entry.GetSequence())
	if err != nil {
		return err
	}
	if len(items) != int(entry.GetOrderCount()) {
		return fmt.Errorf("audit item count mismatch")
	}
	touched := make(map[domain.AccountKey]struct{})
	deletedHere := make(map[string]struct{})
	for _, item := range items {
		sequence := item.GetLogSequence()
		if sequence == 0 || sequence < success.GetMinLogSequence() || sequence > success.GetMaxLogSequence() {
			continue
		}
		log, err := query.ReadLogBySequence(ctx, f.reader, sequence)
		if err != nil || log == nil {
			return fmt.Errorf("reading audit-linked log %d: %w", sequence, err)
		}
		if err := f.processLog(log, touched, deletedHere, write); err != nil {
			return err
		}
	}
	if !write {
		return nil
	}
	for key := range touched {
		if _, deleted := deletedHere[key.LedgerName]; deleted {
			continue
		}
		types, exists := f.types[key.LedgerName]
		if !exists {
			return fmt.Errorf("account write without live ledger %q", key.LedgerName)
		}
		if matched := accounttype.FindMatchingType(key.Account, accounttype.CompileTypes(types)); matched != nil && matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL {
			continue
		}
		if err := f.touch(key, entry.GetTimestamp()); err != nil {
			return err
		}
	}
	return nil
}

func (f *folder) processLog(log *commonpb.Log, touched map[domain.AccountKey]struct{}, deletedHere map[string]struct{}, write bool) error {
	payload := log.GetPayload()
	if create := payload.GetCreateLedger(); create != nil {
		f.types[create.GetName()] = maps.Clone(create.GetAccountTypes())
		return nil
	}
	if deleted := payload.GetDeleteLedger(); deleted != nil {
		name := deleted.GetName()
		delete(f.types, name)
		deletedHere[name] = struct{}{}
		if write {
			f.deletes[name] = struct{}{}
			for key := range f.rows {
				if key.LedgerName == name {
					delete(f.rows, key)
				}
			}
		}
		return nil
	}
	apply := payload.GetApply()
	if apply == nil {
		return nil
	}
	ledger := apply.GetLedgerName()
	data := apply.GetLog().GetData()
	if added := data.GetAddedAccountType(); added != nil {
		if f.types[ledger] == nil {
			return fmt.Errorf("account type added to unknown ledger %q", ledger)
		}
		typ := added.GetAccountType()
		f.types[ledger][typ.GetName()] = typ
	}
	if removed := data.GetRemovedAccountType(); removed != nil {
		delete(f.types[ledger], removed.GetName())
	}
	add := func(account string) { touched[domain.AccountKey{LedgerName: ledger, Account: account}] = struct{}{} }
	if created := data.GetCreatedTransaction(); created != nil {
		for _, posting := range created.GetTransaction().GetPostings() {
			add(posting.GetSource())
			add(posting.GetDestination())
		}
		for account, metadata := range created.GetAccountMetadata() {
			if len(metadata.GetValues()) > 0 {
				add(account)
			}
		}
	}
	if reverted := data.GetRevertedTransaction(); reverted != nil {
		for _, posting := range reverted.GetRevertTransaction().GetPostings() {
			add(posting.GetSource())
			add(posting.GetDestination())
		}
	}
	if saved := data.GetSavedMetadata(); saved != nil && len(saved.GetMetadata()) > 0 {
		if account := saved.GetTarget().GetAccount(); account != nil {
			add(account.GetAddr())
		}
	}
	if deleted := data.GetDeletedMetadata(); deleted != nil {
		if account := deleted.GetTarget().GetAccount(); account != nil {
			add(account.GetAddr())
		}
	}
	return nil
}

func (f *folder) touch(key domain.AccountKey, date *commonpb.Timestamp) error {
	if date == nil {
		return errors.New("accepted audit entry has no timestamp")
	}
	row := f.rows[key]
	if row == nil {
		if _, deleted := f.deletes[key.LedgerName]; !deleted {
			var err error
			row, err = f.attrs.AccountIncarnation.Get(f.current, key.Bytes())
			if err != nil {
				return err
			}
		}
		if row == nil {
			row = &commonpb.AccountIncarnation{Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL}
		}
		f.rows[key] = row
	}
	if row.GetInsertedAt() == nil || date.GetData() < row.GetInsertedAt().GetData() {
		row.InsertedAt = date.CloneVT()
	}
	if row.GetUpdatedAt() == nil || date.GetData() > row.GetUpdatedAt().GetData() {
		row.UpdatedAt = date.CloneVT()
	}
	return nil
}

func (f *folder) commit(store *dal.Store, marker Marker) error {
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	for ledger := range f.deletes {
		prefix := make([]byte, 2+dal.LedgerNameFixedSize)
		prefix[0], prefix[1] = dal.ZoneAttributes, dal.SubAttrAccountIncarnation
		copy(prefix[2:], ledger)
		if err := batch.DeleteRange(prefix, dal.PrefixUpperBound(prefix), nil); err != nil {
			return err
		}
	}
	for key, row := range f.rows {
		if _, err := f.attrs.AccountIncarnation.Set(batch, key.Bytes(), row); err != nil {
			return err
		}
	}
	if err := putMarker(batch, marker); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	clear(f.rows)
	clear(f.deletes)
	return nil
}

func clearIncarnationCacheAndSeal(store *dal.Store, marker Marker) error {
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	for gen := byte(0); gen <= 1; gen++ {
		prefix := []byte{dal.ZoneCache, gen, dal.SubAttrAccountIncarnation}
		if err := batch.DeleteRange(prefix, dal.PrefixUpperBound(prefix), nil); err != nil {
			return err
		}
	}
	if err := putMarker(batch, marker); err != nil {
		return err
	}
	return batch.Commit()
}
