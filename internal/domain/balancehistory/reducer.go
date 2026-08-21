// Package balancehistory reduces authoritative, ordered ledger logs into the
// normalized monetary effects consumed by historical balance projections.
// It performs no I/O and never inspects the accepted order: transaction logs
// already contain the postings resolved by the FSM for direct, Numscript,
// mirror, and reversal paths.
package balancehistory

import (
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

var (
	// ErrInvalidPosition means the audit coordinates or referenced log
	// sequence cannot identify one authoritative source log.
	ErrInvalidPosition = errors.New("invalid balance history source position")
	// ErrOutOfOrder means Reduce was called with a source position that does
	// not strictly follow the previous successful call.
	ErrOutOfOrder = errors.New("balance history source log is out of order")
	// ErrMalformedLog means a log that should carry monetary or lifecycle data
	// is structurally incomplete or contains an invalid resolved posting.
	ErrMalformedLog = errors.New("malformed balance history source log")
	// ErrMissingIncarnation means a ledger-scoped log has no active,
	// audit-derived CreatedLedgerLog incarnation.
	ErrMissingIncarnation = errors.New("missing active ledger incarnation")
	// ErrInvalidLifecycle means create/delete logs contradict the active
	// audit-derived ledger lifecycle.
	ErrInvalidLifecycle = errors.New("invalid ledger incarnation lifecycle")
)

// Position identifies the AuditItem and referenced Log being reduced.
// AuditSequence and OrderIndex define the authoritative processing order;
// LogSequence must match Log.Sequence.
type Position struct {
	AuditSequence uint64
	OrderIndex    uint32
	LogSequence   uint64
}

// Amount is the canonical unsigned 256-bit posting amount, encoded big-endian.
// It is immutable and independent from both protobuf and storage encodings.
type Amount [32]byte

// AmountFromProto converts a wire Uint256 into its canonical immutable form.
func AmountFromProto(value *commonpb.Uint256) Amount {
	if value == nil {
		return Amount{}
	}

	var number uint256.Int
	value.IntoUint256(&number)

	return Amount(number.Bytes32())
}

// AmountFromUint64 is useful for deterministic fixtures and synthetic loads.
func AmountFromUint64(value uint64) Amount {
	return Amount(uint256.NewInt(value).Bytes32())
}

// BigInt returns an independent arbitrary-precision representation.
func (a Amount) BigInt() *big.Int {
	return new(big.Int).SetBytes(a[:])
}

// IsZero reports whether the amount has no set limb.
func (a Amount) IsZero() bool {
	return a == Amount{}
}

// Effect is one normalized monetary mutation. Every resolved posting produces
// a source output effect and a destination input effect. Reversal logs already
// contain compensating postings and are deliberately not inverted again.
type Effect struct {
	LedgerName     string
	AuditSequence  uint64
	OrderIndex     uint32
	LogSequence    uint64
	EffectiveAt    uint64
	InsertedAt     uint64
	Account        string
	AssetBase      string
	AssetPrecision uint8
	Color          string
	Input          Amount
	Output         Amount
}

type lifecycleTransition struct {
	create bool
	delete bool
	name   string
	id     uint32
}

// Reducer tracks audit-derived ledger lifecycle and historical-balance client
// configuration. The zero value is ready for use.
type Reducer struct {
	activeByName map[string]uint32
	seenIDs      map[uint32]string
	enabled      map[string]bool
	projected    map[string]struct{}
	last         Position
	hasLast      bool
}

// NewReducer creates an empty reducer. Call Reduce with source logs in strict
// (audit sequence, order index) order, starting with their ledger creations.
func NewReducer() *Reducer {
	return &Reducer{
		activeByName: make(map[string]uint32),
		seenIDs:      make(map[uint32]string),
		enabled:      make(map[string]bool),
	}
}

// SetProjectedLedgers fixes the ledger set whose monetary effects this replay
// emits. Configuration logs continue to update State independently.
func (r *Reducer) SetProjectedLedgers(ledgers []string) {
	r.projected = make(map[string]struct{}, len(ledgers))
	for _, ledger := range ledgers {
		r.projected[ledger] = struct{}{}
	}
}

// State returns an independent, deterministically ordered snapshot.
func (r *Reducer) State() State {
	if r == nil {
		return State{}
	}

	state := State{Last: r.last, HasLast: r.hasLast}
	if len(r.activeByName) > 0 {
		state.Active = make([]IncarnationState, 0, len(r.activeByName))
		for name, id := range r.activeByName {
			state.Active = append(state.Active, IncarnationState{Name: name, ID: id})
		}
	}
	if len(r.seenIDs) > 0 {
		state.Seen = make([]IncarnationState, 0, len(r.seenIDs))
		for id, name := range r.seenIDs {
			state.Seen = append(state.Seen, IncarnationState{Name: name, ID: id})
		}
	}
	for ledger, enabled := range r.enabled {
		if enabled {
			state.Enabled = append(state.Enabled, ledger)
		}
	}
	sort.Slice(state.Active, func(i, j int) bool { return state.Active[i].ID < state.Active[j].ID })
	sort.Slice(state.Seen, func(i, j int) bool { return state.Seen[i].ID < state.Seen[j].ID })
	sort.Strings(state.Enabled)

	return state
}

// Reduce folds one referenced commonpb.Log into zero or more monetary effects.
// Lifecycle and ordering state advance only when the complete log is valid, so
// callers may repair a source-read error and retry the same position.
func (r *Reducer) Reduce(position Position, log *commonpb.Log) ([]Effect, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: nil reducer", ErrMalformedLog)
	}
	if err := r.validatePosition(position, log); err != nil {
		return nil, err
	}

	effects, transition, err := r.reduceLog(position, log)
	if err != nil {
		return nil, err
	}

	r.ensureState()
	switch {
	case transition.create:
		r.activeByName[transition.name] = transition.id
		r.seenIDs[transition.id] = transition.name
	case transition.delete:
		delete(r.activeByName, transition.name)
		delete(r.enabled, transition.name)
	}
	r.last = position
	r.hasLast = true

	return effects, nil
}

func (r *Reducer) ensureState() {
	if r.activeByName == nil {
		r.activeByName = make(map[string]uint32)
	}
	if r.seenIDs == nil {
		r.seenIDs = make(map[uint32]string)
	}
	if r.enabled == nil {
		r.enabled = make(map[string]bool)
	}
}

func (r *Reducer) validatePosition(position Position, log *commonpb.Log) error {
	if position.AuditSequence == 0 || position.LogSequence == 0 {
		return fmt.Errorf("%w: audit and log sequences must be non-zero", ErrInvalidPosition)
	}
	if log == nil {
		return fmt.Errorf("%w: nil log", ErrMalformedLog)
	}
	if log.GetSequence() != position.LogSequence {
		return fmt.Errorf(
			"%w: position log sequence %d does not match log sequence %d",
			ErrInvalidPosition,
			position.LogSequence,
			log.GetSequence(),
		)
	}
	if r.hasLast && !positionAfter(position, r.last) {
		return fmt.Errorf(
			"%w: (%d,%d) does not follow (%d,%d)",
			ErrOutOfOrder,
			position.AuditSequence,
			position.OrderIndex,
			r.last.AuditSequence,
			r.last.OrderIndex,
		)
	}

	return nil
}

func positionAfter(candidate, previous Position) bool {
	return candidate.AuditSequence > previous.AuditSequence ||
		(candidate.AuditSequence == previous.AuditSequence && candidate.OrderIndex > previous.OrderIndex)
}

func (r *Reducer) reduceLog(position Position, log *commonpb.Log) ([]Effect, lifecycleTransition, error) {
	payload := log.GetPayload()
	if payload == nil || payload.GetType() == nil {
		return nil, lifecycleTransition{}, fmt.Errorf("%w: log %d has no payload", ErrMalformedLog, log.GetSequence())
	}

	r.ensureState()
	switch typed := payload.GetType().(type) {
	case *commonpb.LogPayload_CreateLedger:
		return r.reduceCreateLedger(typed.CreateLedger)
	case *commonpb.LogPayload_DeleteLedger:
		return r.reduceDeleteLedger(typed.DeleteLedger)
	case *commonpb.LogPayload_Apply:
		effects, err := r.reduceApply(position, typed.Apply)

		return effects, lifecycleTransition{}, err
	default:
		return nil, lifecycleTransition{}, nil
	}
}

func (r *Reducer) reduceCreateLedger(created *commonpb.CreatedLedgerLog) ([]Effect, lifecycleTransition, error) {
	if created == nil || created.GetName() == "" || created.GetId() == 0 {
		return nil, lifecycleTransition{}, fmt.Errorf("%w: incomplete create-ledger log", ErrMalformedLog)
	}
	if id, ok := r.activeByName[created.GetName()]; ok {
		return nil, lifecycleTransition{}, fmt.Errorf(
			"%w: ledger %q is already active as incarnation %d",
			ErrInvalidLifecycle,
			created.GetName(),
			id,
		)
	}
	for _, seenName := range r.seenIDs {
		if seenName == created.GetName() {
			// The primary FSM retains a soft-deleted LedgerInfo and rejects
			// recreation with ErrLedgerDeleted. Observing the same name twice in
			// the audit therefore means the authoritative lifecycle is invalid.
			return nil, lifecycleTransition{}, fmt.Errorf(
				"%w: ledger name %q was already used",
				ErrInvalidLifecycle,
				created.GetName(),
			)
		}
	}
	if name, ok := r.seenIDs[created.GetId()]; ok {
		return nil, lifecycleTransition{}, fmt.Errorf(
			"%w: incarnation %d was already assigned to ledger %q",
			ErrInvalidLifecycle,
			created.GetId(),
			name,
		)
	}

	return nil, lifecycleTransition{create: true, name: created.GetName(), id: created.GetId()}, nil
}

func (r *Reducer) reduceDeleteLedger(deleted *commonpb.DeletedLedgerLog) ([]Effect, lifecycleTransition, error) {
	if deleted == nil || deleted.GetName() == "" {
		return nil, lifecycleTransition{}, fmt.Errorf("%w: incomplete delete-ledger log", ErrMalformedLog)
	}
	if _, ok := r.activeByName[deleted.GetName()]; !ok {
		return nil, lifecycleTransition{}, fmt.Errorf(
			"%w: deleting inactive ledger %q",
			ErrInvalidLifecycle,
			deleted.GetName(),
		)
	}

	return nil, lifecycleTransition{delete: true, name: deleted.GetName()}, nil
}

func (r *Reducer) reduceApply(position Position, apply *commonpb.ApplyLedgerLog) ([]Effect, error) {
	if apply == nil || apply.GetLedgerName() == "" || apply.GetLog() == nil || apply.GetLog().GetData() == nil {
		return nil, fmt.Errorf("%w: incomplete apply log", ErrMalformedLog)
	}
	if apply.GetLog().GetData().GetPayload() == nil {
		return nil, fmt.Errorf("%w: apply log has no ledger payload", ErrMalformedLog)
	}

	_, ok := r.activeByName[apply.GetLedgerName()]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %q", ErrMissingIncarnation, apply.GetLedgerName())
	}

	switch typed := apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_ConfiguredHistoricalBalances:
		if typed.ConfiguredHistoricalBalances == nil {
			return nil, fmt.Errorf("%w: nil historical-balances configuration payload", ErrMalformedLog)
		}
		r.enabled[apply.GetLedgerName()] = typed.ConfiguredHistoricalBalances.GetEnabled()

		return nil, nil
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if typed.CreatedTransaction == nil {
			return nil, fmt.Errorf("%w: nil created-transaction payload", ErrMalformedLog)
		}
		if r.projected != nil {
			if _, projected := r.projected[apply.GetLedgerName()]; !projected {
				return nil, nil
			}
		}

		return reduceTransaction(position, apply.GetLedgerName(), typed.CreatedTransaction.GetTransaction())
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if typed.RevertedTransaction == nil {
			return nil, fmt.Errorf("%w: nil reverted-transaction payload", ErrMalformedLog)
		}
		if r.projected != nil {
			if _, projected := r.projected[apply.GetLedgerName()]; !projected {
				return nil, nil
			}
		}

		// The FSM log contains already-reversed postings. Reusing the same
		// reduction path is what prevents a second, incorrect inversion.
		return reduceTransaction(position, apply.GetLedgerName(), typed.RevertedTransaction.GetRevertTransaction())
	default:
		return nil, nil
	}
}

func reduceTransaction(position Position, ledgerName string, transaction *commonpb.Transaction) ([]Effect, error) {
	if transaction == nil || transaction.GetTimestamp() == nil || transaction.GetInsertedAt() == nil {
		return nil, fmt.Errorf("%w: transaction is missing its effective or insertion timestamp", ErrMalformedLog)
	}
	if len(transaction.GetPostings()) == 0 {
		return nil, fmt.Errorf("%w: transaction has no resolved posting", ErrMalformedLog)
	}

	effects := make([]Effect, 0, len(transaction.GetPostings())*2)
	for index, posting := range transaction.GetPostings() {
		postingEffects, err := reducePosting(position, ledgerName, transaction, posting)
		if err != nil {
			return nil, fmt.Errorf("posting %d: %w", index, err)
		}
		effects = append(effects, postingEffects...)
	}

	return effects, nil
}

func reducePosting(
	position Position,
	ledgerName string,
	transaction *commonpb.Transaction,
	posting *commonpb.Posting,
) ([]Effect, error) {
	if posting == nil || posting.GetSource() == "" || posting.GetDestination() == "" || posting.GetAmount() == nil {
		return nil, fmt.Errorf("%w: incomplete resolved posting", ErrMalformedLog)
	}
	if err := domain.ValidateAccountAddress(posting.GetSource()); err != nil {
		return nil, fmt.Errorf("%w: invalid source account: %w", ErrMalformedLog, err)
	}
	if err := domain.ValidateAccountAddress(posting.GetDestination()); err != nil {
		return nil, fmt.Errorf("%w: invalid destination account: %w", ErrMalformedLog, err)
	}
	if err := domain.ValidateAsset(posting.GetAsset()); err != nil {
		return nil, fmt.Errorf("%w: invalid asset: %w", ErrMalformedLog, err)
	}
	if err := domain.ValidateColor(posting.GetColor()); err != nil {
		return nil, fmt.Errorf("%w: invalid color: %w", ErrMalformedLog, err)
	}

	amount := AmountFromProto(posting.GetAmount())
	if amount == (Amount{}) {
		return nil, fmt.Errorf("%w: resolved posting amount is zero", ErrMalformedLog)
	}
	assetBase, assetPrecision := domain.ParseAssetPrecision(posting.GetAsset())
	common := Effect{
		LedgerName:     ledgerName,
		AuditSequence:  position.AuditSequence,
		OrderIndex:     position.OrderIndex,
		LogSequence:    position.LogSequence,
		EffectiveAt:    transaction.GetTimestamp().GetData(),
		InsertedAt:     transaction.GetInsertedAt().GetData(),
		AssetBase:      assetBase,
		AssetPrecision: assetPrecision,
		Color:          posting.GetColor(),
	}

	source := common
	source.Account = posting.GetSource()
	source.Output = amount
	destination := common
	destination.Account = posting.GetDestination()
	destination.Input = amount

	return []Effect{source, destination}, nil
}
