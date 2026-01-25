package store

import (
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/commandsfb"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	flatbuffers "github.com/google/flatbuffers/go"
)

// FlatBufferLedgerInfo wraps FlatBuffer-serialized LedgerInfo bytes
type FlatBufferLedgerInfo struct {
	data []byte
	fb   *commandsfb.LedgerInfo
}

// NewFlatBufferLedgerInfo creates a new FlatBufferLedgerInfo from raw bytes
func NewFlatBufferLedgerInfo(data []byte) *FlatBufferLedgerInfo {
	return &FlatBufferLedgerInfo{
		data: data,
		fb:   commandsfb.GetRootAsLedgerInfo(data, 0),
	}
}

// Bytes returns the raw FlatBuffer bytes
func (l *FlatBufferLedgerInfo) Bytes() []byte {
	return l.data
}

// Name returns the ledger name
func (l *FlatBufferLedgerInfo) Name() string {
	return string(l.fb.Name())
}

// Id returns the ledger ID
func (l *FlatBufferLedgerInfo) Id() uint32 {
	return l.fb.Id()
}

// CreatedAt returns the creation timestamp as uint64
func (l *FlatBufferLedgerInfo) CreatedAt() uint64 {
	if ts := l.fb.CreatedAt(nil); ts != nil {
		return ts.Data()
	}
	return 0
}

// Metadata returns the metadata as a Go map
func (l *FlatBufferLedgerInfo) Metadata() map[string]string {
	count := l.fb.MetadataLength()
	if count == 0 {
		return nil
	}
	result := make(map[string]string, count)
	var entry commandsfb.MetadataEntry
	for i := 0; i < count; i++ {
		if l.fb.Metadata(&entry, i) {
			result[string(entry.Key())] = string(entry.Value())
		}
	}
	return result
}

// FlatBufferLedgerInfoToProtobuf converts to protobuf LedgerInfo (for API compatibility)
func FlatBufferLedgerInfoToProtobuf(l *FlatBufferLedgerInfo) *ledgerpb.LedgerInfo {
	return &ledgerpb.LedgerInfo{
		Name:      l.Name(),
		Metadata:  l.Metadata(),
		CreatedAt: &ledgerpb.Timestamp{Data: l.CreatedAt()},
		Id:        l.Id(),
	}
}

// FlatBufferLedgerInfoBuilder builds FlatBuffer LedgerInfo
type FlatBufferLedgerInfoBuilder struct {
	builder *flatbuffers.Builder
}

// NewFlatBufferLedgerInfoBuilder creates a new builder
func NewFlatBufferLedgerInfoBuilder() *FlatBufferLedgerInfoBuilder {
	return &FlatBufferLedgerInfoBuilder{
		builder: flatbuffers.NewBuilder(256),
	}
}

// Build creates FlatBuffer bytes from ledger info components
func (b *FlatBufferLedgerInfoBuilder) Build(name string, id uint32, createdAt uint64, metadata map[string]string) []byte {
	b.builder.Reset()

	// Build name
	nameOffset := b.builder.CreateString(name)

	// Build metadata
	var metadataOffset flatbuffers.UOffsetT
	if len(metadata) > 0 {
		offsets := make([]flatbuffers.UOffsetT, 0, len(metadata))
		for k, v := range metadata {
			keyOffset := b.builder.CreateString(k)
			valueOffset := b.builder.CreateString(v)
			commandsfb.MetadataEntryStart(b.builder)
			commandsfb.MetadataEntryAddKey(b.builder, keyOffset)
			commandsfb.MetadataEntryAddValue(b.builder, valueOffset)
			offsets = append(offsets, commandsfb.MetadataEntryEnd(b.builder))
		}
		commandsfb.LedgerInfoStartMetadataVector(b.builder, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.builder.PrependUOffsetT(offsets[i])
		}
		metadataOffset = b.builder.EndVector(len(offsets))
	}

	// Build created_at timestamp
	commandsfb.TimestampStart(b.builder)
	commandsfb.TimestampAddData(b.builder, createdAt)
	createdAtOffset := commandsfb.TimestampEnd(b.builder)

	// Build LedgerInfo
	commandsfb.LedgerInfoStart(b.builder)
	commandsfb.LedgerInfoAddName(b.builder, nameOffset)
	if metadataOffset != 0 {
		commandsfb.LedgerInfoAddMetadata(b.builder, metadataOffset)
	}
	commandsfb.LedgerInfoAddCreatedAt(b.builder, createdAtOffset)
	commandsfb.LedgerInfoAddId(b.builder, id)
	ledgerInfoOffset := commandsfb.LedgerInfoEnd(b.builder)
	b.builder.Finish(ledgerInfoOffset)

	// Return a copy
	result := make([]byte, len(b.builder.FinishedBytes()))
	copy(result, b.builder.FinishedBytes())
	return result
}

// FlatBufferLedgerState represents in-memory mutable state for a ledger
type FlatBufferLedgerState struct {
	Info              *FlatBufferLedgerInfo
	NextLogId         uint64
	NextTransactionId uint64
}

// FlatBufferLedgerStateToProtobuf converts to protobuf LedgerState (for API compatibility)
func FlatBufferLedgerStateToProtobuf(s *FlatBufferLedgerState) *ledgerpb.LedgerState {
	return &ledgerpb.LedgerState{
		LedgerInfo:        FlatBufferLedgerInfoToProtobuf(s.Info),
		NextLogId:         s.NextLogId,
		NextTransactionId: s.NextTransactionId,
	}
}

// GetNextLogID returns and increments the next log ID
func (s *FlatBufferLedgerState) GetNextLogID() uint64 {
	ret := s.NextLogId
	s.NextLogId++
	return ret
}

// GetNextTransactionID returns and increments the next transaction ID
func (s *FlatBufferLedgerState) GetNextTransactionID() uint64 {
	ret := s.NextTransactionId
	s.NextTransactionId++
	return ret
}

// FlatBufferState represents the entire FSM state using FlatBuffer types
type FlatBufferState struct {
	Ledgers      map[uint32]*FlatBufferLedgerState
	NextLedgerId uint32
}

// NewFlatBufferState creates a new empty state
func NewFlatBufferState() *FlatBufferState {
	return &FlatBufferState{
		Ledgers:      make(map[uint32]*FlatBufferLedgerState),
		NextLedgerId: 1,
	}
}

// FlatBufferStateToProtobuf converts to protobuf State (for API compatibility)
func FlatBufferStateToProtobuf(s *FlatBufferState) *ledgerpb.State {
	state := &ledgerpb.State{
		Ledgers:      make(map[uint32]*ledgerpb.LedgerState, len(s.Ledgers)),
		NextLedgerId: s.NextLedgerId,
	}
	for id, ledgerState := range s.Ledgers {
		state.Ledgers[id] = FlatBufferLedgerStateToProtobuf(ledgerState)
	}
	return state
}

// ParseFlatBufferState parses FlatBuffer bytes into a mutable state
func ParseFlatBufferState(data []byte) *FlatBufferState {
	if len(data) == 0 {
		return NewFlatBufferState()
	}

	fb := commandsfb.GetRootAsState(data, 0)
	state := &FlatBufferState{
		Ledgers:      make(map[uint32]*FlatBufferLedgerState, fb.LedgersLength()),
		NextLedgerId: fb.NextLedgerId(),
	}

	var entry commandsfb.LedgerStateEntry
	for i := 0; i < fb.LedgersLength(); i++ {
		if fb.Ledgers(&entry, i) {
			ledgerState := entry.Value(nil)
			if ledgerState != nil {
				ledgerInfo := ledgerState.LedgerInfo(nil)
				if ledgerInfo != nil {
					// Build FlatBufferLedgerInfo from the parsed data
					builder := NewFlatBufferLedgerInfoBuilder()

					// Extract metadata
					var metadata map[string]string
					if ledgerInfo.MetadataLength() > 0 {
						metadata = make(map[string]string, ledgerInfo.MetadataLength())
						var metaEntry commandsfb.MetadataEntry
						for j := 0; j < ledgerInfo.MetadataLength(); j++ {
							if ledgerInfo.Metadata(&metaEntry, j) {
								metadata[string(metaEntry.Key())] = string(metaEntry.Value())
							}
						}
					}

					var createdAt uint64
					if ts := ledgerInfo.CreatedAt(nil); ts != nil {
						createdAt = ts.Data()
					}

					infoData := builder.Build(
						string(ledgerInfo.Name()),
						ledgerInfo.Id(),
						createdAt,
						metadata,
					)

					state.Ledgers[entry.Key()] = &FlatBufferLedgerState{
						Info:              NewFlatBufferLedgerInfo(infoData),
						NextLogId:         ledgerState.NextLogId(),
						NextTransactionId: ledgerState.NextTransactionId(),
					}
				}
			}
		}
	}

	return state
}

// FlatBufferStateBuilder builds FlatBuffer State
type FlatBufferStateBuilder struct {
	builder *flatbuffers.Builder
}

// NewFlatBufferStateBuilder creates a new builder
func NewFlatBufferStateBuilder() *FlatBufferStateBuilder {
	return &FlatBufferStateBuilder{
		builder: flatbuffers.NewBuilder(4096),
	}
}

// Build serializes the state to FlatBuffer bytes
func (b *FlatBufferStateBuilder) Build(state *FlatBufferState) []byte {
	b.builder.Reset()

	// Build ledger entries
	var ledgerEntryOffsets []flatbuffers.UOffsetT
	for id, ledgerState := range state.Ledgers {
		// Build ledger info
		nameOffset := b.builder.CreateString(ledgerState.Info.Name())

		// Build metadata
		var metadataOffset flatbuffers.UOffsetT
		metadata := ledgerState.Info.Metadata()
		if len(metadata) > 0 {
			offsets := make([]flatbuffers.UOffsetT, 0, len(metadata))
			for k, v := range metadata {
				keyOffset := b.builder.CreateString(k)
				valueOffset := b.builder.CreateString(v)
				commandsfb.MetadataEntryStart(b.builder)
				commandsfb.MetadataEntryAddKey(b.builder, keyOffset)
				commandsfb.MetadataEntryAddValue(b.builder, valueOffset)
				offsets = append(offsets, commandsfb.MetadataEntryEnd(b.builder))
			}
			commandsfb.LedgerInfoStartMetadataVector(b.builder, len(offsets))
			for i := len(offsets) - 1; i >= 0; i-- {
				b.builder.PrependUOffsetT(offsets[i])
			}
			metadataOffset = b.builder.EndVector(len(offsets))
		}

		// Build created_at timestamp
		commandsfb.TimestampStart(b.builder)
		commandsfb.TimestampAddData(b.builder, ledgerState.Info.CreatedAt())
		createdAtOffset := commandsfb.TimestampEnd(b.builder)

		// Build LedgerInfo
		commandsfb.LedgerInfoStart(b.builder)
		commandsfb.LedgerInfoAddName(b.builder, nameOffset)
		if metadataOffset != 0 {
			commandsfb.LedgerInfoAddMetadata(b.builder, metadataOffset)
		}
		commandsfb.LedgerInfoAddCreatedAt(b.builder, createdAtOffset)
		commandsfb.LedgerInfoAddId(b.builder, ledgerState.Info.Id())
		ledgerInfoOffset := commandsfb.LedgerInfoEnd(b.builder)

		// Build LedgerState
		commandsfb.LedgerStateStart(b.builder)
		commandsfb.LedgerStateAddLedgerInfo(b.builder, ledgerInfoOffset)
		commandsfb.LedgerStateAddNextLogId(b.builder, ledgerState.NextLogId)
		commandsfb.LedgerStateAddNextTransactionId(b.builder, ledgerState.NextTransactionId)
		ledgerStateOffset := commandsfb.LedgerStateEnd(b.builder)

		// Build LedgerStateEntry
		commandsfb.LedgerStateEntryStart(b.builder)
		commandsfb.LedgerStateEntryAddKey(b.builder, id)
		commandsfb.LedgerStateEntryAddValue(b.builder, ledgerStateOffset)
		ledgerEntryOffsets = append(ledgerEntryOffsets, commandsfb.LedgerStateEntryEnd(b.builder))
	}

	// Build ledgers vector
	commandsfb.StateStartLedgersVector(b.builder, len(ledgerEntryOffsets))
	for i := len(ledgerEntryOffsets) - 1; i >= 0; i-- {
		b.builder.PrependUOffsetT(ledgerEntryOffsets[i])
	}
	ledgersOffset := b.builder.EndVector(len(ledgerEntryOffsets))

	// Build State
	commandsfb.StateStart(b.builder)
	commandsfb.StateAddLedgers(b.builder, ledgersOffset)
	commandsfb.StateAddNextLedgerId(b.builder, state.NextLedgerId)
	stateOffset := commandsfb.StateEnd(b.builder)
	b.builder.Finish(stateOffset)

	// Return a copy
	result := make([]byte, len(b.builder.FinishedBytes()))
	copy(result, b.builder.FinishedBytes())
	return result
}

// FlatBufferMetadata wraps metadata for store operations
type FlatBufferMetadata struct {
	data []byte
	fb   *commandsfb.Metadata
}

// NewFlatBufferMetadata creates a new FlatBufferMetadata from raw bytes
func NewFlatBufferMetadata(data []byte) *FlatBufferMetadata {
	return &FlatBufferMetadata{
		data: data,
		fb:   commandsfb.GetRootAsMetadata(data, 0),
	}
}

// Bytes returns the raw FlatBuffer bytes
func (m *FlatBufferMetadata) Bytes() []byte {
	return m.data
}

// ToMap converts to a Go map
func (m *FlatBufferMetadata) ToMap() map[string]string {
	count := m.fb.EntriesLength()
	if count == 0 {
		return nil
	}
	result := make(map[string]string, count)
	var entry commandsfb.EntriesEntry
	for i := 0; i < count; i++ {
		if m.fb.Entries(&entry, i) {
			result[string(entry.Key())] = string(entry.Value())
		}
	}
	return result
}

// FlatBufferMetadataBuilder builds FlatBuffer Metadata
type FlatBufferMetadataBuilder struct {
	builder *flatbuffers.Builder
}

// NewFlatBufferMetadataBuilder creates a new builder
func NewFlatBufferMetadataBuilder() *FlatBufferMetadataBuilder {
	return &FlatBufferMetadataBuilder{
		builder: flatbuffers.NewBuilder(256),
	}
}

// Build creates FlatBuffer bytes from a map
func (b *FlatBufferMetadataBuilder) Build(metadata map[string]string) []byte {
	b.builder.Reset()

	var entriesOffset flatbuffers.UOffsetT
	if len(metadata) > 0 {
		offsets := make([]flatbuffers.UOffsetT, 0, len(metadata))
		for k, v := range metadata {
			keyOffset := b.builder.CreateString(k)
			valueOffset := b.builder.CreateString(v)
			commandsfb.EntriesEntryStart(b.builder)
			commandsfb.EntriesEntryAddKey(b.builder, keyOffset)
			commandsfb.EntriesEntryAddValue(b.builder, valueOffset)
			offsets = append(offsets, commandsfb.EntriesEntryEnd(b.builder))
		}
		commandsfb.MetadataStartEntriesVector(b.builder, len(offsets))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.builder.PrependUOffsetT(offsets[i])
		}
		entriesOffset = b.builder.EndVector(len(offsets))
	}

	commandsfb.MetadataStart(b.builder)
	if entriesOffset != 0 {
		commandsfb.MetadataAddEntries(b.builder, entriesOffset)
	}
	metadataOffset := commandsfb.MetadataEnd(b.builder)
	b.builder.Finish(metadataOffset)

	// Return a copy
	result := make([]byte, len(b.builder.FinishedBytes()))
	copy(result, b.builder.FinishedBytes())
	return result
}

// FlatBufferBigInt wraps BigInt for store operations
// Format: first byte is sign (0 = positive/zero, 1 = negative), followed by absolute value bytes.
type FlatBufferBigInt struct {
	data []byte
}

// NewFlatBufferBigInt creates a new FlatBufferBigInt from raw big.Int bytes
// Format: first byte is sign (0 = positive/zero, 1 = negative), followed by absolute value bytes.
func NewFlatBufferBigInt(data []byte) *FlatBufferBigInt {
	return &FlatBufferBigInt{data: data}
}

// Bytes returns the raw bytes (sign + absolute value)
func (b *FlatBufferBigInt) Bytes() []byte {
	return b.data
}

// Value decodes the bytes to a big.Int with proper sign handling
func (b *FlatBufferBigInt) Value() *big.Int {
	if len(b.data) == 0 {
		return new(big.Int)
	}
	sign := b.data[0]
	x := new(big.Int).SetBytes(b.data[1:])
	if sign == 1 && x.Sign() != 0 {
		x.Neg(x)
	}
	return x
}
