# Uint256 Wire Format for Monetary Amounts

## Context

All monetary amounts in the ledger (posting amounts, volume inputs/outputs) are **non-negative integers**. The system never stores negative balances at the volume level — negative balances are derived at read time as `input - output`.

The original implementation used a `BigInt` protobuf message with a variable-length `bytes` field encoding `[sign_byte][big-endian magnitude]`. While flexible, this format introduces overhead on the hot path:

- **Encoding**: requires `big.Int.Bytes()` (allocates a byte slice) + prepend a sign byte
- **Decoding**: requires `big.Int.SetBytes()` (allocates internal `nat` storage)
- **Every volume read/write** in the FSM goes through this conversion

Since the FSM is called synchronously during Raft entry application, serialization overhead directly impacts transaction throughput.

## Decision

Replace `BigInt` with `Uint256` — a fixed-size message of 4 `fixed64` fields representing the four 64-bit limbs of a 256-bit unsigned integer.

```protobuf
message Uint256 {
  fixed64 v0 = 1;  // least significant limb
  fixed64 v1 = 2;
  fixed64 v2 = 3;
  fixed64 v3 = 4;  // most significant limb
}
```

This message maps directly to the `[4]uint64` internal layout of [`holiman/uint256.Int`](https://github.com/holiman/uint256), the library used for arithmetic on the hot path.

## Why not native `*big.Int`?

Go's `math/big.Int` is the standard arbitrary-precision integer, but it has properties that make it suboptimal for the hot path:

| Concern | `*big.Int` | `uint256.Int` |
|---------|-----------|---------------|
| **Allocation** | Heap-allocated; each operation may allocate | Stack-friendly `[4]uint64` value type |
| **Serialization** | `Bytes()` allocates a new `[]byte` | Direct `uint64` copy — no allocation |
| **GC pressure** | Pointer-heavy (`*nat` slice inside) | Zero pointers — invisible to GC |
| **Arithmetic** | General-purpose (handles arbitrary size) | Unrolled 256-bit operations, ~2-10x faster |
| **Range** | Unlimited | 0 to 2^256-1 (sufficient for any monetary system) |

The `uint256.Int` library was originally designed for Ethereum's EVM (which uses 256-bit words) and provides constant-time, allocation-free arithmetic. Since our volumes never exceed 256 bits (2^256 ≈ 1.16 × 10^77, far beyond any real-world monetary quantity), this is a safe upper bound.

## Wire format benefits

| Property | Old `BigInt` (bytes) | New `Uint256` (4 × fixed64) |
|----------|---------------------|------------------------------|
| **Wire size (zero)** | 2 bytes (sign + empty) | 0 bytes (all zero defaults) |
| **Wire size (small, e.g. 100)** | 3 bytes | 8 bytes (1 fixed64) |
| **Wire size (large, 128-bit)** | ~18 bytes | 16 bytes (2 fixed64) |
| **Wire size (max, 256-bit)** | ~34 bytes | 32 bytes (4 fixed64) |
| **Encode cost** | `Bytes()` alloc + copy | 4 × `uint64` copy |
| **Decode cost** | `SetBytes()` alloc + copy | 4 × `uint64` copy |
| **vtprotobuf compat** | Yes (bytes field) | Yes (fixed64 fields) |

For typical ledger amounts (fitting in 64 bits), the wire size is comparable. For the hot path, the key win is **zero allocation**: converting between proto and `uint256.Int` is just 4 uint64 assignments.

## Code structure

### Helper methods (`internal/proto/commonpb/uint256.go`)

```go
// Zero-allocation: direct limb copy from proto → uint256.Int
func (u *Uint256) IntoUint256(dst *uint256.Int)

// Zero-allocation: direct limb copy from uint256.Int → proto
func (u *Uint256) SetFromUint256(v *uint256.Int)

// Allocating constructor (non-hot-path)
func NewUint256(v *uint256.Int) *Uint256

// Display/non-hot-path: converts to *big.Int
func (u *Uint256) ToBigInt() *big.Int

// Fast zero check (no conversion needed)
func (u *Uint256) IsZero() bool
```

### BigInt removed

The `BigInt` protobuf message and its Go helpers have been **fully removed** — no proto field references it anymore. The numscript interpreter (`github.com/formancehq/numscript`) operates on `*big.Int` (Go standard library), so conversion happens at the edge via `Uint256.ToBigInt()` and `NewPosting()`:

```
numscript *big.Int  →  uint256.Int  →  Uint256 (proto)
     ↑                                     ↓
     └──────── ToBigInt() ←───────── IntoUint256()
```

Note: the checker (`internal/service/check/checker.go`) uses `*big.Int` from the standard library for its internal arithmetic — this is unrelated to the removed `BigInt` proto message and is fine for a non-hot-path verification tool.

### Hot-path zero-allocation flow

During FSM entry application (the critical path):

1. **Volume read**: `volumePair.InputKnown.IntoUint256(&v)` — 4 uint64 copies, no alloc
2. **Arithmetic**: `v.Add(&v, &amount)` — in-place 256-bit add, no alloc
3. **Volume write**: `volumePair.InputKnown.SetFromUint256(&v)` — 4 uint64 copies, no alloc

The entire apply-posting path touches zero heap allocations for volume arithmetic.

## Affected protobuf messages

| Message | Fields changed |
|---------|----------------|
| `Posting` | `amount` |
| `VolumePair` | `input_known`, `input_diff`, `output_known`, `output_diff` |
| `PreloadVolume` | `input`, `output` |
| `VolumeAttributeSnapshotEntry` | `input_known`, `input_diff`, `output_known`, `output_diff` |

## Trade-offs

- **Fixed size vs variable size**: `Uint256` always uses up to 32 bytes on wire even for small values, while `BigInt` could be smaller for small values. In practice, protobuf's default-value elision means zero limbs are not encoded at all, so a value like `100` only encodes 1 `fixed64` (8 bytes) vs `BigInt`'s 3 bytes. The hot-path savings far outweigh the small wire overhead.
- **256-bit limit**: Amounts are capped at 2^256-1. This is intentional — no real-world monetary system needs more than 77 decimal digits of precision.
- **Numscript boundary**: The numscript library still uses `*big.Int`, so there is a conversion at the script execution boundary. This is acceptable since numscript execution is not the bottleneck (volume reads/writes are).
