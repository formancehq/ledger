# WASM modules

Ledger can load user-provided WASM modules through the public gRPC
`WasmService`. Upload chunks and lifecycle changes are technical Raft
updates. The FSM stores the module configuration, metadata, and chunks in the
cluster-persistent store, so a follower can reconstruct the complete module
after a snapshot or a leader change.

`ledgerctl wasm upload <file> --config <json>` streams the module in 1 MiB
chunks. `ledgerctl wasm list`, `get`, `stop`, and `delete` expose the MVP
lifecycle operations.

Execution is leader-only. A leader loads modules after its readiness gate and
starts only modules whose persisted status is `running`. Losing leadership
cancels local instances; the next leader starts them again from `init(config)`.
The module is responsible for recovering its cursor and business state by
querying Ledger, and for making its writes idempotent.

The MVP runtime uses wazero with a 64 MiB linear-memory limit. The guest ABI
exports `alloc`, `dealloc`, `init`, and `run`, and imports opaque byte-oriented
`ledger.call` and text `ledger.log` functions. WASI is instantiated for the
guest; the Bitcoin example source and its local configuration are under
`examples/wasm/bitcoin`.

Every module also gets a generic raw TCP host module. `net.connect(host_ptr,
host_len, port)` returns a connection handle (or `0xffffffff`),
`net.read(handle, buffer_ptr, buffer_cap)` and `net.write(handle, buffer_ptr,
buffer_len)` return a byte count (or `0xffffffff`), and `net.close(handle)`
returns zero on success. Connections are closed when the module stops. There
is deliberately no Bitcoin-specific networking capability or allow-list in
this MVP; the module owns its protocol and endpoint configuration.

`ledger.call` receives and returns serialized `LedgerCallRequest` and
`LedgerCallResponse` messages. The request names a unary `BucketService` RPC
and carries that RPC's protobuf payload. The runtime dispatches the call over
an in-process gRPC client, so the module uses the normal Ledger service
implementation without a public network hop.

The versioned Bitcoin example uses `getblockcount`, `getblockhash`, and
verbose `getblock` JSON-RPC calls, maps regular and coinbase flows into
`BTC/8` postings, and submits one idempotent atomic Apply batch per block.
Its configuration template is [config.example.json](/Users/gfyrag/Work/Formance/Projects/dataplane/modules/ledger/examples/wasm/bitcoin/config.example.json).
The lifecycle e2e test uses `examples/wasm/dummy`, built with `just wasm-dummy`,
so it does not depend on a Bitcoin node.

An interrupted upload is removed through a replicated delete update. A module
that traps, returns a non-zero code, or exceeds the five-second stop deadline
is retained with status `error` and its latest error message.
