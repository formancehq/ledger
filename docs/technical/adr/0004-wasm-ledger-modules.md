# 0004 — WASM modules hosted by the Ledger

**Status:** Proposed (2026-10-26). The prototype implementation is recorded in
[PR #2365](https://github.com/formance/ledger/pull/2365), which is closed. This
ADR records the architectural conclusions and the constraints that a future
implementation must address.

## Context

The Ledger needs to run user-provided integrations that read external systems
and write Ledger transactions. The Bitcoin ingestion prototype used a WASM
module that received configuration and a generic Ledger client from the host.
The module owned its polling loop, cursor policy and business mapping. The host
provided execution, network access, resource limits, logging and atomic Ledger
writes.

The prototype showed that WASM can isolate integration code, but it also showed
that the host must provide most system capabilities. A WASM module is not a
native Go plugin and must not be treated as one.

## Decision

Use WASM as an isolated integration process hosted by the Ledger. Keep the
module responsible for integration behaviour and keep the Ledger responsible
for capabilities and safety boundaries.

The host contract should provide:

- a versioned `run` entry point with module configuration;
- a generic Ledger client for reads and writes;
- network and timer host calls;
- text logging and structured execution errors;
- bounded CPU, memory, request count and execution time;
- an atomic batch operation for business writes and module progress;
- lifecycle operations to start, stop, restart and remove modules;
- metrics for execution, memory, host calls, network requests and failures.

The module should own its cursor and recover it by reading the Ledger. The host
must commit the cursor and the corresponding business writes atomically when a
module submits a batch. The host must not persist the module's linear memory as
the module's durable state.

## Constraints

### Execution is normally single-threaded

A standard WASM instance executes one call at a time. Concurrent host calls do
not make the module's guest code parallel. A blocking host call can therefore
stall the instance.

The host must move concurrency outside the instance. It can run asynchronous
network operations, prepare several batches ahead of the current Ledger write,
or run multiple isolated instances. A single instance must not be assumed to
provide parallel execution.

### Network access is a host capability

Without WASI or explicit host calls, a module cannot open sockets. Even with a
network API, the host must define DNS, TCP, TLS, proxy, timeout, cancellation,
connection reuse and response-size behaviour.

The module may build requests and parse responses, but the host controls the
actual transport. This keeps the runtime portable and makes resource limits
enforceable. It also means that network behaviour is part of the WASM ABI.

### Memory is separate from Go memory

WASM linear memory is managed by the module and is not reclaimed by the Go GC.
The runtime can observe the module's memory size, but it cannot treat guest
allocations as ordinary Go heap allocations. Large JSON payloads, network
responses and batches can grow the guest heap and keep pages reserved.

The host must enforce a memory limit and measure guest memory separately from
host memory. Interfaces should favour bounded, batched payloads over many small
cross-boundary calls.

### State must survive restarts and leader changes

An instance can disappear at any time. A module must be able to rebuild its
state from Ledger data after a process restart, pod replacement or Raft leader
change.

The durable recovery sequence is:

1. the module reads its cursor from the Ledger;
2. it fetches and prepares the next batch;
3. it submits business writes and the new cursor in one atomic Ledger request;
4. it repeats from the committed cursor.

This design avoids coupling recovery to a runtime-specific memory snapshot.

### Atomicity is a host guarantee

The module owns the mapping from external data to Ledger requests. The Ledger
owns the transaction boundary. A cursor update written separately from the
business batch can cause skipped data or duplicate ingestion after a failure.

The generic client must therefore expose one atomic batch operation. The host
must reject partial success and return a single error for the whole batch.

### The ABI needs compatibility rules

The `run` signature, configuration encoding, host calls, error format, log
format and client messages form a public ABI. Changing any of them can break
already uploaded modules.

The ABI must be versioned independently from the WASM binary. New host calls
should be additive. Removed calls and changed message fields need an explicit
migration or a compatibility version.

### Lifecycle and failure handling are explicit

The host must handle modules that loop forever, exhaust memory, exceed a
timeout, return repeated errors or stop responding to host calls. A running
module must not be deleted silently. The API needs a stop operation and must
return an error when removal is requested while the module is running.

### Isolation is not a complete security policy

WASM provides memory isolation, but a module with an unrestricted Ledger client
and network access still has broad authority. A later version must support
capability configuration for Ledger scopes, network destinations, secrets,
CPU, memory and request rates.

The MVP may keep the capability set open, but it must keep the host boundary
explicit so that restrictions can be added without changing module business
logic.

### Observability crosses two runtimes

Failures can occur in guest code, host calls, the runtime, the network or the
Ledger. Text logs alone do not identify the failing layer.

Every execution and host call should carry a module ID and execution ID. The
host should record duration, memory, in-flight calls, response sizes and error
categories. Guest logs should remain text in the MVP, with structured fields
added later if needed.

## Alternatives considered

### Native Go plugins

Native plugins would provide direct access to Go concurrency and networking,
but they share process memory and runtime assumptions with the Ledger. A bad
module can crash or corrupt the host, and binaries are tied to the host's Go
toolchain and platform. They do not provide the isolation required for
user-provided integrations.

### A specialised Bitcoin capability in the Ledger

A Bitcoin-specific host API would simplify one integration, but it would make
the Ledger aware of every external system and force a new capability for each
use case. The generic network and Ledger clients keep integration logic in the
module and make Bitcoin one example rather than a Ledger subsystem.

### Running modules as external services

External workers provide natural process isolation and concurrency, but they
need deployment, discovery, authentication and lifecycle management outside the
Ledger. They also make atomic coordination between module progress and Ledger
writes harder. WASM keeps the execution contract close to the Ledger while
retaining a portable module format.

## Consequences

This decision gives users a portable extension point and keeps external-system
logic out of the Ledger core. It also makes the host runtime, ABI, resource
limits, lifecycle and observability part of the product surface.

The main performance limit is not WASM instruction speed. It is the cost of
crossing the host boundary, serialising payloads, making network calls and
writing Ledger batches. The design should therefore batch work and pipeline
network preparation with Ledger writes.

The first implementation should target one runtime and one ABI, but it must
keep the host contract explicit enough to allow more runtimes or multiple
instances later.

## Follow-up work

- Specify and version the protobuf ABI for module configuration, `run`, errors,
  logs and host calls.
- Define memory, CPU, timeout, network and payload limits.
- Add lifecycle and status APIs with deterministic stop semantics.
- Add an atomic batch API that includes module progress metadata.
- Add restart and leader-change tests using a module that writes synthetic
  transactions in a loop.
- Measure guest memory and host-call overhead separately from Ledger throughput.
- Define capability policy before enabling untrusted user modules in production.

