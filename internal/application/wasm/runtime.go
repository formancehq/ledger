// Package wasm contains the leader-local execution runtime for user modules.
// Durable module bytes and lifecycle state are owned by the FSM; this package
// only owns an instance while the local node is the leader.
package wasm

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	// MemoryLimitPages is 64 MiB (WASI/WASM pages are 64 KiB).
	MemoryLimitPages uint32 = 1024
)

// LedgerCaller is the generic opaque Ledger bridge exposed to the guest. The
// request and response are protobuf bytes; the runtime intentionally does not
// know their schema or implement service-specific capabilities.
type LedgerCaller func(context.Context, []byte) ([]byte, error)

// Logger receives guest debug messages. The caller normally adapts this to the
// Ledger logger so module logs never become durable state.
type Logger func(level uint32, text string)

// Runtime executes one module instance. A Runtime is not safe for concurrent
// Run calls, but its host callbacks may be invoked from the guest goroutine.
type Runtime struct {
	ledger LedgerCaller
	log    Logger
}

func NewRuntime(ledger LedgerCaller, logger Logger) *Runtime {
	return &Runtime{ledger: ledger, log: logger}
}

// Run validates, initializes, and runs a module until it returns or ctx is
// cancelled. The guest must export alloc, dealloc, init and run. The init and
// run return values are zero for success and non-zero for failure.
func (r *Runtime) Run(ctx context.Context, wasmBytes, config []byte) error {
	if r == nil {
		return fmt.Errorf("nil wasm runtime")
	}

	runtime := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithMemoryLimitPages(MemoryLimitPages).
		WithCloseOnContextDone(true))
	defer func() { _ = runtime.Close(ctx) }()

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		return fmt.Errorf("instantiate wasi: %w", err)
	}

	host := runtime.NewHostModuleBuilder("ledger")
	host.NewFunctionBuilder().WithFunc(r.ledgerCall).Export("call")
	host.NewFunctionBuilder().WithFunc(r.logMessage).Export("log")
	if _, err := host.Instantiate(ctx); err != nil {
		return fmt.Errorf("instantiate ledger host: %w", err)
	}

	network := newNetwork()
	defer network.closeAll()
	netHost := runtime.NewHostModuleBuilder("net")
	netHost.NewFunctionBuilder().WithFunc(network.connect).Export("connect")
	netHost.NewFunctionBuilder().WithFunc(network.read).Export("read")
	netHost.NewFunctionBuilder().WithFunc(network.write).Export("write")
	netHost.NewFunctionBuilder().WithFunc(network.close).Export("close")
	if _, err := netHost.Instantiate(ctx); err != nil {
		return fmt.Errorf("instantiate network host: %w", err)
	}

	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("compile wasm: %w", err)
	}
	defer compiled.Close(ctx)

	module, err := runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		return fmt.Errorf("instantiate wasm: %w", err)
	}
	defer module.Close(ctx)

	alloc := module.ExportedFunction("alloc")
	dealloc := module.ExportedFunction("dealloc")
	initFn := module.ExportedFunction("init")
	runFn := module.ExportedFunction("run")
	if alloc == nil || dealloc == nil || initFn == nil || runFn == nil {
		return fmt.Errorf("wasm ABI requires alloc, dealloc, init and run exports")
	}

	ptr, err := r.writeBytes(ctx, module, alloc, config)
	if err != nil {
		return err
	}
	initResult, err := initFn.Call(ctx, uint64(ptr), uint64(len(config)))
	_, _ = dealloc.Call(ctx, uint64(ptr), uint64(len(config)))
	if err != nil {
		return fmt.Errorf("wasm init trap: %w", err)
	}
	if len(initResult) != 1 || initResult[0] != 0 {
		return fmt.Errorf("wasm init returned error code %d", firstResult(initResult))
	}

	result, err := runFn.Call(ctx)
	if err != nil {
		return fmt.Errorf("wasm run trap: %w", err)
	}
	if len(result) != 1 || result[0] != 0 {
		return fmt.Errorf("wasm run returned error code %d", firstResult(result))
	}

	return nil
}

func firstResult(values []uint64) uint64 {
	if len(values) == 0 {
		return ^uint64(0)
	}

	return values[0]
}

func (r *Runtime) writeBytes(ctx context.Context, module api.Module, alloc api.Function, bytes []byte) (uint32, error) {
	result, err := alloc.Call(ctx, uint64(len(bytes)))
	if err != nil {
		return 0, fmt.Errorf("wasm alloc trap: %w", err)
	}
	if len(result) != 1 || result[0] > uint64(^uint32(0)) {
		return 0, fmt.Errorf("wasm alloc returned invalid pointer")
	}

	ptr := uint32(result[0])
	if ok := module.Memory().Write(ptr, bytes); !ok {
		return 0, fmt.Errorf("wasm memory write out of bounds")
	}

	return ptr, nil
}

func (r *Runtime) ledgerCall(ctx context.Context, module api.Module, reqPtr, reqLen, respPtr, respCap uint32) uint64 {
	request, ok := module.Memory().Read(reqPtr, reqLen)
	if !ok || r.ledger == nil {
		return ^uint64(0)
	}

	response, err := r.ledger(ctx, append([]byte(nil), request...))
	if err != nil || uint32(len(response)) > respCap {
		return ^uint64(0)
	}
	if !module.Memory().Write(respPtr, response) {
		return ^uint64(0)
	}

	return uint64(len(response))
}

func (r *Runtime) logMessage(_ context.Context, module api.Module, level, ptr, length uint32) {
	if r.log == nil {
		return
	}
	text, ok := module.Memory().Read(ptr, length)
	if !ok {
		return
	}

	r.log(level, string(text))
}

// SlogLogger adapts the standard Ledger slog logger to the runtime Logger
// callback. It is intentionally a small adapter so the runtime remains easy
// to unit-test without importing Ledger's logging package.
func SlogLogger(logger *slog.Logger) Logger {
	return func(level uint32, text string) {
		switch level {
		case 0:
			logger.Debug(text)
		case 1:
			logger.Info(text)
		case 2:
			logger.Warn(text)
		default:
			logger.Error(text)
		}
	}
}
