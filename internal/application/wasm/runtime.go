// Package wasm contains the leader-local execution runtime for user modules.
// Durable module bytes and lifecycle state are owned by the FSM; this package
// only owns an instance while the local node is the leader.
package wasm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const (
	// MemoryLimitPages is 512 MiB (WASI/WASM pages are 64 KiB). Go's WASI
	// runtime and connectivity-style modules need more than the tiny default
	// heap even when their application state is small.
	MemoryLimitPages uint32 = 8192
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
	ledger      LedgerCaller
	log         Logger
	ledgerCalls atomic.Uint64
}

func NewRuntime(ledger LedgerCaller, logger Logger) *Runtime {
	return &Runtime{ledger: ledger, log: logger}
}

// Run validates, initializes, and runs a module until it returns or ctx is
// cancelled. The guest must export alloc, dealloc, init and run. The init and
// run return values are zero for success and non-zero for failure.
func (r *Runtime) Run(ctx context.Context, wasmBytes, config []byte) error {
	if r == nil {
		return errors.New("nil wasm runtime")
	}

	runtime := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithMemoryLimitPages(MemoryLimitPages).
		WithCloseOnContextDone(true))
	defer func() { _ = runtime.Close(ctx) }()

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		return fmt.Errorf("instantiate wasi: %w", err)
	}

	ready := make(chan struct{})
	var readyOnce sync.Once
	guestDone := make(chan error, 1)
	var executeGuest func(context.Context, api.Module) error
	host := runtime.NewHostModuleBuilder("ledger")
	host.NewFunctionBuilder().WithFunc(r.ledgerCall).Export("call")
	host.NewFunctionBuilder().WithFunc(r.logMessage).Export("log")
	host.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module) {
		readyOnce.Do(func() { close(ready) })
		if executeGuest != nil {
			guestDone <- executeGuest(ctx, module)
		}
	}).Export("ready")
	if _, err := host.Instantiate(ctx); err != nil {
		return fmt.Errorf("instantiate ledger host: %w", err)
	}

	network := newNetwork(r.log)
	defer network.closeAll()
	netHost := runtime.NewHostModuleBuilder("net")
	netHost.NewFunctionBuilder().WithFunc(network.connect).Export("connect")
	netHost.NewFunctionBuilder().WithFunc(network.read).Export("read")
	netHost.NewFunctionBuilder().WithFunc(network.write).Export("write")
	netHost.NewFunctionBuilder().WithFunc(network.request).Export("request")
	netHost.NewFunctionBuilder().WithFunc(network.pollRequest).Export("poll")
	netHost.NewFunctionBuilder().WithFunc(network.responseSize).Export("response_size")
	netHost.NewFunctionBuilder().WithFunc(network.readRequest).Export("response_read")
	netHost.NewFunctionBuilder().WithFunc(network.close).Export("close")
	if _, err := netHost.Instantiate(ctx); err != nil {
		return fmt.Errorf("instantiate network host: %w", err)
	}

	compiled, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("compile wasm: %w", err)
	}
	defer func() { _ = compiled.Close(ctx) }()

	// The module exposes its own init/run ABI. Do not invoke WASI's `_start`
	// automatically: the guest calls the ready import from main, and we invoke
	// the ABI from that same guest call stack to avoid concurrent Go/WASM calls.
	module, err := runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().
		WithStartFunctions().
		WithStderr(os.Stderr).
		WithStdout(os.Stdout))
	if err != nil {
		return fmt.Errorf("instantiate wasm: %w", err)
	}
	defer func() { _ = module.Close(ctx) }()
	alloc := module.ExportedFunction("alloc")
	dealloc := module.ExportedFunction("dealloc")
	initFn := module.ExportedFunction("init")
	runFn := module.ExportedFunction("run")
	if alloc == nil || dealloc == nil || initFn == nil || runFn == nil {
		return errors.New("wasm ABI requires alloc, dealloc, init and run exports")
	}
	executeGuest = func(callCtx context.Context, guest api.Module) error {
		ptr, err := r.writeBytes(callCtx, guest, alloc, config)
		if err != nil {
			return err
		}
		initResult, err := initFn.Call(callCtx, uint64(ptr), uint64(len(config)))
		_, _ = dealloc.Call(callCtx, uint64(ptr), uint64(len(config)))
		if err != nil {
			return fmt.Errorf("wasm init trap: %w", err)
		}
		if len(initResult) != 1 || initResult[0] != 0 {
			return fmt.Errorf("wasm init returned error code %d", firstResult(initResult))
		}
		result, err := runFn.Call(callCtx)
		if err != nil {
			return fmt.Errorf("wasm run trap: %w", err)
		}
		if len(result) != 1 || result[0] != 0 {
			return fmt.Errorf("wasm run returned error code %d", firstResult(result))
		}

		return nil
	}

	start := module.ExportedFunction("_start")
	if start == nil {
		return errors.New("wasm module does not export _start")
	}
	startErr := make(chan error, 1)
	go func() {
		_, err := start.Call(ctx)
		startErr <- err
	}()
	select {
	case err := <-guestDone:
		return err
	case err := <-startErr:
		if err != nil {
			return fmt.Errorf("wasm start trap: %w", err)
		}

		return errors.New("wasm start returned before ready")
	case <-ctx.Done():
		return ctx.Err()
	}
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
		return 0, errors.New("wasm alloc returned invalid pointer")
	}

	ptr := uint32(result[0])
	if ok := module.Memory().Write(ptr, bytes); !ok {
		return 0, errors.New("wasm memory write out of bounds")
	}

	return ptr, nil
}

func (r *Runtime) ledgerCall(ctx context.Context, module api.Module, reqPtr, reqLen, respPtr, respCap uint32) uint64 {
	callNumber := r.ledgerCalls.Add(1)
	request, ok := module.Memory().Read(reqPtr, reqLen)
	if !ok || r.ledger == nil {
		return ^uint64(0)
	}

	response, err := r.ledger(ctx, append([]byte(nil), request...))
	if err != nil {
		if r.log != nil {
			r.log(3, fmt.Sprintf("ledger.call: %v", err))
		}

		return ^uint64(0)
	}
	if uint32(len(response)) > respCap {
		if r.log != nil {
			r.log(3, fmt.Sprintf("ledger.call response exceeds capacity: %d > %d", len(response), respCap))
		}

		return ^uint64(0)
	}
	if !module.Memory().Write(respPtr, response) {
		return ^uint64(0)
	}
	if r.log != nil && callNumber%100 == 0 {
		r.log(0, fmt.Sprintf("wasm runtime memory: linear_memory_bytes=%d ledger_calls=%d request_bytes=%d response_bytes=%d", module.Memory().Size(), callNumber, len(request), len(response)))
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
