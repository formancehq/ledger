//go:build wasip1

// Command dummy is a tiny WASI module used by the e2e tests. It exercises the
// same ABI as real modules by creating one transaction on every polling tick.
package main

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
)

//go:wasmimport ledger call
func ledgerCall(requestPtr, requestLen, responsePtr, responseCap uint32) uint64

//go:wasmimport ledger log
func ledgerLog(level, textPtr, textLen uint32)

//go:wasmimport ledger ready
func ledgerReady()

var moduleConfig struct {
	Ledger       string `json:"ledger"`
	PollInterval uint64 `json:"poll_interval_ms"`
}

func main() {
	ledgerReady()
	for {
		time.Sleep(time.Hour)
	}
}

//go:wasmexport alloc
func alloc(size uint32) uint32 {
	if size == 0 {
		return 0
	}
	b := make([]byte, size)
	return uint32(uintptr(unsafe.Pointer(&b[0])))
}

//go:wasmexport dealloc
func dealloc(uint32, uint32) {}

//go:wasmexport init
func initModule(ptr, length uint32) uint32 {
	if err := json.Unmarshal(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length), &moduleConfig); err != nil {
		return 1
	}
	if moduleConfig.Ledger == "" {
		moduleConfig.Ledger = "wasm-e2e"
	}
	if moduleConfig.PollInterval == 0 {
		moduleConfig.PollInterval = 100
	}
	_, _ = call("CreateLedger", mustMarshal(&servicepb.CreateLedgerRequest{Name: moduleConfig.Ledger}))
	return 0
}

//go:wasmexport run
func run() uint32 {
	var sequence uint64
	for {
		payload := &servicepb.CreateTransactionPayload{Postings: []*commonpb.Posting{{
			Source: "world", Destination: "wasm", Amount: &commonpb.Uint256{V0: 1}, Asset: "USD",
		}}}
		action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}
		apply := &servicepb.LedgerApplyRequest{Ledger: moduleConfig.Ledger, Action: action}
		batch := &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: &servicepb.ApplyBatch{
			IdempotencyKey: fmt.Sprintf("wasm-dummy:%d", sequence),
			Requests:       []*servicepb.Request{{Type: &servicepb.Request_Apply{Apply: apply}}},
		}}}
		if _, err := call("Apply", mustMarshal(batch)); err != nil {
			logText(2, err.Error())
		}
		sequence++
		time.Sleep(time.Duration(moduleConfig.PollInterval) * time.Millisecond)
	}
}

func call(method string, payload []byte) ([]byte, error) {
	request, err := (&wasmpb.LedgerCallRequest{Method: method, Payload: payload}).MarshalVT()
	if err != nil {
		return nil, err
	}
	response := make([]byte, 1<<20)
	requestPtr := alloc(uint32(len(request)))
	responsePtr := alloc(uint32(len(response)))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(requestPtr))), len(request)), request)
	result := ledgerCall(requestPtr, uint32(len(request)), responsePtr, uint32(len(response)))
	if result == ^uint64(0) || result > uint64(len(response)) {
		return nil, fmt.Errorf("ledger.call failed")
	}
	var envelope wasmpb.LedgerCallResponse
	if err := envelope.UnmarshalVT(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(responsePtr))), result)); err != nil {
		return nil, err
	}
	if envelope.Error != "" {
		return nil, fmt.Errorf("ledger: %s", envelope.Error)
	}
	return envelope.Payload, nil
}

func mustMarshal(message interface{ MarshalVT() ([]byte, error) }) []byte {
	b, _ := message.MarshalVT()
	return b
}

func logText(level uint32, text string) {
	b := []byte(text)
	ptr := alloc(uint32(len(b)))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), len(b)), b)
	ledgerLog(level, ptr, uint32(len(b)))
}
