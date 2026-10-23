package wasm

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
	"google.golang.org/protobuf/proto"
)

type fakeBucketService struct {
	servicepb.UnimplementedBucketServiceServer
	called bool
}

func (s *fakeBucketService) Apply(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.called = true
	return &servicepb.ApplyResponse{}, nil
}

func TestGRPCLedgerCallerDispatchesGenericUnaryCall(t *testing.T) {
	service := &fakeBucketService{}
	caller, err := NewGRPCLedgerCaller(service)
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()

	payload, err := proto.Marshal(&servicepb.ApplyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request, err := proto.Marshal(&wasmpb.LedgerCallRequest{Method: "Apply", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	response, err := caller.Call(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !service.called {
		t.Fatal("expected Apply to be called")
	}
	var envelope wasmpb.LedgerCallResponse
	if err := proto.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	var applyResponse servicepb.ApplyResponse
	if err := proto.Unmarshal(envelope.GetPayload(), &applyResponse); err != nil {
		t.Fatal(err)
	}
}
