package servicepb

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"google.golang.org/protobuf/proto"
)

func TestPeekBatchDecodesSignedPayload(t *testing.T) {
	t.Parallel()

	want := &ApplyBatch{
		IdempotencyKey: "test-key",
		Requests: []*Request{{
			Type: &Request_CreateLedger{
				CreateLedger: &CreateLedgerRequest{Name: "test-ledger"},
			},
		}},
	}
	payload, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal signed payload: %v", err)
	}

	got, err := PeekBatch(SignedApplyRequest(&signaturepb.SignedApplyBatch{Payload: payload}))
	if err != nil {
		t.Fatalf("peek signed payload: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("peeked batch differs: got %v, want %v", got, want)
	}
}
