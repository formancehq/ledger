package ledgerv3_test

import (
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestB2InternalStubsAreReachable(t *testing.T) {
	if got := string((&servicepb.ApplyBatch{}).ProtoReflect().Descriptor().FullName()); got != "ledger.ApplyBatch" {
		t.Fatalf("ApplyBatch = %q", got)
	}
	if got := string((&signaturepb.SignedApplyBatch{}).ProtoReflect().Descriptor().FullName()); got != "signature.SignedApplyBatch" {
		t.Fatalf("SignedApplyBatch = %q", got)
	}
	service := servicepb.File_bucket_proto.Services().ByName(protoreflect.Name("BucketService"))
	if service == nil || service.Methods().ByName(protoreflect.Name("Apply")) == nil {
		t.Fatal("BucketService.Apply descriptor is unavailable")
	}
	_ = sdk.CapabilitySignLedgerApplyBatch
}
