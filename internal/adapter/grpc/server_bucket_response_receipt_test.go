package grpc

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestApplySignsBatchAndOrderedLogs(t *testing.T) {
	t.Parallel()
	impl, controller := newCheckpointWaitHarness(t)
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	impl.responseSigner = signer
	request := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "test", Name: "old"}}})
	payload, err := request.GetUnsigned().MarshalVT()
	require.NoError(t, err)
	request.ResponseBindingPayload = payload
	log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedPreparedQuery{DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{Ledger: "test", Name: "old"}}}}
	controller.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{log}}, nil)
	response, err := impl.Apply(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.GetLogs(), 1)
	require.NoError(t, signing.VerifyResponseLog(response.GetLogs()[0], signer.PublicKey()))
	require.NoError(t, signing.VerifyResponseReceipt(request, response.GetLogs(), response.GetResponseReceipt(), signer.PublicKey()))
	other := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "test", Name: "other"}}})
	other.ResponseBindingPayload, err = other.GetUnsigned().MarshalVT()
	require.NoError(t, err)
	require.ErrorContains(t, signing.VerifyResponseReceipt(other, response.GetLogs(), response.GetResponseReceipt(), signer.PublicKey()), "does not match submitted batch")
}

func TestApplyRejectsMismatchedResponseBindingBeforeCommit(t *testing.T) {
	t.Parallel()
	impl, _ := newCheckpointWaitHarness(t)
	request := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "test", Name: "old"}}})
	other := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "test", Name: "other"}}})
	var err error
	request.ResponseBindingPayload, err = other.GetUnsigned().MarshalVT()
	require.NoError(t, err)
	_, err = impl.Apply(context.Background(), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, "response binding payload does not match submitted batch")
	request.ResponseBindingPayload = []byte{0xff}
	_, err = impl.Apply(context.Background(), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
