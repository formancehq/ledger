package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessRegisterSigningKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().AddSigningKey("key-001", []byte{0xAB, 0xCD}, "parent-key")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
					RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
						KeyId:       "key-001",
						PublicKey:   []byte{0xAB, 0xCD},
						ParentKeyId: "parent-key",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	regLog := result.GetRegisterSigningKey()
	require.NotNil(t, regLog)
	require.Equal(t, "key-001", regLog.GetKeyId())
	require.Equal(t, []byte{0xAB, 0xCD}, regLog.GetPublicKey())
	require.Equal(t, "parent-key", regLog.GetParentKeyId())
}

func TestProcessRevokeSigningKey_NoCascade(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().RemoveSigningKey("key-001")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
					RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
						KeyId:   "key-001",
						Cascade: false,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	revokeLog := result.GetRevokeSigningKey()
	require.NotNil(t, revokeLog)
	require.Equal(t, "key-001", revokeLog.GetKeyId())
	require.Empty(t, revokeLog.GetCascadedKeyIds())
}

func TestProcessRevokeSigningKey_WithCascade(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// key-001 has children: child-a, child-b
	// child-a has children: grandchild-c
	// child-b has no children
	// grandchild-c has no children
	mockStore.EXPECT().GetSigningKeyChildren("key-001").Return([]string{"child-a", "child-b"})
	mockStore.EXPECT().GetSigningKeyChildren("child-a").Return([]string{"grandchild-c"})
	mockStore.EXPECT().GetSigningKeyChildren("child-b").Return(nil)
	mockStore.EXPECT().GetSigningKeyChildren("grandchild-c").Return(nil)
	mockStore.EXPECT().RemoveSigningKey("key-001")
	mockStore.EXPECT().RemoveSigningKey("child-a")
	mockStore.EXPECT().RemoveSigningKey("child-b")
	mockStore.EXPECT().RemoveSigningKey("grandchild-c")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
					RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
						KeyId:   "key-001",
						Cascade: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	revokeLog := result.GetRevokeSigningKey()
	require.NotNil(t, revokeLog)
	require.Equal(t, "key-001", revokeLog.GetKeyId())
	require.ElementsMatch(t, []string{"child-a", "child-b", "grandchild-c"}, revokeLog.GetCascadedKeyIds())
}

// TestProcessRevokeSigningKey_CascadeVisitsEachKeyOnce pins the visited set in
// processRevokeSigningKey's BFS.
//
// Nothing validates that the signing-key parent graph is acyclic:
// processRegisterSigningKey shape-checks the two key IDs and never verifies that
// the parent exists, and registration is an upsert — so "register a under b"
// then "register b under a" is accepted end to end. Without the visited set the
// walk alternates between the two forever, inside the Raft apply path, wedging
// every replica at once and replaying on restart because the order is committed.
//
// The same set dedups cascaded_key_ids, which reaches the audit chain through
// RevokedSigningKeyLog: GetSigningKeyChildren appends the key of EVERY pending
// addition whose parent matches, so one child registered twice under one parent
// in a single proposal would otherwise be reported twice.
//
// Each case asserts the EXACT cascaded slice rather than its elements: the
// payload is hashed into the audit chain, so its order is part of the contract.
// gomock's default exactly-once expectations are the second half of the
// assertion — a re-visit would fail on an unexpected call. If a case ever hangs
// instead of failing, the visited set is gone.
func TestProcessRevokeSigningKey_CascadeVisitsEachKeyOnce(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		revoke   string
		children map[string][]string
		cascaded []string
	}{
		{
			// a parents b and b parents a: each is the other's descendant.
			name:     "parent cycle",
			revoke:   "a",
			children: map[string][]string{"a": {"b"}, "b": {"a"}},
			cascaded: []string{"b"},
		},
		{
			// The degenerate cycle. The target is pre-seeded into visited, so it is
			// skipped on sight and never lands in cascaded — RemoveSigningKey
			// already covers it.
			name:     "self parent",
			revoke:   "a",
			children: map[string][]string{"a": {"a"}},
			cascaded: nil,
		},
		{
			// A longer cycle, to show the set is not just a two-key special case.
			name:     "three-key cycle",
			revoke:   "a",
			children: map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"a"}},
			cascaded: []string{"b", "c"},
		},
		{
			// Two in-proposal registrations of the same child under one parent.
			// Acyclic, so this one is about the duplicate in the audited payload
			// rather than about termination.
			name:     "duplicate pending addition under one parent",
			revoke:   "parent",
			children: map[string][]string{"parent": {"child", "child"}, "child": nil},
			cascaded: []string{"child"},
		},
		{
			// The same key reachable through two distinct branches — a child
			// re-registered under a second parent within the revoking proposal,
			// which GetSigningKeyChildren reports for both.
			name:     "key reachable through two branches",
			revoke:   "root",
			children: map[string][]string{"root": {"a", "b"}, "a": {"shared"}, "b": {"shared"}, "shared": nil},
			cascaded: []string{"a", "b", "shared"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// Exactly one child lookup per visited key, and one removal per key the
			// walk decided on.
			for key, children := range tc.children {
				mockStore.EXPECT().GetSigningKeyChildren(key).Return(children)
			}

			mockStore.EXPECT().RemoveSigningKey(tc.revoke)

			for _, key := range tc.cascaded {
				mockStore.EXPECT().RemoveSigningKey(key)
			}

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SystemScoped{
					SystemScoped: &raftcmdpb.SystemScopedOrder{
						Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
							RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
								KeyId:   tc.revoke,
								Cascade: true,
							},
						},
					},
				},
			}

			result, err := processor.ProcessOrder(order, mockStore)
			require.NoError(t, err)
			require.NotNil(t, result)

			revokeLog := result.GetRevokeSigningKey()
			require.NotNil(t, revokeLog)
			require.Equal(t, tc.revoke, revokeLog.GetKeyId())
			require.Equal(t, tc.cascaded, revokeLog.GetCascadedKeyIds(),
				"cascaded_key_ids reaches the audit chain, so both its contents and its order are pinned")
		})
	}
}

// TestProcessRegisterSigningKey_RejectsInvalidIDs pins the validator branches
// added when the `signing keys list` pagination cursor started flowing the
// raw KeyId through the `x-next-cursor` gRPC trailer. Bad IDs must be
// rejected before the FSM mutates anything.
func TestProcessRegisterSigningKey_RejectsInvalidIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		keyID       string
		parentKeyID string
		wantErr     domain.Describable
	}{
		{name: "empty key id", keyID: "", parentKeyID: "parent", wantErr: domain.ErrSigningKeyIDRequired},
		{name: "key id with newline", keyID: "key\n001", parentKeyID: "", wantErr: domain.ErrSigningKeyIDInvalidChar},
		{name: "key id with non-ASCII", keyID: "clé", parentKeyID: "", wantErr: domain.ErrSigningKeyIDInvalidChar},
		{name: "parent key id with newline", keyID: "key-001", parentKeyID: "p\nkey", wantErr: domain.ErrSigningKeyIDInvalidChar},
		{name: "parent key id with non-ASCII", keyID: "key-001", parentKeyID: "pärent", wantErr: domain.ErrSigningKeyIDInvalidChar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// Bad IDs short-circuit before any store mutation; no AddSigningKey
			// EXPECT, so gomock will fail the test if the validator is bypassed.

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SystemScoped{
					SystemScoped: &raftcmdpb.SystemScopedOrder{
						Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
							RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
								KeyId:       tt.keyID,
								PublicKey:   []byte{0xAB, 0xCD},
								ParentKeyId: tt.parentKeyID,
							},
						},
					},
				},
			}

			result, err := processor.ProcessOrder(order, mockStore)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

func TestProcessRevokeSigningKey_RejectsInvalidIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyID   string
		wantErr domain.Describable
	}{
		{name: "empty key id", keyID: "", wantErr: domain.ErrSigningKeyIDRequired},
		{name: "key id with control byte", keyID: "key\x07id", wantErr: domain.ErrSigningKeyIDInvalidChar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// No RemoveSigningKey / GetSigningKeyChildren expectations —
			// gomock will fail if the validator does not short-circuit.

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SystemScoped{
					SystemScoped: &raftcmdpb.SystemScopedOrder{
						Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
							RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
								KeyId:   tt.keyID,
								Cascade: false,
							},
						},
					},
				},
			}

			result, err := processor.ProcessOrder(order, mockStore)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

func TestProcessSetSigningConfig(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetRequireSignatures(true)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{
					SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{
						RequireSignatures: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	configLog := result.GetSetSigningConfig()
	require.NotNil(t, configLog)
	require.True(t, configLog.GetRequireSignatures())
}
