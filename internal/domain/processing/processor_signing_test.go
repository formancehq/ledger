package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessRegisterSigningKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().AddSigningKey("key-001", []byte{0xAB, 0xCD}, "parent-key")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_RegisterSigningKey{
			RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
				KeyId:       "key-001",
				PublicKey:   []byte{0xAB, 0xCD},
				ParentKeyId: "parent-key",
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

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().RemoveSigningKey("key-001")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_RevokeSigningKey{
			RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
				KeyId:   "key-001",
				Cascade: false,
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

	mockStore := NewMockInMemoryStore(ctrl)
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
		Type: &raftcmdpb.Order_RevokeSigningKey{
			RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
				KeyId:   "key-001",
				Cascade: true,
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

func TestProcessSetSigningConfig(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetRequireSignatures(true)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetSigningConfig{
			SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{
				RequireSignatures: true,
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
