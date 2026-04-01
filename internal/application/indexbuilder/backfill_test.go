package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

func TestSchemaRewriteBBKey_Account(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("myledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")

	// Expected format: [ledger\x00]S[targetType_byte][key]
	expected := []byte("myledger")
	expected = append(expected, 0x00, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))
	expected = append(expected, "status"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_Transaction(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("prod", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")

	expected := []byte("prod")
	expected = append(expected, 0x00, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_TRANSACTION))
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_EmptyMetadataKey(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "")

	expected := []byte("ledger")
	expected = append(expected, 0x00, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_DifferentTargetTypesProduceDifferentKeys(t *testing.T) {
	t.Parallel()

	keyAcct := schemaRewriteBBKey("ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key")
	keyTx := schemaRewriteBBKey("ledger", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "key")

	assert.NotEqual(t, keyAcct, keyTx)
}

func TestBackfillBBKey_TxBuiltin(t *testing.T) {
	t.Parallel()

	id := indexID{
		transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_Builtin{
				Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			},
		},
	}

	key := backfillBBKey("myledger", id)

	expected := []byte("myledger")
	expected = append(expected, 0x00, readstore.BackfillKindTxBuiltin, byte(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_TxMetadata(t *testing.T) {
	t.Parallel()

	id := indexID{
		transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_MetadataKey{
				MetadataKey: "category",
			},
		},
	}

	key := backfillBBKey("prod", id)

	expected := []byte("prod")
	expected = append(expected, 0x00, readstore.BackfillKindTxMetadata)
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctBuiltin(t *testing.T) {
	t.Parallel()

	id := indexID{
		account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_Builtin{
				Builtin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED,
			},
		},
	}

	key := backfillBBKey("myledger", id)

	expected := []byte("myledger")
	expected = append(expected, 0x00, readstore.BackfillKindAcctBuiltin, byte(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctMetadata(t *testing.T) {
	t.Parallel()

	id := indexID{
		account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_MetadataKey{
				MetadataKey: "role",
			},
		},
	}

	key := backfillBBKey("ledger1", id)

	expected := []byte("ledger1")
	expected = append(expected, 0x00, readstore.BackfillKindAcctMetadata)
	expected = append(expected, "role"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_LogBuiltin(t *testing.T) {
	t.Parallel()

	logIdx := commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER
	id := indexID{logBuiltin: &logIdx}

	key := backfillBBKey("myledger", id)

	expected := []byte("myledger")
	expected = append(expected, 0x00, readstore.BackfillKindLogBuiltin, byte(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_EmptyIndexID_ReturnsNil(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("ledger", indexID{})
	assert.Nil(t, key)
}

func TestBackfillIndexName_TxBuiltin(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexID{
		transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_Builtin{
				Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			},
		},
	})

	assert.Equal(t, "tx:"+commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE.String(), name)
}

func TestBackfillIndexName_TxMetadata(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexID{
		transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_MetadataKey{
				MetadataKey: "category",
			},
		},
	})

	assert.Equal(t, "tx:metadata:category", name)
}

func TestBackfillIndexName_AcctBuiltin(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexID{
		account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_Builtin{
				Builtin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED,
			},
		},
	})

	assert.Equal(t, "acct:"+commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED.String(), name)
}

func TestBackfillIndexName_AcctMetadata(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexID{
		account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_MetadataKey{
				MetadataKey: "role",
			},
		},
	})

	assert.Equal(t, "acct:metadata:role", name)
}

func TestBackfillIndexName_LogBuiltin(t *testing.T) {
	t.Parallel()

	logIdx := commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER
	name := backfillIndexName(indexID{logBuiltin: &logIdx})

	assert.Equal(t, "log:"+commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER.String(), name)
}

func TestBackfillIndexName_Unknown(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexID{})
	assert.Equal(t, "unknown", name)
}

func TestMatchesBackfillIndex_TxBuiltin(t *testing.T) {
	t.Parallel()

	a := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}}
	b := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}}

	assert.True(t, matchesBackfillIndex(a, b))
}

func TestMatchesBackfillIndex_TxBuiltinMismatch(t *testing.T) {
	t.Parallel()

	a := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}}
	b := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP},
	}}

	assert.False(t, matchesBackfillIndex(a, b))
}

func TestMatchesBackfillIndex_TxMetadata(t *testing.T) {
	t.Parallel()

	a := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
	}}
	b := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
	}}

	assert.True(t, matchesBackfillIndex(a, b))
}

func TestMatchesBackfillIndex_TxMetadataMismatch(t *testing.T) {
	t.Parallel()

	a := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
	}}
	b := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "type"},
	}}

	assert.False(t, matchesBackfillIndex(a, b))
}

func TestMatchesBackfillIndex_DifferentTypes(t *testing.T) {
	t.Parallel()

	a := indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
	}}
	b := indexID{account: &commonpb.AccountIndex{
		Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: "role"},
	}}

	assert.False(t, matchesBackfillIndex(a, b))
}

func TestMatchesBackfillIndex_LogBuiltin(t *testing.T) {
	t.Parallel()

	ledgerIdx := commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER
	dateIdx := commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE

	a := indexID{logBuiltin: &ledgerIdx}
	b := indexID{logBuiltin: &ledgerIdx}

	assert.True(t, matchesBackfillIndex(a, b))

	c := indexID{logBuiltin: &dateIdx}
	assert.False(t, matchesBackfillIndex(a, c))
}

func TestMatchesBackfillIndex_BothEmpty(t *testing.T) {
	t.Parallel()

	assert.False(t, matchesBackfillIndex(indexID{}, indexID{}))
}

func TestIsDataLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		log      *commonpb.Log
		expected bool
	}{
		{
			name:     "nil payload",
			log:      &commonpb.Log{},
			expected: false,
		},
		{
			name: "non-apply payload",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{},
				},
			},
			expected: false,
		},
		{
			name: "apply with nil log",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{},
					},
				},
			},
			expected: false,
		},
		{
			name: "apply with nil data",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "created transaction",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreatedTransaction{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "reverted transaction",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_RevertedTransaction{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "saved metadata",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_SavedMetadata{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "deleted metadata",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_DeletedMetadata{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "create index (config mutation)",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreateIndex{},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "drop index (config mutation)",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_DropIndex{},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "index ready (config mutation)",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_IndexReady{},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, isDataLog(tc.log))
		})
	}
}

func TestIsPostingIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       indexID
		expected bool
	}{
		{
			name:     "nil transaction",
			id:       indexID{},
			expected: false,
		},
		{
			name: "metadata key (not posting)",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
			}},
			expected: false,
		},
		{
			name: "address index",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS},
			}},
			expected: true,
		},
		{
			name: "source address index",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS},
			}},
			expected: true,
		},
		{
			name: "dest address index",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS},
			}},
			expected: true,
		},
		{
			name: "reference index (not posting)",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE},
			}},
			expected: false,
		},
		{
			name: "timestamp index (not posting)",
			id: indexID{transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP},
			}},
			expected: false,
		},
		{
			name: "account index (not posting)",
			id: indexID{account: &commonpb.AccountIndex{
				Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: "role"},
			}},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, isPostingIndex(tc.id))
		})
	}
}

func TestBuildBackfillConfig_TxBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	task := &backfillTask{
		ledger: "test",
		index: indexID{transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_Builtin{
				Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			},
		}},
	}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.False(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])
	assert.Empty(t, cfg.txMetadataIndexed)
	assert.Empty(t, cfg.acctMetadataIndexed)
	assert.Empty(t, cfg.logBuiltinIndexed)
}

func TestBuildBackfillConfig_TxMetadata(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	task := &backfillTask{
		ledger: "test",
		index: indexID{transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
		}},
	}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.txMetadataIndexed["category"])
	assert.False(t, cfg.txMetadataIndexed["other"])
}

func TestBuildBackfillConfig_AcctBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	task := &backfillTask{
		ledger: "test",
		index: indexID{account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_Builtin{
				Builtin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED,
			},
		}},
	}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.acctBuiltinIndexed[commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED])
}

func TestBuildBackfillConfig_AcctMetadata(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	task := &backfillTask{
		ledger: "test",
		index: indexID{account: &commonpb.AccountIndex{
			Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: "role"},
		}},
	}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.acctMetadataIndexed["role"])
}

func TestBuildBackfillConfig_LogBuiltin(t *testing.T) {
	t.Parallel()

	logIdx := commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE
	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	task := &backfillTask{
		ledger: "test",
		index:  indexID{logBuiltin: &logIdx},
	}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE])
	assert.False(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER])
}
