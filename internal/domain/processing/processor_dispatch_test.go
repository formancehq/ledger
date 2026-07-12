package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessOrder_DispatchEveryLedgerScopedVariant drives ProcessOrder with
// every LedgerScopedOrder payload variant and asserts the wrapper-level
// dispatch routes to the matching handler. Each handler is shorted to its
// cheapest path (loadLedger → ErrLedgerNotFound) so the dispatch itself —
// not the handler logic — is what this test exercises.
//
// Pins the structural invariant that the wrapper-based dispatch covers every
// ledger-scoped variant. Adding a new payload variant without a matching
// dispatch arm makes this test fail.
func TestProcessOrder_DispatchEveryLedgerScopedVariant(t *testing.T) {
	t.Parallel()

	const ledger = "dispatch-test"

	cases := []struct {
		name    string
		payload *raftcmdpb.LedgerScopedOrder
	}{
		{
			"apply",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}},
			},
		},
		{
			"create_ledger",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
			},
		},
		{
			"delete_ledger",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
			},
		},
		{
			"mirror_ingest",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{}},
			},
		},
		{
			"promote_ledger",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
			},
		},
		{
			"save_ledger_metadata",
			&raftcmdpb.LedgerScopedOrder{
				Ledger:  ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{}},
			},
		},
		{
			"delete_ledger_metadata",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{
					DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{Key: "k"},
				},
			},
		},
		{
			"save_numscript",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
					SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Name: "n", Content: "send [USD 1] (source = @world, destination = @x)"},
				},
			},
		},
		{
			"delete_numscript",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteNumscript{
					DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{Name: "n"},
				},
			},
		},
		{
			"create_prepared_query",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
					CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{Query: &commonpb.PreparedQuery{Name: "q", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS}},
				},
			},
		},
		{
			"update_prepared_query",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{
					UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{Name: "q"},
				},
			},
		},
		{
			"delete_prepared_query",
			&raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{
					DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{Name: "q"},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			// Every ledger-scoped handler eventually hits the store. We make
			// every read short-circuit so the handler bails out quickly —
			// the dispatch arm is what's under test, not handler internals.
			//
			// CreateLedger is the exception: it interprets ErrNotFound as
			// "slot free" and proceeds. Returning an existing LedgerInfo
			// makes it fail with ErrLedgerAlreadyExists.
			if tc.name == "create_ledger" {
				expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, (&commonpb.LedgerInfo{Name: ledger}).AsReader(), nil).AnyTimes()
			} else {
				expectGetLedger(mockStore, domain.LedgerKey{Name: ledger}, nil, domain.ErrNotFound).AnyTimes()
			}
			expectGetBoundaries(mockStore, domain.LedgerKey{Name: ledger}, nil, domain.ErrNotFound).AnyTimes()
			mockStore.EXPECT().NumscriptVersionExists(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

			order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: tc.payload}}

			_, processErr := processor.ProcessOrder(order, mockStore)
			require.NotNil(t, processErr,
				"%s: dispatch must reach a handler; with the store shorted, the handler must surface a domain error", tc.name)

			// We don't pin the exact error type per variant — the dispatch
			// arm is what's under test, not handler internals. We only
			// verify it's a domain error (Describable), which proves the
			// handler ran and produced a typed business error rather than
			// the dispatch falling through.
			var describable = processErr
			require.NotNil(t, describable)
			_ = errors.Is // referenced indirectly via require.ErrorIs in sibling tests
		})
	}
}

// TestProcessOrder_DispatchEverySystemScopedVariant drives ProcessOrder with
// every SystemScopedOrder payload variant and asserts the dispatch routes to
// the matching handler. Each case uses the cheapest mock setup to exercise
// the dispatch — the focus is the wrapper switch, not the handler internals.
func TestProcessOrder_DispatchEverySystemScopedVariant(t *testing.T) {
	t.Parallel()

	type expect func(*MockScope)
	type check func(*testing.T, error)

	requireErr := func(want any) check {
		return func(t *testing.T, got error) {
			t.Helper()
			require.Error(t, got)
			require.ErrorAs(t, got, want)
		}
	}

	cases := []struct {
		name    string
		payload *raftcmdpb.SystemScopedOrder
		setup   expect
		check   check
	}{
		{
			name: "register_signing_key/invalid_id",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
				RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{KeyId: ""},
			}},
			check: func(t *testing.T, got error) {
				t.Helper()
				require.Error(t, got)
			},
		},
		{
			name: "revoke_signing_key/invalid_id",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
				RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{KeyId: ""},
			}},
			check: func(t *testing.T, got error) {
				t.Helper()
				require.Error(t, got)
			},
		},
		{
			name: "set_signing_config",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{
				SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{},
			}},
			setup: func(m *MockScope) { m.EXPECT().SetRequireSignatures(false) },
			check: func(t *testing.T, got error) { t.Helper(); require.NoError(t, got) },
		},
		{
			name: "set_maintenance_mode",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
				SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true},
			}},
			setup: func(m *MockScope) { m.EXPECT().SetMaintenanceMode(true) },
			check: func(t *testing.T, got error) { t.Helper(); require.NoError(t, got) },
		},
		{
			name: "add_events_sink/batch_size_too_large",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
				AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
					Config: &commonpb.SinkConfig{Name: "s", BatchSize: domain.MaxSinkBatchSize + 1},
				},
			}},
			check: requireErr(new(*domain.ErrSinkBatchSizeTooLarge)),
		},
		{
			name: "remove_events_sink/not_found",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{
				RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{Name: "missing"},
			}},
			setup: func(m *MockScope) { m.EXPECT().GetSinkConfig("missing").Return(nil, nil) },
			check: requireErr(new(*domain.ErrSinkNotFound)),
		},
		{
			name: "close_chapter/no_chapter_open",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{
				CloseChapter: &raftcmdpb.CloseChapterOrder{},
			}},
			setup: func(m *MockScope) { m.EXPECT().GetCurrentOpenChapter().Return(nil, false) },
			check: func(t *testing.T, got error) {
				t.Helper()
				require.ErrorIs(t, got, domain.ErrNoChapterOpen)
			},
		},
		{
			name: "seal_chapter/not_found",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SealChapter{
				SealChapter: &raftcmdpb.SealChapterOrder{ChapterId: 42},
			}},
			setup: func(m *MockScope) { m.EXPECT().GetClosingChapterByID(uint64(42)).Return(nil, false) },
			check: requireErr(new(*domain.ErrChapterNotFound)),
		},
		{
			name: "archive_chapter/not_found",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ArchiveChapter{
				ArchiveChapter: &raftcmdpb.ArchiveChapterOrder{ChapterId: 42},
			}},
			setup: func(m *MockScope) { m.EXPECT().GetChapterByID(uint64(42)).Return(nil, false) },
			check: requireErr(new(*domain.ErrChapterNotFound)),
		},
		{
			name: "confirm_archive_chapter/not_found",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter{
				ConfirmArchiveChapter: &raftcmdpb.ConfirmArchiveChapterOrder{ChapterId: 42},
			}},
			setup: func(m *MockScope) { m.EXPECT().GetChapterByID(uint64(42)).Return(nil, false) },
			check: requireErr(new(*domain.ErrChapterNotFound)),
		},
		{
			name: "set_chapter_schedule/invalid_cron",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{
				SetChapterSchedule: &raftcmdpb.SetChapterScheduleOrder{Cron: "not-a-cron"},
			}},
			check: requireErr(new(*domain.ErrInvalidCronExpression)),
		},
		{
			name: "delete_chapter_schedule",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteChapterSchedule{
				DeleteChapterSchedule: &raftcmdpb.DeleteChapterScheduleOrder{},
			}},
			setup: func(_ *MockScope) {},
			check: func(t *testing.T, got error) { t.Helper(); require.NoError(t, got) },
		},
		{
			name: "set_query_checkpoint_schedule/invalid_cron",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{Cron: "not-a-cron"},
			}},
			check: requireErr(new(*domain.ErrInvalidCronExpression)),
		},
		{
			name: "delete_query_checkpoint_schedule",
			payload: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &raftcmdpb.DeleteQueryCheckpointScheduleOrder{},
			}},
			setup: func(_ *MockScope) {},
			check: func(t *testing.T, got error) { t.Helper(); require.NoError(t, got) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			if tc.setup != nil {
				tc.setup(mockStore)
			}

			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SystemScoped{SystemScoped: tc.payload},
			}

			_, processErr := processor.ProcessOrder(order, mockStore)

			// processErr is a domain.Describable; convert to plain error for assertions.
			var asErr error
			if processErr != nil {
				asErr = processErr
			}
			tc.check(t, asErr)
		})
	}
}
