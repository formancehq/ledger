package check

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCheck_CanonicalLedgerEnforcement(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrderWithEnforcementMode("ledger", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT))
	events, terminalErr := runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.Empty(t, events, "healthy paired projections must pass")

	info := engine.ledgers["ledger"].CloneVT()
	info.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
	batch := engine.store.OpenWriteSession()
	_, err := engine.attrs.Ledger.Set(batch, domain.LedgerKey{Name: "ledger"}.Bytes(), info)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// A fresh admission preload consumes the changed canonical policy even
	// though the global row and the fixture's existing cache remain intact.
	fresh, err := cache.New(1000, noop.NewMeterProvider().Meter("ledger-policy"))
	require.NoError(t, err)
	builder := plan.NewBuilder(node.NewIndexTracker(1), fresh, attributes.New(), engine.store, nil, logging.Testing(), 0)
	coverage := plan.NewCoverage()
	coverage.Add(dal.SubAttrLedger, domain.LedgerKey{Name: "ledger"}.Bytes())
	build, err := builder.Build(coverage, nil)
	defer build.ReleaseLoaders()
	require.NoError(t, err)
	require.Len(t, build.ExecutionPlan.GetAttributes(), 1)
	loaded := &commonpb.LedgerInfo{}
	require.NoError(t, loaded.UnmarshalVT(build.ExecutionPlan.GetAttributes()[0].GetValue().GetRawValue()))
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, loaded.GetDefaultEnforcementMode())

	events, terminalErr = runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	mismatches := errorsOfType(events, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_DEFAULT_ENFORCEMENT_MODE_MISMATCH)
	require.Len(t, mismatches, 1, "canonical policy corruption must be reported with intact global row and audit")
	require.Equal(t, "ledger", mismatches[0].GetLedger())
}

func TestCheck_LedgerPolicyCorruption(t *testing.T) {
	t.Parallel()
	for _, projection := range []string{"canonical", "global", "both"} {
		for _, field := range []string{"schema", "account types", "enforcement", "mode", "source", "source credentials", "source rewrite rules", "name", "deleted", "missing", "extra", "key"} {
			t.Run(projection+"/"+field, func(t *testing.T) {
				t.Parallel()
				engine := ledgerPolicyFixture(t, true)
				events, terminalErr := runMetadataAuditCheck(engine)
				require.NoError(t, terminalErr)
				require.Empty(t, events)
				info := engine.ledgers["ledger"].CloneVT()
				kind := servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LEDGER_INFO_MISMATCH
				key := "ledger"
				switch field {
				case "schema":
					info.MetadataSchema.AccountFields["status"].Type = commonpb.MetadataType_METADATA_TYPE_BOOL
					kind = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SCHEMA_MISMATCH
				case "account types":
					info.AccountTypes["users"].Pattern = "corrupt:*"
					kind = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ACCOUNT_TYPE_MISMATCH
				case "enforcement":
					info.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
					kind = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_DEFAULT_ENFORCEMENT_MODE_MISMATCH
				case "mode":
					info.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL
				case "source":
					info.MirrorSource.LedgerName = "different-upstream"
				case "source credentials":
					info.MirrorSource.GetHttp().Oauth2ClientCredentials.ClientSecret = "changed-secret"
				case "source rewrite rules":
					info.MirrorSource.RewriteRules = []*commonpb.MirrorRewriteRule{{Stop: true}}
				case "name":
					info.Name = "different-name"
				case "deleted":
					info.DeletedAt = &commonpb.Timestamp{Data: 123}
				case "missing":
					info = nil
					kind = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_MISSING_LEDGER
				case "extra", "key":
					key = "injected"
					kind = servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNAUDITED_LEDGER
				}
				batch := engine.store.OpenWriteSession()
				if field == "key" {
					writeLedgerPolicyRow(t, engine, batch, projection, "ledger", nil)
				}
				writeLedgerPolicyRow(t, engine, batch, projection, key, info)
				require.NoError(t, batch.Commit())
				events, terminalErr = runMetadataAuditCheck(engine)
				require.NoError(t, terminalErr)
				mismatches := errorsOfType(events, kind)
				want := 1
				if projection == "both" {
					want = 2
				}
				require.Len(t, mismatches, want)
				for _, mismatch := range mismatches {
					require.Equal(t, key, mismatch.GetLedger())
					require.Contains(t, mismatch.GetMessage(), "audit-derived")
					require.NotContains(t, mismatch.GetMessage(), "changed-secret")
					require.NotContains(t, mismatch.GetMessage(), "original-secret")
				}
			})
		}
	}
}

func TestCheck_LedgerPolicyEmptyCanonicalKey(t *testing.T) {
	t.Parallel()
	engine := ledgerPolicyFixture(t, false)
	events, terminalErr := runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.Empty(t, events)

	batch := engine.store.OpenWriteSession()
	writeLedgerPolicyRow(t, engine, batch, "canonical", "", engine.ledgers["ledger"].CloneVT())
	require.NoError(t, batch.Commit())

	events, terminalErr = runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	mismatches := errorsOfType(events, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNAUDITED_LEDGER)
	require.Len(t, mismatches, 1)
	require.Empty(t, mismatches[0].GetLedger())
	require.Equal(t, `canonical ledger "" unaudited ledger row differs from audit-derived state`, mismatches[0].GetMessage())
}

func ledgerPolicyFixture(t *testing.T, mirror bool) *testEngine {
	t.Helper()
	engine := newTestEngine(t)
	order := createLedgerOrder("ledger")
	create := order.GetLedgerScoped().GetCreateLedger()
	create.InitialSchema = []*commonpb.SetMetadataFieldTypeCommand{{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "status", Type: commonpb.MetadataType_METADATA_TYPE_STRING}}
	create.AccountTypes = map[string]*commonpb.AccountType{"users": {Name: "noncanonical", Pattern: "users:{id}"}}
	if mirror {
		create.Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
		create.MirrorSource = &commonpb.MirrorSourceConfig{LedgerName: "upstream", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl: "https://mirror.invalid", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientSecret: "original-secret"},
		}}}
	}
	engine.processAndCommit(order)

	return engine
}

func writeLedgerPolicyRow(t *testing.T, engine *testEngine, batch *dal.WriteSession, projection, key string, info *commonpb.LedgerInfo) {
	t.Helper()
	if projection == "canonical" || projection == "both" {
		if info == nil {
			require.NoError(t, engine.attrs.Ledger.Delete(batch, []byte(key)))
		} else {
			_, err := engine.attrs.Ledger.Set(batch, []byte(key), info)
			require.NoError(t, err)
		}
	}
	if projection == "global" || projection == "both" {
		if info == nil {
			require.NoError(t, batch.DeleteKey(dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName(key).Build()))
		} else {
			require.NoError(t, state.SaveLedger(batch, key, info))
		}
	}
}

func TestCheck_LedgerPolicyLifecycle(t *testing.T) {
	t.Parallel()
	for _, lifecycle := range []string{"normal", "mirror", "promoted"} {
		for _, deleted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deleted=%t", lifecycle, deleted), func(t *testing.T) {
				t.Parallel()
				engine := ledgerPolicyFixture(t, lifecycle != "normal")
				if lifecycle == "promoted" {
					engine.processAndCommit(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
					}}})
				}
				if lifecycle != "mirror" {
					engine.processAndCommit(updateDefaultEnforcementModeOrder("ledger", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))
				}
				if deleted {
					engine.processAndCommit(deleteLedgerOrder("ledger"))
				}
				events, terminalErr := runMetadataAuditCheck(engine)
				require.NoError(t, terminalErr)
				require.Empty(t, events, "valid promotion, policy update and deletion must remain clean")
				info := engine.ledgers["ledger"].CloneVT()
				if lifecycle == "mirror" {
					info.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
				} else {
					info.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
				}
				batch := engine.store.OpenWriteSession()
				writeLedgerPolicyRow(t, engine, batch, "canonical", "ledger", info)
				require.NoError(t, batch.Commit())
				events, terminalErr = runMetadataAuditCheck(engine)
				require.NoError(t, terminalErr)
				require.Len(t, errorsOfType(events, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_DEFAULT_ENFORCEMENT_MODE_MISMATCH), 1, "retained tombstone policies remain verifiable")
			})
		}
	}
}

func TestCheck_LedgerPolicySchemaAndTypeUpdates(t *testing.T) {
	t.Parallel()
	engine := ledgerPolicyFixture(t, false)
	apply := func(order *raftcmdpb.LedgerApplyOrder) {
		engine.processAndCommit(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: order},
		}}})
	}
	apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
		TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "score", Type: commonpb.MetadataType_METADATA_TYPE_UINT64,
	}}})
	apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "status",
	}}})
	apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{AddAccountType: &raftcmdpb.AddAccountTypeOrder{
		AccountType: &commonpb.AccountType{Name: "banks", Pattern: "banks:{id}"},
	}}})
	apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{Name: "users"}}})
	events, terminalErr := runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.Empty(t, events, "audited schema and type updates must remain clean")
	info := engine.ledgers["ledger"].CloneVT()
	info.MetadataSchema.TransactionFields["score"].Type = commonpb.MetadataType_METADATA_TYPE_BOOL
	batch := engine.store.OpenWriteSession()
	writeLedgerPolicyRow(t, engine, batch, "canonical", "ledger", info)
	require.NoError(t, batch.Commit())
	events, terminalErr = runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.Len(t, errorsOfType(events, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SCHEMA_MISMATCH), 1)
}

func TestCheck_LedgerPolicySkippedUpdate(t *testing.T) {
	t.Parallel()
	engine := ledgerPolicyFixture(t, false)
	// A successful proposal containing a skipped add must retain the original
	// account type rather than fold the rejected replacement into expectations.
	engine.processAndCommit(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS},
				Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{AddAccountType: &raftcmdpb.AddAccountTypeOrder{
					AccountType: &commonpb.AccountType{Name: "users", Pattern: "different:{id}"},
				}},
			}},
		}},
	})
	events, terminalErr := runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.Empty(t, events)
}

func TestCheck_LedgerPolicyBrokenChain(t *testing.T) {
	t.Parallel()
	engine := ledgerPolicyFixture(t, true)
	reader, err := engine.store.NewReadHandle()
	require.NoError(t, err)
	entry, err := query.ReadLastAuditEntry(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	entry.Hash[0] ^= 1
	batch := engine.store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(entry.GetSequence()).Build()
	require.NoError(t, batch.SetProto(key, entry))
	require.NoError(t, batch.Commit())
	events, terminalErr := runMetadataAuditCheck(engine)
	require.NoError(t, terminalErr)
	require.NotEmpty(t, errorsOfType(events, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH))
	for _, event := range events {
		require.NotContains(t, event.GetMessage(), "audit-derived state", "an unverified prefix cannot certify final policy")
	}
}
