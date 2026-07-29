package ctrl

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const redactedSecret = "***REDACTED***"

// projectingCursor applies a read-only projection to each item while preserving
// the underlying cursor's lifecycle and routed pagination trailer.
type projectingCursor[T any] struct {
	inner   cursor.Cursor[T]
	project func(T) (T, error)
}

func (c *projectingCursor[T]) Next() (T, error) {
	item, err := c.inner.Next()
	if err != nil {
		var zero T

		return zero, err
	}

	return c.project(item)
}

func (c *projectingCursor[T]) Close() error {
	return c.inner.Close()
}

// NextCursor forwards routed gRPC pagination metadata when the wrapped cursor
// provides it. Local cursors return an empty token.
func (c *projectingCursor[T]) NextCursor() string {
	if upstream, ok := c.inner.(interface{ NextCursor() string }); ok {
		return upstream.NextCursor()
	}

	return ""
}

func newProjectingCursor[T any](inner cursor.Cursor[T], project func(T) (T, error)) cursor.Cursor[T] {
	return &projectingCursor[T]{inner: inner, project: project}
}

// projectAuditEntryForRead returns a deep-cloned, secret-safe representation
// for public audit reads. The persisted entry remains the authoritative,
// hash-chain-verifiable source; projected order and signed-batch bytes are not
// themselves signature/hash preimages after credentials are replaced.
func projectAuditEntryForRead(entry *auditpb.AuditEntry) (*auditpb.AuditEntry, error) {
	if entry == nil {
		return nil, nil
	}

	projected := entry.CloneVT()

	for _, item := range projected.GetItems() {
		serialized, err := projectSerializedOrderForRead(item.GetSerializedOrder())
		if err != nil {
			return nil, fmt.Errorf("projecting audit item %d: %w", item.GetOrderIndex(), err)
		}

		item.SerializedOrder = serialized
	}

	if signature := projected.GetSignature(); signature != nil && len(signature.GetPayload()) > 0 {
		payload, err := projectSignedBatchPayloadForRead(signature.GetPayload())
		if err != nil {
			return nil, fmt.Errorf("projecting signed audit payload: %w", err)
		}

		// The original Ed25519 signature covers the unredacted payload. Keeping
		// it beside projected bytes would falsely imply that those bytes verify.
		signature.Signature = nil
		signature.Payload = payload
	}

	return projected, nil
}

func projectSerializedOrderForRead(serialized []byte) ([]byte, error) {
	if len(serialized) == 0 {
		return nil, nil
	}

	order := &raftcmdpb.Order{}
	if err := order.UnmarshalVT(serialized); err != nil {
		return nil, fmt.Errorf("unmarshaling serialized order: %w", err)
	}

	redactOrderSecrets(order)

	// Use the same business-intent projection as current audit capture. This
	// also safely handles pre-EN-1558 entries that embedded OrderTechnical.
	return processing.MarshalOrderBusinessIntent(order, nil), nil
}

func projectSignedBatchPayloadForRead(payload []byte) ([]byte, error) {
	batch := &servicepb.ApplyBatch{}
	if err := batch.UnmarshalVT(payload); err != nil {
		return nil, fmt.Errorf("unmarshaling ApplyBatch: %w", err)
	}

	for _, request := range batch.GetRequests() {
		if addSink := request.GetAddEventsSink(); addSink != nil {
			redactSinkConfigSecrets(addSink.GetConfig())
		}

		if createLedger := request.GetCreateLedger(); createLedger != nil {
			redactMirrorSourceSecrets(createLedger.GetMirrorSource())
		}
	}

	projected, err := batch.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling projected ApplyBatch: %w", err)
	}

	return projected, nil
}

func redactOrderSecrets(order *raftcmdpb.Order) {
	if addSink := order.GetSystemScoped().GetAddEventsSink(); addSink != nil {
		redactSinkConfigSecrets(addSink.GetConfig())
	}

	if createLedger := order.GetLedgerScoped().GetCreateLedger(); createLedger != nil {
		redactMirrorSourceSecrets(createLedger.GetMirrorSource())
	}
}

func projectLogForRead(log *commonpb.Log) (*commonpb.Log, error) {
	if log == nil {
		return nil, nil
	}

	projected := log.CloneVT()
	payload := projected.GetPayload()

	if addedSink := payload.GetAddedEventsSink(); addedSink != nil {
		redactSinkConfigSecrets(addedSink.GetConfig())
	}

	if createdLedger := payload.GetCreateLedger(); createdLedger != nil {
		redactMirrorSourceSecrets(createdLedger.GetMirrorSource())
	}

	return projected, nil
}

func redactSinkConfigSecrets(config *commonpb.SinkConfig) {
	if config == nil {
		return
	}

	if nats := config.GetNats(); nats != nil && nats.GetUrl() != "" {
		// A NATS URL may use either user:password or token userinfo. The URL
		// is a single opaque field, so redact it as a unit to cover both forms.
		nats.Url = redactedSecret
	}

	if clickhouse := config.GetClickhouse(); clickhouse != nil && clickhouse.GetDsn() != "" {
		clickhouse.Dsn = redactedSecret
	}

	if kafka := config.GetKafka(); kafka != nil && kafka.GetSaslPassword() != "" {
		kafka.SaslPassword = redactedSecret
	}

	if httpConfig := config.GetHttp(); httpConfig != nil && httpConfig.GetSecret() != "" {
		httpConfig.Secret = redactedSecret
	}

	if databricks := config.GetDatabricks(); databricks != nil {
		switch auth := databricks.GetAuth().(type) {
		case *commonpb.DatabricksSinkConfig_Token:
			if auth.Token != "" {
				auth.Token = redactedSecret
			}
		case *commonpb.DatabricksSinkConfig_OauthM2M:
			if auth.OauthM2M != nil && auth.OauthM2M.GetClientSecret() != "" {
				auth.OauthM2M.ClientSecret = redactedSecret
			}
		}
	}
}

func redactMirrorSourceSecrets(source *commonpb.MirrorSourceConfig) {
	if source == nil {
		return
	}

	if httpSource := source.GetHttp(); httpSource != nil {
		credentials := httpSource.GetOauth2ClientCredentials()
		if credentials != nil && credentials.GetClientSecret() != "" {
			credentials.ClientSecret = redactedSecret
		}
	}

	if postgres := source.GetPostgres(); postgres != nil && postgres.GetDsn() != "" {
		postgres.Dsn = redactedSecret
	}
}
