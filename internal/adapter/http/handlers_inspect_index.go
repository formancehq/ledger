package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type inspectDistinctValuesJSON struct {
	Values     []any  `json:"values"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
}

type inspectFacetJSON struct {
	Value any    `json:"value"`
	Count uint64 `json:"count"`
}

type inspectFacetsJSON struct {
	Facets     []inspectFacetJSON `json:"facets"`
	HasMore    bool               `json:"hasMore"`
	NextCursor string             `json:"nextCursor,omitempty"`
}

type inspectSummaryJSON struct {
	Cardinality      uint64 `json:"cardinality"`
	Min              any    `json:"min"`
	Max              any    `json:"max"`
	EntitiesWithKey  uint64 `json:"entitiesWithKey"`
	EntitiesWithNull uint64 `json:"entitiesWithNull"`
}

// metadataValueToAny renders a decoded index value for JSON output. declaredType
// is the field's schema type for the inspected key: datetime index keys share
// the int64 encoding (decode reconstructs an int_value), so a datetime field is
// rendered as an RFC3339 string here rather than as a raw integer.
func metadataValueToAny(v *commonpb.MetadataValue, declaredType commonpb.MetadataType) any {
	if v == nil {
		return nil
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return t.StringValue
	case *commonpb.MetadataValue_IntValue:
		if commonpb.IsDatetimeType(declaredType) {
			return time.UnixMicro(t.IntValue).UTC().Format(time.RFC3339Nano)
		}

		return t.IntValue
	case *commonpb.MetadataValue_UintValue:
		return t.UintValue
	case *commonpb.MetadataValue_DatetimeValue:
		return time.UnixMicro(t.DatetimeValue).UTC().Format(time.RFC3339Nano)
	case *commonpb.MetadataValue_BoolValue:
		return t.BoolValue
	case *commonpb.MetadataValue_NullValue:
		return nil
	default:
		return nil
	}
}

func (s *Server) handleInspectIndex(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	canonical := chi.URLParam(r, "canonicalId")
	if canonical == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("index id is required"))

		return
	}

	id, err := indexes.ParseCanonical(canonical)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	// Inspect only makes sense on metadata indexes. Builtin indexes have no
	// per-key value space to scan.
	metaID := id.GetMetadata()
	if metaID == nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("inspect is only supported on metadata indexes, got %s", canonical))

		return
	}

	targetType := metaID.GetTarget()
	metadataKey := metaID.GetKey()

	var mode servicepb.InspectIndexMode
	switch r.URL.Query().Get("mode") {
	case "distinctValues", "distinct-values":
		mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES
	case "facets":
		mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS
	default:
		mode = servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY
	}

	var pageSize uint32
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		v, err := strconv.ParseUint(ps, 10, 32)
		if err != nil {
			writeBadRequest(w, "INVALID_PAGE_SIZE", err)

			return
		}

		pageSize = uint32(v)
	}

	resp, err := s.backend.InspectIndex(r.Context(), &servicepb.InspectIndexRequest{
		Ledger:      ledgerName,
		TargetType:  targetType,
		MetadataKey: metadataKey,
		Mode:        mode,
		PageSize:    pageSize,
		Cursor:      r.URL.Query().Get("cursor"),
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	declaredType := s.declaredMetadataType(r.Context(), ledgerName, targetType, metadataKey)

	switch result := resp.GetResult().(type) {
	case *servicepb.InspectIndexResponse_DistinctValues:
		dv := result.DistinctValues
		values := make([]any, len(dv.GetValues()))

		for i, v := range dv.GetValues() {
			values[i] = metadataValueToAny(v, declaredType)
		}

		writeOK(w, &inspectDistinctValuesJSON{
			Values:     values,
			HasMore:    dv.GetHasMore(),
			NextCursor: dv.GetNextCursor(),
		})

	case *servicepb.InspectIndexResponse_Facets:
		f := result.Facets
		facets := make([]inspectFacetJSON, len(f.GetFacets()))

		for i, fv := range f.GetFacets() {
			facets[i] = inspectFacetJSON{
				Value: metadataValueToAny(fv.GetValue(), declaredType),
				Count: fv.GetCount(),
			}
		}

		writeOK(w, &inspectFacetsJSON{
			Facets:     facets,
			HasMore:    f.GetHasMore(),
			NextCursor: f.GetNextCursor(),
		})

	case *servicepb.InspectIndexResponse_Summary:
		s := result.Summary
		writeOK(w, &inspectSummaryJSON{
			Cardinality:      s.GetCardinality(),
			Min:              metadataValueToAny(s.GetMin(), declaredType),
			Max:              metadataValueToAny(s.GetMax(), declaredType),
			EntitiesWithKey:  s.GetEntitiesWithKey(),
			EntitiesWithNull: s.GetEntitiesWithNull(),
		})
	}
}

// declaredMetadataType returns the schema-declared MetadataType for
// (ledger, targetType, key), or METADATA_TYPE_STRING when the ledger lookup
// fails or the key has no declaration. It is a render hint only: a failed
// lookup degrades to the default (raw integer) rendering rather than erroring.
func (s *Server) declaredMetadataType(ctx context.Context, ledgerName string, targetType commonpb.TargetType, key string) commonpb.MetadataType {
	info, err := s.backend.GetLedgerByName(ctx, ledgerName)
	if err != nil {
		return commonpb.MetadataType_METADATA_TYPE_STRING
	}

	_, fs := commonpb.SchemaFieldForTarget(info.GetMetadataSchema(), targetType, key)

	return fs.GetType()
}
