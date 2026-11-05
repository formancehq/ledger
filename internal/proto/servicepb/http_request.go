package servicepb

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// DecodeHTTPRequest derives the complete batch intent from an HTTP request.
// It does not verify its signature. Admission must authenticate the retained
// request before trusting this result; authorization may inspect it beforehand.
// Both paths use this decoder so no separately supplied batch can substitute
// different operations for the signed HTTP bytes.
func DecodeHTTPRequest(request *signaturepb.SignedHTTPRequest) (*ApplyBatch, error) {
	if request == nil {
		return nil, errors.New("empty signed HTTP request")
	}
	target := request.GetRequestTarget()
	if !strings.HasPrefix(target, "/") || strings.HasPrefix(target, "//") {
		return nil, errors.New("signed HTTP request requires an origin-form target")
	}
	parsed, err := url.ParseRequestURI(target)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP request target: %w", err)
	}
	if !utf8.ValidString(parsed.Path) {
		return nil, errors.New("HTTP request path is not UTF-8")
	}
	// Chi matches RawPath when present and Path otherwise. Ledger names and
	// transaction IDs retain that representation; metadata keys decode once.
	path := parsed.Path
	if parsed.RawPath != "" {
		path = parsed.RawPath
	}
	parts := strings.Split(path, "/")
	if len(parts) < 4 || parts[0] != "" || parts[1] != "v3" {
		return nil, errors.New("unsupported signed HTTP route")
	}
	ledger := parts[2]
	if err := invariants.ValidateLedgerName(ledger); err != nil {
		return nil, fmt.Errorf("invalid HTTP ledger name: %w", err)
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP query string: %w", err)
	}
	bulk := len(parts) == 4 && parts[3] == "bulk" && request.GetMethod() == http.MethodPost
	for name := range query {
		if !bulk || (name != "atomic" && name != "continueOnFailure") {
			return nil, fmt.Errorf("unknown HTTP query parameter %q", name)
		}
	}
	if bulk {
		return decodeHTTPBulk(request, ledger, query)
	}
	if parts[3] != "transactions" {
		return nil, errors.New("unsupported signed HTTP route")
	}
	var action *LedgerAction
	switch {
	case len(parts) == 4 && request.GetMethod() == http.MethodPost:
		payload := &CreateTransactionPayload{}
		if err := decodeHTTPJSON(request.GetBody(), payload); err != nil {
			return nil, err
		}
		action = &LedgerAction{Data: &LedgerAction_CreateTransaction{CreateTransaction: payload}}
	case len(parts) == 6 || len(parts) == 7:
		id, err := strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP transaction id: %w", err)
		}
		action, err = decodeHTTPTransactionAction(request, parsed.RawPath != "", parts[5:], id)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported signed HTTP route")
	}

	return &ApplyBatch{
		IdempotencyKey: request.GetIdempotencyKey(),
		Requests:       []*Request{httpApplyRequest(ledger, action, nil)},
	}, nil
}

func decodeHTTPTransactionAction(request *signaturepb.SignedHTTPRequest, escapedPath bool, suffix []string, id uint64) (*LedgerAction, error) {
	switch {
	case len(suffix) == 1 && suffix[0] == "revert" && request.GetMethod() == http.MethodPost:
		var body struct {
			Force           bool           `json:"force"`
			AtEffectiveDate bool           `json:"atEffectiveDate"`
			Metadata        map[string]any `json:"metadata"`
		}
		if len(request.GetBody()) != 0 {
			if !stdjson.Valid(request.GetBody()) {
				return nil, errors.New("invalid HTTP request body: expected a single JSON value")
			}
			decoder := stdjson.NewDecoder(bytes.NewReader(request.GetBody()))
			decoder.UseNumber()
			if err := decoder.Decode(&body); err != nil {
				return nil, fmt.Errorf("invalid HTTP revert body: %w", err)
			}
		}
		metadata, err := commonpb.MetadataFromAnyMap(body.Metadata)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP revert metadata: %w", err)
		}

		return &LedgerAction{Data: &LedgerAction_RevertTransaction{RevertTransaction: &RevertTransactionPayload{
			TransactionId: id, Force: body.Force, AtEffectiveDate: body.AtEffectiveDate, Metadata: metadata,
		}}}, nil
	case len(suffix) == 1 && suffix[0] == "metadata" && request.GetMethod() == http.MethodPost:
		if !stdjson.Valid(request.GetBody()) {
			return nil, errors.New("invalid HTTP metadata body: expected a single JSON value")
		}
		var body map[string]any
		if err := json.UnmarshalReadUseNumber(bytes.NewReader(request.GetBody()), &body); err != nil {
			return nil, fmt.Errorf("invalid HTTP metadata body: %w", err)
		}
		metadata, err := commonpb.MetadataFromAnyMap(body)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP metadata: %w", err)
		}

		return &LedgerAction{Data: &LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}, Metadata: metadata,
		}}}, nil
	case len(suffix) == 2 && suffix[0] == "metadata" && request.GetMethod() == http.MethodDelete:
		if len(request.GetBody()) != 0 {
			return nil, errors.New("signed HTTP metadata deletion requires an empty body")
		}
		key := suffix[1]
		if escapedPath {
			var err error
			key, err = url.PathUnescape(key)
			if err != nil {
				return nil, fmt.Errorf("invalid HTTP metadata key: %w", err)
			}
		}
		if key == "" {
			return nil, errors.New("HTTP metadata key is required")
		}

		return &LedgerAction{Data: &LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}, Key: key,
		}}}, nil
	default:
		return nil, errors.New("unsupported signed HTTP route")
	}
}

func decodeHTTPBulk(request *signaturepb.SignedHTTPRequest, ledger string, query url.Values) (*ApplyBatch, error) {
	if query.Get("atomic") != "true" {
		return nil, errors.New("signed HTTP bulk requires atomic=true")
	}
	var elements []*BulkElement
	if err := decodeHTTPJSON(request.GetBody(), &elements); err != nil {
		return nil, err
	}
	if len(elements) == 0 {
		return nil, errors.New("signed HTTP bulk requires at least one element")
	}
	batch := &ApplyBatch{IdempotencyKey: request.GetIdempotencyKey(), Requests: make([]*Request, len(elements))}
	for i, element := range elements {
		if element == nil {
			return nil, fmt.Errorf("HTTP bulk element %d is null", i)
		}
		batch.Requests[i] = httpApplyRequest(ledger, element.Action, element.SkippableReasons)
	}

	return batch, nil
}

func decodeHTTPJSON(body []byte, target any) error {
	if !stdjson.Valid(body) {
		return errors.New("invalid HTTP request body: expected a single JSON value")
	}
	if err := json.UnmarshalRead(bytes.NewReader(body), target); err != nil {
		return fmt.Errorf("invalid HTTP request body: %w", err)
	}

	return nil
}

func httpApplyRequest(ledger string, action *LedgerAction, skippable []commonpb.ErrorReason) *Request {
	return &Request{Type: &Request_Apply{Apply: &LedgerApplyRequest{Ledger: ledger, Action: action, SkippableReasons: skippable}}}
}
