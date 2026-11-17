package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// decodeAtomicApplyBatch accepts canonical protobuf JSON field names, with the
// existing HTTP decimal and timestamp representations. Metadata remains typed:
// its protobuf oneof identity is part of the signed business intent.
func decodeAtomicApplyBatch(data []byte) (*servicepb.ApplyBatch, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := readAtomicJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("expected a single JSON document")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("batch must be an object")
	}
	if _, ok := object["idempotencyKey"]; ok {
		return nil, errors.New("use the Idempotency-Key header")
	}
	if _, ok := object["requests"].([]any); !ok {
		return nil, errors.New("requests must be an array")
	}
	batch := new(servicepb.ApplyBatch)
	value, err = normalizeAtomicJSON(value, batch.ProtoReflect().Descriptor())
	if err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := (protojson.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(normalized, batch); err != nil {
		return nil, err
	}
	for i, request := range batch.GetRequests() {
		if request.GetType() == nil {
			return nil, fmt.Errorf("requests[%d] requires an operation", i)
		}
		if apply := request.GetApply(); apply != nil && (apply.GetAction() == nil || apply.GetAction().GetData() == nil) {
			return nil, fmt.Errorf("requests[%d].apply requires an action", i)
		}
	}

	return batch, nil
}

// Build the tree once, retaining number tokens and rejecting duplicate object
// keys before normalization. Depth is bounded before descending into input.
func readAtomicJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds 64 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if _, container := token.(json.Delim); container && depth >= 64 {
		return nil, errors.New("JSON nesting exceeds 64 levels")
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := token.(string)
			if !ok {
				return nil, errors.New("expected object key")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate field %q", key)
			}
			value, err := readAtomicJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}

		return object, nil
	case json.Delim('['):
		array := make([]any, 0)
		for decoder.More() {
			value, err := readAtomicJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}

		return array, nil
	default:
		if _, ok := token.(json.Delim); ok {
			return nil, errors.New("unexpected JSON delimiter")
		}

		return token, nil
	}
}

func normalizeAtomicJSON(value any, descriptor protoreflect.MessageDescriptor) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("%s cannot be null", descriptor.Name())
	}
	// These messages have established scalar HTTP codecs; protojson otherwise
	// expects their internal fields. Normalize only these leaves, never whole
	// subtrees, avoiding repeated subtree encoding.
	var scalar interface {
		proto.Message
		UnmarshalJSON([]byte) error
	}
	switch descriptor.FullName() {
	case "common.Uint256":
		scalar = new(commonpb.Uint256)
	case "common.BigUint":
		scalar = new(commonpb.BigUint)
	case "common.SignedBigInt":
		scalar = new(commonpb.SignedBigInt)
	case "common.Timestamp":
		scalar = new(commonpb.Timestamp)
	}
	if scalar != nil {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if descriptor.FullName() == "common.Uint256" {
			// Posting owns the public integer-token / decimal-string contract.
			var posting commonpb.Posting
			if err := posting.UnmarshalJSON(append(append([]byte(`{"amount":`), raw...), '}')); err != nil {
				return nil, err
			}
			scalar = posting.GetAmount()
		} else if err := scalar.UnmarshalJSON(raw); err != nil {
			return nil, err
		}
		encoded, err := protojson.Marshal(scalar)
		if err != nil {
			return nil, err
		}

		return json.RawMessage(encoded), nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", descriptor.Name())
	}
	for key, child := range object {
		field := descriptor.Fields().ByJSONName(key)
		if field == nil {
			return nil, fmt.Errorf("unknown field %q in %s", key, descriptor.Name())
		}
		if child == nil {
			return nil, fmt.Errorf("field %q cannot be null", key)
		}
		if field.Message() == nil {
			continue
		}
		switch {
		case field.IsMap():
			entries, ok := child.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s must be an object", key)
			}
			if field.MapValue().Message() == nil {
				continue
			}
			for k, v := range entries {
				normalized, err := normalizeAtomicJSON(v, field.MapValue().Message())
				if err != nil {
					return nil, err
				}
				entries[k] = normalized
			}
		case field.IsList():
			entries, ok := child.([]any)
			if !ok {
				return nil, fmt.Errorf("%s must be an array", key)
			}
			for i, v := range entries {
				normalized, err := normalizeAtomicJSON(v, field.Message())
				if err != nil {
					return nil, err
				}
				entries[i] = normalized
			}
		default:
			normalized, err := normalizeAtomicJSON(child, field.Message())
			if err != nil {
				return nil, err
			}
			object[key] = normalized
		}
	}

	return object, nil
}
