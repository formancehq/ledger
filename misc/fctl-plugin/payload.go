package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type payloadValidator func(json.RawMessage, string) error

// These validators check the public HTTP payload shape before either executor.
// Missing fields and null optional fields retain the server's defaults. Unknown
// fields are left untouched for future contracts; schema, account existence,
// balance checks and other state-dependent rules remain server responsibilities.
// Validation never remarshals the body or decodes a number through float64.
func validateTransactionPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"postings":        payloadArray(validatePostingPayload),
		"script":          validateScriptPayload,
		"scriptReference": validateScriptReferencePayload,
		"timestamp":       validateJSONString,
		"reference":       validateJSONString,
		"metadata":        validateMetadataPayload,
		"accountMetadata": payloadMap(validateMetadataPayload),
		"force":           validateJSONBool,
	})
}

func validatePostingPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"source": validateJSONString, "destination": validateJSONString,
		"asset": validateJSONString, "color": validateJSONString,
		"amount": validatePostingAmount,
	})
}

func validateScriptPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"plain": validateJSONString, "vars": payloadMap(validateJSONString),
	})
}

func validateScriptReferencePayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"name": validateJSONString, "version": validateJSONString,
		"vars": payloadMap(validateJSONString),
	})
}

func validateRevertPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"force": validateJSONBool, "atEffectiveDate": validateJSONBool,
		"metadata": validateMetadataPayload,
	})
}

func validateLedgerPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"metadata": validateMetadataPayload, "mode": validateJSONString,
		"defaultEnforcementMode": validateJSONString,
		"mirrorSource":           validateMirrorPayload,
		"initialSchema": payloadArray(func(raw json.RawMessage, path string) error {
			return payloadFields(raw, path, map[string]payloadValidator{
				"targetType": validateJSONString, "key": validateJSONString, "type": validateJSONString,
			})
		}),
		"accountTypes": payloadMap(validateAccountTypePayload),
	})
}

func validateMirrorPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"ledgerName": validateJSONString, "type": validateJSONString,
		"baseUrl": validateJSONString, "dsn": validateJSONString,
		"oauth2ClientId": validateJSONString, "oauth2ClientSecret": validateJSONString,
		"oauth2TokenEndpoint": validateJSONString,
		"oauth2Scopes":        payloadArray(validateJSONString),
		"batchSize":           validateJSONUint32,
		"rewriteRules": payloadArray(func(raw json.RawMessage, path string) error {
			_, err := payloadObject(raw, path)

			return err
		}),
	})
}

func validateAccountTypePayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{
		"name": validateJSONString, "pattern": validateJSONString, "persistence": validateJSONString,
		"segmentTypes": payloadMap(func(raw json.RawMessage, path string) error {
			return payloadFields(raw, path, map[string]payloadValidator{
				"type": validateJSONString, "regex": validateJSONString,
			})
		}),
	})
}

func validateIndexPayload(raw json.RawMessage, path string) error {
	return payloadFields(raw, path, map[string]payloadValidator{"id": validateJSONString})
}

func validateMetadataPayload(raw json.RawMessage, path string) error {
	return payloadMap(func(raw json.RawMessage, path string) error {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		switch value := value.(type) {
		case nil, string, bool:
			return nil
		case json.Number:
			return validateMetadataNumber(value.String(), path)
		default:
			return fmt.Errorf("%s must be a string, boolean, exact integer or null", path)
		}
	})(raw, path)
}

func validateBulkPayload(raw json.RawMessage, path string) error {
	return payloadArray(func(raw json.RawMessage, path string) error {
		fields, err := payloadObject(raw, path)
		if err != nil {
			return err
		}
		var action string
		if err := validateJSONString(fields["action"], path+".action"); err != nil {
			return err
		}
		if err := json.Unmarshal(fields["action"], &action); err != nil {
			return fmt.Errorf("%s.action: %w", path, err)
		}
		if action == "" {
			return fmt.Errorf("%s.action must not be empty", path)
		}
		if err := payloadFields(raw, path, map[string]payloadValidator{
			"ik": validateIdempotencyPayload, "skippableReasons": payloadArray(validateJSONString),
		}); err != nil {
			return err
		}
		dataPath := path + ".data"
		data, err := payloadObject(fields["data"], dataPath)
		if err != nil {
			return err
		}
		switch action {
		case "CREATE_TRANSACTION":
			return validateTransactionPayload(fields["data"], dataPath)
		case "REVERT_TRANSACTION":
			if err := validateRevertPayload(fields["data"], dataPath); err != nil {
				return err
			}
			if id, ok := data["id"]; ok {
				return validateJSONUint64(id, dataPath+".id")
			}
		case "ADD_METADATA", "DELETE_METADATA":
			if err := payloadFields(fields["data"], dataPath, map[string]payloadValidator{
				"targetType": validateJSONString, "metadata": validateMetadataPayload, "key": validateJSONString,
			}); err != nil {
				return err
			}
			if target, ok := data["targetId"]; ok {
				var kind string
				if err := json.Unmarshal(data["targetType"], &kind); err != nil {
					return fmt.Errorf("%s.targetType must be a string", dataPath)
				}
				switch strings.ToUpper(kind) {
				case "ACCOUNT":
					return validateJSONString(target, dataPath+".targetId")
				case "TRANSACTION":
					return validateJSONUint64(target, dataPath+".targetId")
				}
			}
		}
		// Future actions keep their object payload and are validated by the server.
		return nil
	})(raw, path)
}

func payloadObject(raw json.RawMessage, path string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("%s must be a JSON object", path)
	}

	return fields, nil
}

func payloadFields(raw json.RawMessage, path string, validators map[string]payloadValidator) error {
	fields, err := payloadObject(raw, path)
	if err != nil {
		return err
	}
	for key, validate := range validators {
		value, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			continue
		}
		if err := validate(value, path+"."+key); err != nil {
			return err
		}
	}

	return nil
}

func payloadMap(validate payloadValidator) payloadValidator {
	return func(raw json.RawMessage, path string) error {
		fields, err := payloadObject(raw, path)
		if err != nil {
			return err
		}
		for key, value := range fields {
			if err := validate(value, fmt.Sprintf("%s[%q]", path, key)); err != nil {
				return err
			}
		}

		return nil
	}
}

func payloadArray(validate payloadValidator) payloadValidator {
	return func(raw json.RawMessage, path string) error {
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil || values == nil {
			return fmt.Errorf("%s must be a JSON array", path)
		}
		for i, value := range values {
			if err := validate(value, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}

		return nil
	}
}

func validateJSONString(raw json.RawMessage, path string) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%s must be a string", path)
	}

	return nil
}

func validateJSONBool(raw json.RawMessage, path string) error {
	value := string(bytes.TrimSpace(raw))
	if value != "true" && value != "false" {
		return fmt.Errorf("%s must be a boolean", path)
	}

	return nil
}

func validateJSONUint32(raw json.RawMessage, path string) error {
	if _, err := strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 32); err != nil {
		return fmt.Errorf("%s must be an unsigned 32-bit integer", path)
	}

	return nil
}

func validateJSONUint64(raw json.RawMessage, path string) error {
	if _, err := strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64); err != nil {
		return fmt.Errorf("%s must be an unsigned 64-bit integer", path)
	}

	return nil
}

func validatePostingAmount(raw json.RawMessage, path string) error {
	raw = bytes.TrimSpace(raw)
	decimal := string(raw)
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &decimal); err != nil {
			return fmt.Errorf("%s must be an unsigned decimal integer", path)
		}
	}
	if decimal == "" || len(decimal) > 78 || (len(decimal) > 1 && decimal[0] == '0') {
		return fmt.Errorf("%s must be a canonical unsigned decimal integer within uint256", path)
	}
	for _, digit := range decimal {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("%s must be a canonical unsigned decimal integer within uint256", path)
		}
	}
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok || value.BitLen() > 256 {
		return fmt.Errorf("%s exceeds uint256", path)
	}

	return nil
}

func validateIdempotencyPayload(raw json.RawMessage, path string) error {
	if err := validateJSONString(raw, path); err != nil {
		return err
	}
	var key string
	if err := json.Unmarshal(raw, &key); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if len(key) > 256 || strings.ContainsAny(key, "\r\n") {
		return fmt.Errorf("%s must be at most 256 bytes without newlines", path)
	}

	return nil
}

// Integral decimal/exponent forms are valid metadata. Bound exponent work by
// token length and the 20-digit uint64 limit, never by an untrusted exponent.
func validateMetadataNumber(number, path string) error {
	failure := func() error { return fmt.Errorf("%s must be an exact integer within int64/uint64", path) }
	negative := strings.HasPrefix(number, "-")
	mantissa := strings.TrimPrefix(number, "-")
	exponentText := "0"
	if i := strings.IndexAny(mantissa, "eE"); i >= 0 {
		exponentText, mantissa = mantissa[i+1:], mantissa[:i]
	}
	fraction := 0
	if i := strings.IndexByte(mantissa, '.'); i >= 0 {
		fraction = len(mantissa) - i - 1
		mantissa = mantissa[:i] + mantissa[i+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return nil
	}
	exponent, err := strconv.ParseInt(exponentText, 10, 64)
	limit := int64(len(number)) + 20
	if err != nil || exponent < -limit || exponent > limit {
		return failure()
	}
	shift := exponent - int64(fraction)
	if shift < 0 {
		zeros := len(digits) - len(strings.TrimRight(digits, "0"))
		if -shift > int64(zeros) {
			return failure()
		}
		digits = digits[:len(digits)+int(shift)]
	} else {
		if int64(len(digits))+shift > 20 {
			return failure()
		}
		digits += strings.Repeat("0", int(shift))
	}
	value, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || (negative && value > uint64(1)<<63) {
		return failure()
	}

	return nil
}
