// Command clickhouse-account-metadata converts the JSONEachRow export described
// in docs/ops/clickhouse-account-metadata.md without rounding JSON numbers.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	if err := convert(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func convert(input io.Reader, output io.Writer) error {
	decoder, encoder := json.NewDecoder(input), json.NewEncoder(output)
	for row := 1; ; row++ {
		var envelope map[string]json.RawMessage
		if err := decoder.Decode(&envelope); err != nil {
			if err == io.EOF {
				return nil
			}

			return fmt.Errorf("row %d: %w", row, err)
		}
		var text string
		if err := json.Unmarshal(envelope["data"], &text); err != nil {
			return fmt.Errorf("row %d data must be an exported JSON string: %w", row, err)
		}
		data, err := object([]byte(text))
		if err != nil {
			return fmt.Errorf("row %d data: %w", row, err)
		}
		if raw, ok := data["accountMetadata"]; ok {
			accounts, err := object(raw)
			if err != nil {
				return fmt.Errorf("row %d account metadata: %w", row, err)
			}
			for account, raw := range accounts {
				var text string
				if err := json.Unmarshal(raw, &text); err != nil {
					return fmt.Errorf("row %d account %q: %w", row, account, err)
				}
				metadata, err := object([]byte(text))
				if err != nil {
					return fmt.Errorf("row %d account %q: %w", row, account, err)
				}
				for key, value := range metadata {
					var text string
					if len(value) == 0 || value[0] != '"' {
						return fmt.Errorf("row %d account %q key %q: expected string", row, account, key)
					}
					if err := json.Unmarshal(value, &text); err != nil {
						return fmt.Errorf("row %d account %q key %q: %w", row, account, key, err)
					}
				}
				accounts[account] = json.RawMessage(text)
			}
			data["accountMetadata"], err = json.Marshal(accounts)
			if err != nil {
				return fmt.Errorf("row %d metadata: %w", row, err)
			}
		}
		// ClickHouse 24.x parses nested JSON through a generic numeric parser.
		// Quote only the schema's UInt256 amounts for import; the typed paths
		// restore them to integers. All other tokens remain RawMessages.
		for _, key := range []string{"transaction", "revertTransaction"} {
			if raw, ok := data[key]; ok {
				transaction, err := object(raw)
				if err != nil {
					return fmt.Errorf("row %d %s: %w", row, key, err)
				}
				if raw, ok := transaction["postings"]; ok {
					var postings []map[string]json.RawMessage
					if err := json.Unmarshal(raw, &postings); err != nil {
						return fmt.Errorf("row %d postings: %w", row, err)
					}
					for _, posting := range postings {
						if amount, ok := posting["amount"]; ok && len(amount) > 0 && amount[0] != '"' {
							posting["amount"], err = json.Marshal(string(amount))
							if err != nil {
								return fmt.Errorf("row %d amount: %w", row, err)
							}
						}
					}
					transaction["postings"], err = json.Marshal(postings)
					if err != nil {
						return fmt.Errorf("row %d postings: %w", row, err)
					}
				}
				data[key], err = json.Marshal(transaction)
				if err != nil {
					return fmt.Errorf("row %d %s: %w", row, key, err)
				}
			}
		}
		envelope["data"], err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("row %d data: %w", row, err)
		}
		if err := encoder.Encode(envelope); err != nil {
			return fmt.Errorf("row %d output: %w", row, err)
		}
	}
}

// object rejects duplicates rather than accepting encoding/json's last-key wins.
func object(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start != json.Delim('{') {
		return nil, errors.New("expected object")
	}
	result := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("expected object key")
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(raw[decoder.InputOffset():])) != "" {
		return nil, errors.New("unexpected data after object")
	}

	return result, nil
}
