package bulking

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/machine/vm"
)

func ParseTextStream(scanner *bufio.Scanner) (*BulkElement, error) {

	// Read header
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())

		switch {
		case text == "":
		case strings.HasPrefix(text, "//script"):
			bulkElement := BulkElement{}
			bulkElement.Action = ActionCreateTransaction
			text = strings.TrimPrefix(text, "//script")
			text = strings.TrimSpace(text)

			if len(text) > 0 {
				idempotencyKeySeen := false
				emptyIdempotencyKey := false
				for _, part := range strings.Split(text, ",") {
					key, value, hasValue := strings.Cut(strings.TrimSpace(part), "=")
					key = strings.TrimSpace(key)
					value = strings.TrimSpace(value)
					switch key {
					case "ik":
						if !hasValue {
							return nil, errors.New("invalid header, idempotency key must use key=value format")
						}
						if idempotencyKeySeen {
							return nil, errors.New("invalid header, idempotency key already set")
						}
						idempotencyKeySeen = true
						if value == "" {
							emptyIdempotencyKey = true
							continue
						}
						bulkElement.IdempotencyKey = value
					default:
						return nil, errors.New("invalid header, key '" + key + "' not recognized")
					}
				}
				if emptyIdempotencyKey {
					return nil, errors.New("invalid header, idempotency key must not be empty")
				}
			}

			// Read body
			plain := ""
			for scanner.Scan() {
				text = scanner.Text()
				if text == "//end" {
					bulkElement.Data = TransactionRequest{
						Script: ledgercontroller.ScriptV1{
							Script: vm.Script{
								Plain: strings.TrimSuffix(plain, "\n"),
							},
						},
					}
					return &bulkElement, nil
				}
				plain += text + "\n"
			}

			if scanner.Err() != nil {
				return nil, fmt.Errorf("error reading script: %w", scanner.Err())
			}

			if scanner.Err() == nil {
				bulkElement.Data = TransactionRequest{
					Script: ledgercontroller.ScriptV1{
						Script: vm.Script{
							Plain: strings.TrimSuffix(plain, "\n"),
						},
					},
				}
				return &bulkElement, nil
			}
		default:
			return nil, errors.New("invalid header")
		}
	}

	if scanner.Err() != nil {
		return nil, fmt.Errorf("error while reading script: %w", scanner.Err())
	}

	return nil, nil
}
