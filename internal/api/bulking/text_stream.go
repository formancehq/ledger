package bulking

import (
	"bufio"
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/machine/vm"
	"strings"
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
				parts := strings.Split(text, ",")
				for _, part := range parts {
					parts2 := strings.Split(part, "=")
					switch parts2[0] {
					case "ik":
						if bulkElement.IdempotencyKey != "" {
							return nil, errors.New("invalid header, idempotency key already set")
						}
						bulkElement.IdempotencyKey = parts2[1]
					default:
						return nil, errors.New("invalid header, key '" + parts2[0] + "' not recognized")
					}
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
								Plain: plain[:len(plain)-1], // remove last \n
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
							Plain: plain[:len(plain)-1], // remove last \n
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
