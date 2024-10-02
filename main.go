// # Formance Ledger
//
// Formance Ledger (fka numary) is a programmable financial ledger that provides a foundation for money-moving applications. The ledger provides atomic multi-postings transactions and is programmable in [Numscript](doc:machine-instructions), a built-in language dedicated to money movements. It can be used either as a standalone micro-service or as part of the greater Formance Stack, and will shine for apps that require a lot of custom, money-moving code, e.g:
//  * E-commerce with complex payments flows, payments splitting, such as marketplaces
//  * Company-issued currencies systems, e.g. Twitch Bits
//  * In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
//  * Payment gateways using non-standard assets, e.g. learning credits
//  * Local currencies and complementary finance
//
// # Getting started
//
// Formance Ledger works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/formancehq/ledger/releases). You can move the binary to any executable path, such as to `/usr/local/bin`. Installations using brew, apt, yum or docker are also [available](https://docs.formance.com/docs/installation-1).
//
//	docker compose up -d
//
//	# Submit a first transaction
//	curl -X POST http://localhost:3068/quickstart/transactions -d '{"postings": [{"source": "world", "destination": "bank", "amount": 100, "asset": "USD"}]}'
//
//	# Get the balances of drivers:042
//	curl -X GET http://localhost:3068/quickstart/accounts/bank
//
//	# List transactions
//	curl -X GET http://localhost:3068/quickstart/transactions
//
// # Documentation
//
// You can find the complete Numary documentation at [docs.formance.com](https://docs.formance.com)
//
// # Community
//
// If you need help, want to show us what you built or just hang out and chat about ledgers you are more than welcome on our [Slack](https://bit.ly/formance-slack) - looking forward to see you there!
//go:generate gomarkdoc --output README.md .
package main

import (
	"github.com/formancehq/ledger/cmd"
)

func main() {
	cmd.Execute()
}
