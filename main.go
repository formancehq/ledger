// docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest generate  -i ./pkg/api/controllers/swagger.yaml -g go -o ./client --git-user-id=formancehq --git-repo-id=ledger -p packageVersion=latest -p isGoSubmodule=true -p packageName=client
//
//go:generate docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest validate -i ./pkg/api/controllers/swagger.yaml
package main

import (
	"github.com/formancehq/ledger/cmd"
)

func main() {
	cmd.Execute()
}
