//go:generate docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest generate  -i ./membership-swagger.yaml -g go -o ./membershipclient --git-user-id=formancehq --git-repo-id=fctl -p packageVersion=latest -p isGoSubmodule=true -p packageName=membershipclient
//go:generate rm -rf ./membershipclient/test
package main

import (
	"github.com/formancehq/fctl/cmd"
)

func main() {
	cmd.Execute()
}
