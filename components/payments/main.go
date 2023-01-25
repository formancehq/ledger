//go:generate docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest validate -i ./openapi.yaml
package main

import "github.com/formancehq/payments/cmd"

func main() {
	cmd.Execute()
}
