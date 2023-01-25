//go:generate docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest validate -i ./swagger.yaml
package main

import "github.com/formancehq/search/cmd"

func main() {
	cmd.Execute()
}
