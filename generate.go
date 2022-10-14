//go:generate docker run --rm -w /local -v ${PWD}:/local openapitools/openapi-generator-cli:latest generate  -i pkg/api/controllers/swagger.yaml -g go -c tests/client-generator-config.yaml -o ./tests/internal/client --git-user-id=formancehq --git-repo-id=formancehq-sdk-go -p packageVersion=latest -t sdk/templates/go
//go:generate rm ./tests/internal/client/go.mod
//go:generate rm -rf ./tests/internal/client/test
package main
