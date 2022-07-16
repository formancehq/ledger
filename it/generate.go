//go:generate docker run --rm -w /local -v ${PWD}/..:/local openapitools/openapi-generator-cli:latest generate  -i pkg/api/controllers/swagger.yaml -g go -c sdk/configs/go.yaml -o ./it/internal/client --git-user-id=numary --git-repo-id=numary-sdk-go -p packageVersion=latest -t sdk/templates/go
package it
