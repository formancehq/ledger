VERSION --wildcard-builds 0.8
PROJECT FormanceHQ/ledger

IMPORT github.com/formancehq/earthly:tags/v0.19.0 AS core

postgres:
    FROM postgres:15-alpine

sources:
    FROM core+builder-image
    CACHE --id go-mod-cache /go/pkg/mod
    CACHE --id go-cache /root/.cache/go-build
    WORKDIR /src/pkg/client
    COPY pkg/client/go.mod pkg/client/go.sum ./
    RUN go mod download
    WORKDIR /src
    COPY go.mod go.sum ./
    RUN go mod download
    COPY --dir internal pkg cmd .
    COPY main.go .
    SAVE ARTIFACT /src

generate:
    LOCALLY
    RUN go generate ./...
    SAVE ARTIFACT internal
    SAVE ARTIFACT pkg
    SAVE ARTIFACT cmd

compile:
    LOCALLY
    ARG VERSION=latest
    RUN go build -o main -ldflags="-X ${GIT_PATH}/cmd.Version=${VERSION} \
        -X ${GIT_PATH}/cmd.BuildDate=$(date +%s) \
        -X ${GIT_PATH}/cmd.Commit=${EARTHLY_BUILD_SHA}"
    SAVE ARTIFACT main

build-image:
    FROM core+final-image
    ENTRYPOINT ["/bin/ledger"]
    CMD ["serve"]
    COPY --pass-args (+compile/main) /bin/ledger
    ARG REPOSITORY=ghcr.io
    ARG tag=latest
    DO --pass-args core+SAVE_IMAGE --COMPONENT=ledger --REPOSITORY=${REPOSITORY} --TAG=$tag

tests:
    LOCALLY
    ARG includeIntegrationTests="true"
    ARG coverage=""
    ARG debug=false
    ARG additionalArgs=""

    ENV DEBUG=$debug
    ENV CGO_ENABLED=1 # required for -race

    LET goFlags="-race"
    IF [ "$coverage" = "true" ]
        SET goFlags="$goFlags -covermode=atomic"
        SET goFlags="$goFlags -coverpkg=github.com/formancehq/ledger/internal/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/events/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/accounts/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/assets/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/cmd/..."
        SET goFlags="$goFlags -coverprofile coverage.txt"
    END

    IF [ "$includeIntegrationTests" = "true" ]
        SET goFlags="$goFlags -tags it"
        #WITH DOCKER --load=postgres:15-alpine=+postgres
        RUN go test $goFlags $additionalArgs ./...
        #END
    ELSE
        RUN go test $goFlags $additionalArgs ./...
    END
    IF [ "$coverage" = "true" ]
        # as special case, exclude files suffixed by debug.go
        # toremovelater: exclude machine code as it will be updated soon
        RUN cat coverage.txt | grep -v debug.go | grep -v "/machine/" > coverage2.txt
        RUN mv coverage2.txt coverage.txt
        SAVE ARTIFACT coverage.txt
    END

deploy:
    COPY (+sources/*) /src
    LET tag=$(tar cf - /src | sha1sum | awk '{print $1}')
    WAIT
        BUILD --pass-args +build-image --tag=$tag
    END
    FROM --pass-args core+vcluster-deployer-image
    RUN kubectl patch Versions.formance.com default -p "{\"spec\":{\"ledger\": \"${tag}\"}}" --type=merge

deploy-staging:
    BUILD --pass-args core+deploy-staging

lint:
    LOCALLY
    RUN golangci-lint run --fix --build-tags it --timeout 5m
    SAVE ARTIFACT cmd
    SAVE ARTIFACT internal
    SAVE ARTIFACT pkg
    SAVE ARTIFACT test
    SAVE ARTIFACT main.go

pre-commit:
    BUILD +tidy
    BUILD +lint
    BUILD +openapi
    BUILD +openapi-markdown
    BUILD +generate
    BUILD +generate-client
    BUILD +export-docs-events
    BUILD ./tools/*+pre-commit
    BUILD ./deployments/*+pre-commit

pre-commit-nix:
    LOCALLY
    WAIT
      BUILD +tidy
      BUILD +lint
      BUILD +generate
    END
#    BUILD +tests
    BUILD +export-docs-events
#    BUILD +release

openapi:
    FROM node:20-alpine
    RUN apk update && apk add yq
    RUN npm install -g openapi-merge-cli
    WORKDIR /src
    COPY --dir openapi openapi
    RUN openapi-merge-cli --config ./openapi/openapi-merge.json
    RUN yq -oy ./openapi.json > openapi.yaml
    SAVE ARTIFACT ./openapi.yaml AS LOCAL ./openapi.yaml

openapi-markdown:
    FROM node:20-alpine
    RUN npm install -g widdershins
    COPY openapi/v2.yaml openapi.yaml
    RUN widdershins openapi.yaml -o README.md --search false --language_tabs 'http:HTTP' --summary --omitHeader
    SAVE ARTIFACT README.md AS LOCAL docs/api/README.md

tidy:
    LOCALLY
    RUN go mod tidy
    SAVE ARTIFACT go.mod
    SAVE ARTIFACT go.sum

release:
    LOCALLY
    ARG mode=local
    # TODO: Move to function in earthly repostiory
    LET buildArgs = --clean
    IF [ "$mode" = "local" ]
        SET buildArgs = --nightly --skip=publish --clean
    ELSE IF [ "$mode" = "ci" ]
        SET buildArgs = --nightly --clean
    END
    IF [ "$mode" != "local" ]
        WITH DOCKER
            RUN --secret GITHUB_TOKEN echo $GITHUB_TOKEN | docker login ghcr.io -u NumaryBot --password-stdin
        END
    END
    RUN goreleaser release -f .goreleaser.yml $buildArgs

generate-client:
    FROM node:20-alpine
    RUN apk update && apk add yq jq
    WORKDIR /src
    COPY (core+sources-speakeasy/speakeasy) /bin/speakeasy
    COPY (+openapi/openapi.yaml) openapi.yaml
    RUN cat ./openapi.yaml |  yq e -o json > openapi.json
    COPY (core+sources/out --LOCATION=openapi-overlay.json) openapi-overlay.json
    RUN jq -s '.[0] * .[1]' openapi.json openapi-overlay.json > final.json
    COPY --dir pkg/client client
    RUN --secret SPEAKEASY_API_KEY speakeasy generate sdk -s ./final.json -o ./client -l go
    SAVE ARTIFACT client AS LOCAL ./pkg/client

export-database-schema:
    FROM +sources
    RUN go install github.com/roerohan/wait-for-it@latest
    WITH DOCKER --load=postgres:15-alpine=+postgres --pull schemaspy/schemaspy:6.2.4
        RUN bash -c '
            echo "Creating PG server...";
            postgresContainerID=$(docker run -d --rm -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -e POSTGRES_DB=formance --net=host postgres:15-alpine);
            wait-for-it -w 127.0.0.1:5432;

            echo "Creating bucket...";
            go run main.go buckets upgrade _default --postgres-uri "postgres://root:root@127.0.0.1:5432/formance?sslmode=disable";

            echo "Exporting schemas...";
            docker run --rm -u root \
              -v ./docs/database:/output \
              --net=host \
              schemaspy/schemaspy:6.2.4 -u root -db formance -t pgsql11 -host 127.0.0.1 -port 5432 -p root -schemas _system,_default;

            docker kill "$postgresContainerID";
        '
    END
    SAVE ARTIFACT docs/database/_system/diagrams AS LOCAL docs/database/_system/diagrams
    SAVE ARTIFACT docs/database/_default/diagrams AS LOCAL docs/database/_default/diagrams

export-docs-events:
    LOCALLY
    RUN go run . docs events --write-dir docs/events
    SAVE ARTIFACT docs/events