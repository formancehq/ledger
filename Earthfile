VERSION 0.8
PROJECT FormanceHQ/ledger

IMPORT github.com/formancehq/earthly:tags/v0.16.2 AS core
IMPORT github.com/formancehq/stack/releases:main AS releases

FROM core+base-image

CACHE --persist --sharing=shared /go
CACHE --persist --sharing=shared /root/.cache/golangci-lint

sources:
    FROM core+builder-image
    WORKDIR /src/pkg/client
    COPY pkg/client/go.mod pkg/client/go.sum ./
    RUN go mod download
    WORKDIR /src
    COPY go.mod go.sum ./
    RUN go mod download
    COPY --dir internal pkg cmd tools .
    COPY main.go .
    SAVE ARTIFACT /src

generate:
    FROM core+builder-image
    RUN apk update && apk add openjdk11
    RUN go install go.uber.org/mock/mockgen@latest
    RUN go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
    COPY (+sources/*) /src
    WORKDIR /src
    RUN go generate ./...
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT cmd AS LOCAL cmd

compile:
    FROM +sources
    WORKDIR /src
    ARG VERSION=latest
    RUN go build
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
    FROM +tidy
    RUN go install github.com/onsi/ginkgo/v2/ginkgo@latest
    COPY --dir --pass-args (+generate/*) .

    ARG includeIntegrationTests="true"
    ARG includeEnd2EndTests="true"
    ARG coverage=""
    ARG debug=false

    ENV DEBUG=$debug
    ENV CGO_ENABLED=1 # required for -race
    RUN apk add gcc musl-dev

    LET goFlags="-race"
    IF [ "$coverage" = "true" ]
        SET goFlags="$goFlags -covermode=atomic"
        SET goFlags="$goFlags -coverpkg=github.com/formancehq/ledger/internal/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/events/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/accounts/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/pkg/assets/..."
        SET goFlags="$goFlags,github.com/formancehq/ledger/cmd/..."
        SET goFlags="$goFlags -coverprofile cover.out"
    END

    IF [ "$includeIntegrationTests" = "true" ]
        SET goFlags="$goFlags -tags it"
        WITH DOCKER --pull=postgres:15-alpine
            RUN ginkgo -r -p $goFlags
        END
    ELSE
        RUN ginkgo -r -p $goFlags
    END
    IF [ "$coverage" = "true" ]
        # as special case, exclude files suffixed by debug.go
        # toremovelater: exclude machine code as it will be updated soon
        RUN cat cover.out | grep -v debug.go | grep -v "/machine/" > cover2.out
        RUN mv cover2.out cover.out
        SAVE ARTIFACT cover.out AS LOCAL cover.out
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
    FROM +tidy
    RUN golangci-lint run --fix --build-tags it --timeout 5m
    SAVE ARTIFACT cmd AS LOCAL cmd
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT test AS LOCAL test
    SAVE ARTIFACT main.go AS LOCAL main.go

pre-commit:
    WAIT
        BUILD +tidy
        BUILD +lint
        BUILD +openapi
        BUILD +openapi-markdown
    END
    BUILD +generate
    BUILD +generate-client
    BUILD +export-docs-events

bench:
    FROM +tidy
    RUN go install golang.org/x/perf/cmd/benchstat@latest
    WORKDIR /src/test/performance
    ARG benchTime=1s
    ARG count=1
    ARG GOPROXY
    ARG testTimeout=10m
    ARG bench=.
    ARG verbose=0
    ARG GOMAXPROCS=2
    ARG GOMEMLIMIT=1024MiB
    LET additionalArgs=""
    IF [ "$verbose" = "1" ]
        SET additionalArgs=-v
    END
    WITH DOCKER --pull postgres:15-alpine
        RUN go test -timeout $testTimeout -bench=$bench -run ^$ -tags it $additionalArgs \
            -benchtime=$benchTime | tee -a /results.txt
    END
    RUN benchstat /results.txt
    SAVE ARTIFACT /results.txt

benchstat:
    FROM core+builder-image
    RUN go install golang.org/x/perf/cmd/benchstat@latest
    ARG compareAgainstRevision=main
    COPY --pass-args github.com/formancehq/stack/components/ledger:$compareAgainstRevision+bench/results.txt /tmp/main.txt
    COPY --pass-args +bench/results.txt /tmp/branch.txt
    RUN --no-cache benchstat /tmp/main.txt /tmp/branch.txt

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
    FROM +sources
    WORKDIR /src
    COPY --dir test .
    RUN go mod tidy

release:
    FROM core+builder-image
    ARG mode=local
    COPY --dir . /src
    DO core+GORELEASER --mode=$mode

generate-client:
    FROM node:20-alpine
    RUN apk update && apk add yq jq
    WORKDIR /src
    COPY (core+sources-speakeasy/speakeasy) /bin/speakeasy
    COPY (+openapi/openapi.yaml) openapi.yaml
    RUN cat ./openapi.yaml |  yq e -o json > openapi.json
    COPY (releases+sources/src/openapi-overlay.json) openapi-overlay.json
    RUN jq -s '.[0] * .[1]' openapi.json openapi-overlay.json > final.json
    COPY --dir pkg/client client
    RUN --secret SPEAKEASY_API_KEY speakeasy generate sdk -s ./final.json -o ./client -l go
    SAVE ARTIFACT client AS LOCAL ./pkg/client

export-database-schema:
    FROM +sources
    RUN go install github.com/roerohan/wait-for-it@latest
    COPY --dir scripts scripts
    WITH DOCKER --pull postgres:15-alpine --pull schemaspy/schemaspy:6.2.4
        RUN ./scripts/export-database-schema.sh
    END
    SAVE ARTIFACT docs/database/_system/diagrams AS LOCAL docs/database/_system/diagrams
    SAVE ARTIFACT docs/database/_default/diagrams AS LOCAL docs/database/_default/diagrams

export-docs-events:
    FROM +tidy
    RUN go run tools/docs/events/main.go --write-dir docs/events
    SAVE ARTIFACT docs/events AS LOCAL docs/events