# Why BUILD + FROM : https://github.com/earthly/earthly/issues/4100
VERSION 0.8
PROJECT FormanceHQ/ledger

IMPORT github.com/formancehq/earthly:tags/v0.16.2 AS core
IMPORT github.com/formancehq/stack/releases:main AS releases

FROM core+base-image

golang-image:
    RUN apk update && apk add go
    ENV GOPATH=/go
    ENV GOCACHE=$GOPATH/cache
    ENV PATH=$GOPATH/bin:$PATH
    CACHE --persist --sharing=shared $GOPATH

    RUN go build -v std

    SAVE IMAGE --push 192.168.0.83:5555/formancehq/ledger:golang-image

golang-deps:
    BUILD +golang-image
    FROM +golang-image
    WORKDIR /src
    COPY main.go go.mod go.sum .
    COPY pkg/client/go.* pkg/client/
    RUN go mod download
    RUN go get github.com/formancehq/go-libs/...
    RUN go install github.com/formancehq/go-libs/...

    SAVE ARTIFACT /go
    SAVE IMAGE --push 192.168.0.83:5555/formancehq/ledger:deps

sources:
    WORKDIR /src
    COPY --dir internal pkg cmd .
    COPY main.go go.sum go.mod .
    SAVE ARTIFACT /src

test-sources:
    FROM +sources
    COPY --dir test .
    SAVE ARTIFACT /src

tidy:
    BUILD +golang-image
    BUILD +golang-deps
    FROM +golang-image
    COPY (+golang-deps/go) /go
    COPY (+test-sources/src) /src
    WORKDIR /src
    RUN go mod tidy

    SAVE ARTIFACT go.sum
    SAVE ARTIFACT go.mod

    SAVE IMAGE --push 192.168.0.83:5555/formancehq/ledger:tidy

compile:
    BUILD +golang-image
    BUILD +golang-deps
    FROM +golang-image
    COPY (+golang-deps/go) /go
    COPY (+sources/src) /src
    ENV CGO_ENABLED=0
    WORKDIR /src
    ARG VERSION=latest
    RUN go build -v -o main -ldflags="-X ${GIT_PATH}/cmd.Version=${VERSION} \
        -X ${GIT_PATH}/cmd.BuildDate=$(date +%s) \
        -X ${GIT_PATH}/cmd.Commit=${EARTHLY_BUILD_SHA}"
    SAVE ARTIFACT main

build-image:
    BUILD +compile
    ENTRYPOINT ["/bin/ledger"]
    CMD ["serve"]
    COPY (+compile/main) /bin/ledger
    ARG REPOSITORY=ghcr.io
    ARG tag=latest
    DO core+SAVE_IMAGE --COMPONENT=ledger --REPOSITORY=${REPOSITORY} --TAG=$tag

mockgen-image:
    FROM +golang-image
    RUN apk update && apk add openjdk11 bash curl gcc musl-dev
    RUN go install go.uber.org/mock/mockgen@latest

    SAVE IMAGE --push 192.168.0.83:5555/formancehq/ledger:mockgen

generate-mocks:
    BUILD +mockgen-image
    FROM +mockgen-image
    #COPY (+golang-deps/go) /go
    COPY (+sources/src) /src
    WORKDIR /src
    RUN go generate ./...
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT cmd AS LOCAL cmd

tests:
    BUILD +generate-mocks
    BUILD +golang-image
    BUILD +golang-deps

    FROM +golang-image
    RUN apk update && apk add gcc musl-dev

    COPY (+golang-deps/go) /go
    COPY --dir (+test-sources/src) /src
    COPY --dir (+generate-mocks/*) /src/
    COPY (+tidy/*) /src
    WORKDIR /src
    RUN go install github.com/onsi/ginkgo/v2/ginkgo

    ARG it="true"
    ARG coverage=""
    ARG debug=false

    ENV CGO_ENABLED=1
    ENV DEBUG=$debug

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

    IF [ "$it" = "true" ]
        SET goFlags="$goFlags -tags it"
        WITH DOCKER --pull=postgres:15-alpine
            RUN ginkgo -r -p $goFlags
        END
    ELSE
        RUN go test -v $goFlags ./...
    END
    IF [ "$coverage" = "true" ]
        # exclude files suffixed with _generated.go, these are mocks used by tests
        # toremovelater: also exclude machine code as it will be updated soon
        RUN cat cover.out | grep -v "_generated.go" | grep -v "/machine/" > cover2.out
        RUN mv cover2.out cover.out
        SAVE ARTIFACT cover.out AS LOCAL cover.out
    END

deploy:
    COPY (+sources/*) /src
    LET tag=$(tar cf - /src | sha1sum | awk '{print $1}')
    WAIT
        BUILD +build-image --tag=$tag
    END
    FROM core+vcluster-deployer-image
    RUN kubectl patch Versions.formance.com default -p "{\"spec\":{\"ledger\": \"${tag}\"}}" --type=merge

deploy-staging:
    BUILD core+deploy-staging

lint:
    FROM +tidy
    RUN golangci-lint run --fix --build-tags it --timeout 2m
    SAVE ARTIFACT cmd AS LOCAL cmd
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT test AS LOCAL test
    SAVE ARTIFACT main.go AS LOCAL main.go

pre-commit:
    BUILD +tidy
    BUILD +lint
    BUILD +openapi
    BUILD +generate-client
    BUILD +export-docs-events
    IF [ "$EARTHLY_CI" = "true" ]
        BUILD +tests
    END

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
    COPY github.com/formancehq/stack/components/ledger:$compareAgainstRevision+bench/results.txt /tmp/main.txt
    COPY +bench/results.txt /tmp/branch.txt
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
    BUILD +sources
    FROM +sources
    RUN go install github.com/roerohan/wait-for-it@latest
    WORKDIR /src/components/ledger
    COPY --dir scripts scripts
    WITH DOCKER --pull postgres:15-alpine --pull schemaspy/schemaspy:6.2.4
        RUN ./scripts/export-database-schema.sh
    END
    SAVE ARTIFACT docs/database AS LOCAL docs/database

export-docs-events:
    BUILD +tidy
    FROM +tidy
    RUN go run main.go doc events --write-dir docs/events
    SAVE ARTIFACT docs/events AS LOCAL docs/events