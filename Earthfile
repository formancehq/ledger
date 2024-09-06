VERSION 0.8

IMPORT github.com/formancehq/earthly:tags/v0.15.0 AS core
IMPORT ../.. AS stack
IMPORT .. AS components

FROM core+base-image

sources:
    WORKDIR src
    COPY (stack+sources/out --LOCATION=libs/go-libs) /src/libs/go-libs
    COPY (stack+sources/out --LOCATION=libs/core) /src/libs/core
    WORKDIR /src/components/ledger
    COPY go.mod go.sum .
    COPY --dir internal pkg cmd .
    COPY main.go .
    SAVE ARTIFACT /src

generate:
    FROM core+builder-image
    RUN apk update && apk add openjdk11
    DO --pass-args core+GO_INSTALL --package=go.uber.org/mock/mockgen@latest
    COPY (+sources/*) /src
    WORKDIR /src/components/ledger
    DO --pass-args core+GO_GENERATE
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT cmd AS LOCAL cmd

compile:
    FROM core+builder-image
    COPY (+sources/*) /src
    WORKDIR /src/components/ledger
    ARG VERSION=latest
    DO --pass-args core+GO_COMPILE --VERSION=$VERSION

build-image:
    FROM core+final-image
    ENTRYPOINT ["/bin/ledger"]
    CMD ["serve"]
    COPY --pass-args (+compile/main) /bin/ledger
    ARG REPOSITORY=ghcr.io
    ARG tag=latest
    DO --pass-args core+SAVE_IMAGE --COMPONENT=ledger --REPOSITORY=${REPOSITORY} --TAG=$tag

tests:
    FROM core+builder-image
    RUN go install github.com/onsi/ginkgo/v2/ginkgo@latest

    COPY (+sources/*) /src
    WORKDIR /src/components/ledger
    COPY --dir --pass-args (+generate/*) .

    ARG includeIntegrationTests="true"
    ARG coverage=""
    ARG debug=false

    ENV DEBUG=$debug
    ENV CGO_ENABLED=1 # required for -race
    RUN apk add gcc musl-dev

    LET goFlags="-race"
    IF [ "$coverage" = "true" ]
        SET goFlags="$goFlags -covermode=atomic"
        SET goFlags="$goFlags -coverpkg=github.com/formancehq/stack/components/ledger/internal/..."
        SET goFlags="$goFlags,github.com/formancehq/stack/components/ledger/cmd/..."
        SET goFlags="$goFlags -coverprofile cover.out"
    END
    IF [ "$includeIntegrationTests" = "true" ]
        SET goFlags="$goFlags -tags it"
        WITH DOCKER \
            --pull=postgres:15-alpine \
            --pull=clickhouse/clickhouse-server:head \
            --pull=elasticsearch:8.14.3
            RUN --mount type=cache,id=gopkgcache,target=${GOPATH}/pkg/mod \
                --mount type=cache,id=gobuildcache,target=/root/.cache/go-build \
                ginkgo -r -p $goFlags
        END
    ELSE
        RUN --mount type=cache,id=gopkgcache,target=${GOPATH}/pkg/mod \
            --mount type=cache,id=gobuildcache,target=/root/.cache/go-build \
            ginkgo -r -p $goFlags
    END
    IF [ "$coverage" = "true" ]
        # exclude files suffixed with _generated.go, these are mocks used by tests
        RUN cat cover.out | grep -v "_generated.go" > cover2.out
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
    BUILD --pass-args stack+deployer-module --MODULE=ledger

lint:
    FROM core+builder-image
    COPY (+sources/*) /src
    COPY --pass-args +tidy/go.* .
    WORKDIR /src/components/ledger
    DO --pass-args stack+GO_LINT --ADDITIONAL_ARGUMENTS="--build-tags it"
    SAVE ARTIFACT cmd AS LOCAL cmd
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT main.go AS LOCAL main.go

pre-commit:
    WAIT
      BUILD --pass-args +tidy
    END
    BUILD --pass-args +lint
    BUILD +openapi

bench:
    FROM core+builder-image
    DO --pass-args core+GO_INSTALL --package=golang.org/x/perf/cmd/benchstat@latest
    COPY (+sources/*) /src
    WORKDIR /src/components/ledger/internal/storage/ledgerstore
    ARG numberOfTransactions=10000
    ARG ledgers=10
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
        RUN --mount type=cache,id=gopkgcache,target=${GOPATH}/pkg/mod \
            --mount type=cache,id=gobuild,target=/root/.cache/go-build \
            go test -timeout $testTimeout -bench=$bench -run ^$ $additionalArgs \
            -benchtime=$benchTime \
            -count=$count \
            -ledgers=$ledgers \
            -transactions=$numberOfTransactions | tee -a /results.txt
    END
    RUN benchstat /results.txt
    SAVE ARTIFACT /results.txt

benchstat:
    FROM core+builder-image
    DO --pass-args core+GO_INSTALL --package=golang.org/x/perf/cmd/benchstat@latest
    ARG compareAgainstRevision=main
    COPY --pass-args github.com/formancehq/stack/components/ledger:$compareAgainstRevision+bench/results.txt /tmp/main.txt
    COPY --pass-args +bench/results.txt /tmp/branch.txt
    RUN --no-cache benchstat /tmp/main.txt /tmp/branch.txt

openapi:
    FROM node:20-alpine
    RUN apk update && apk add yq
    RUN npm install -g openapi-merge-cli
    WORKDIR /src/components/ledger
    COPY --dir openapi openapi
    RUN openapi-merge-cli --config ./openapi/openapi-merge.json
    RUN yq -oy ./openapi.json > openapi.yaml
    SAVE ARTIFACT ./openapi.yaml AS LOCAL ./openapi.yaml

tidy:
    FROM core+builder-image
    COPY --pass-args (+sources/src) /src
    WORKDIR /src/components/ledger
    DO --pass-args stack+GO_TIDY

release:
    BUILD --pass-args stack+goreleaser --path=components/ledger

generate-sdk:
    FROM node:20-alpine
    RUN apk update && apk add yq git
    WORKDIR /src
    COPY (stack+speakeasy/speakeasy) /bin/speakeasy
    ARG version=v0.0.0
    COPY openapi/v2.yaml openapi.yaml
    COPY --dir pkg/client client
    RUN --secret SPEAKEASY_API_KEY speakeasy generate sdk -s ./openapi.yaml -o ./client -l go

    SAVE ARTIFACT client AS LOCAL ./pkg/client
