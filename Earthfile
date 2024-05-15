VERSION 0.8

ARG core=github.com/formancehq/earthly

IMPORT $core AS core
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
    COPY (+sources/*) /src
    WORKDIR /src/components/ledger
    COPY --dir --pass-args (+generate/*) .
    WITH DOCKER --pull=postgres:15-alpine
        DO --pass-args core+GO_TESTS
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
    DO --pass-args stack+GO_LINT
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