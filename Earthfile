VERSION --pass-args --arg-scope-and-set 0.7

ARG core=github.com/formancehq/earthly:v0.5.2
IMPORT $core AS core
IMPORT ../.. AS stack

FROM core+base-image

sources:
    WORKDIR src
    COPY (stack+sources/out --LOCATION=libs/go-libs) /src/components/ledger/libs
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
    SAVE ARTIFACT libs AS LOCAL ./libs

copy-libs:
  LOCALLY
  RUN rm -rf ./libs
  RUN cp -R ./../../libs/go-libs ./libs

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
    RUN kubectl patch Versions default -p "{\"spec\":{\"ledger\": \"${tag}\"}}" --type=merge

lint:
    FROM core+builder-image
    COPY (+sources/*) /src
    COPY --pass-args (stack+tidy/go.* --component=ledger) .
    WORKDIR /src/components/ledger
    DO --pass-args stack+GO_LINT
    SAVE ARTIFACT cmd AS LOCAL cmd
    SAVE ARTIFACT internal AS LOCAL internal
    SAVE ARTIFACT pkg AS LOCAL pkg
    SAVE ARTIFACT main.go AS LOCAL main.go

pre-commit:
    BUILD --pass-args +copy-libs

bench:
    FROM core+builder-image
    COPY (+sources/*) /src
    WORKDIR /src/components/ledger/internal/storage/ledgerstore
    ARG numberOfTransactions=10000
    ARG benchTime=1s
    ARG count=1
    ARG GOPROXY
    ARG testTimeout=10m
    ARG bench=.
    WITH DOCKER --pull postgres:15-alpine
        RUN --mount type=cache,id=gopkgcache,target=${GOPATH}/pkg/mod \
            --mount type=cache,id=gobuild,target=/root/.cache/go-build \
            go test -timeout $testTimeout -bench=$bench -run ^$ \
            -benchtime=$benchTime \
            -count=$count \
            -transactions=$numberOfTransactions | tee -a /results.txt
    END
    SAVE ARTIFACT /results.txt

benchstat:
    FROM core+builder-image
    DO --pass-args core+GO_INSTALL --package=golang.org/x/perf/cmd/benchstat@latest
    COPY --pass-args +bench/results.txt /tmp/branch.txt
    COPY --pass-args github.com/formancehq/stack/components/ledger:main+bench/results.txt /tmp/main.txt
    RUN benchstat /tmp/main.txt /tmp/branch.txt
