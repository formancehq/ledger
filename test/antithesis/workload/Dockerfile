# Compiler
FROM golang:1.24-alpine3.22 AS compiler

RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@latest

WORKDIR /src/test/antithesis/workload

COPY test/antithesis/workload/go.* .
COPY test/antithesis/workload/bin ./bin
COPY test/antithesis/workload/internal ./internal
COPY pkg/client /src/pkg/client

RUN antithesis-go-instrumentor -assert_only .

RUN go build -o /init ./bin/init
RUN mkdir -p /cmds
RUN for file in $(ls ./bin/cmds/); do go build -o /cmds/$file ./bin/cmds/$file; done

RUN antithesis-go-instrumentor -assert_only .

# Runner
FROM alpine:3.22

RUN apk update && apk add --no-cache curl

ARG LEDGER_LATEST_TAG

COPY --from=compiler /init /init
ENTRYPOINT ["/init"]

COPY --from=compiler /cmds/* /opt/antithesis/test/v1/main/

RUN echo ${LEDGER_LATEST_TAG} > /ledger_latest_tag
