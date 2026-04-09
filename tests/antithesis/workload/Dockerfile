# Compiler
FROM golang:1.26-trixie AS compiler
ARG GOARCH
ARG GOOS

RUN apt-get update && apt-get install -y bash git gcc

RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@v0.7.0

COPY go.* /src/
COPY internal /src/internal
COPY pkg /src/pkg
COPY tests/antithesis/workload/go.* /src/tests/antithesis/workload/
COPY tests/antithesis/workload/bin /src/tests/antithesis/workload/bin
COPY tests/antithesis/workload/internal /src/tests/antithesis/workload/internal

# The instrumentor outputs to /instrumented/customer/ and runs go mod tidy there.
# The replace directive (../../../) resolves to / from that path, so place the root module at /.
RUN cp /src/go.* / && cp -r /src/internal /internal && cp -r /src/pkg /pkg

RUN mkdir /instrumented
WORKDIR /src/tests/antithesis/workload
RUN antithesis-go-instrumentor . /instrumented
WORKDIR /instrumented/customer

RUN go build -o /init ./bin/init
RUN mkdir -p /cmds
RUN for file in $(ls ./bin/cmds/); do go build -race -o /cmds/$file ./bin/cmds/$file; done

# Runner
FROM debian:trixie

RUN apt-get update && apt-get install -y curl

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.44 /ko-app/grpc-health-probe /bin/grpc_health_probe

COPY --from=compiler /init /init
ENTRYPOINT ["/init"]

COPY --from=compiler /cmds/* /opt/antithesis/test/v1/main/
COPY --from=compiler /instrumented/symbols/ /symbols
