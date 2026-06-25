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

# Pre-warm the build cache: compile all shared dependencies once (race-instrumented).
# This avoids recompiling the same packages once per driver — subsequent builds only
# compile the unique main.go (~117 LOC each).
RUN go build -race ./internal/...

# Build all workload binaries in parallel, preserving the
# bin/cmds/<template>/<command> layout so each command lands in its own test
# template directory. The shared dependency cache is warm from the previous step,
# so each build only compiles main.go.
RUN find ./bin/cmds -mindepth 2 -maxdepth 2 -type d \
        | sed 's|^\./bin/cmds/||' \
        | xargs -P"$(nproc)" -I{} sh -c 'mkdir -p "/cmds/$(dirname "$1")" && go build -race -o "/cmds/$1" "./bin/cmds/$1"' _ {}

# Runner
FROM debian:trixie

RUN apt-get update && apt-get install -y curl

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.44 /ko-app/grpc-health-probe /bin/grpc_health_probe

COPY --from=compiler /init /init
ENTRYPOINT ["/init"]

# Antithesis selects exactly one test template per execution history, so commands
# in different templates never run together. The source mirrors this: each
# bin/cmds/<template>/ directory becomes a test template under test/v1/. The
# model checker drives the system itself and must own the whole timeline (no
# concurrent driver, no eventually_* command pre-empting it), so it lives in its
# own template (model), separate from the rest of the suite (main).
COPY --from=compiler /cmds/ /opt/antithesis/test/v1/
COPY --from=compiler /instrumented/symbols/ /symbols
