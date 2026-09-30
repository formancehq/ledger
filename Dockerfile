FROM golang:1.27-alpine AS base
ARG GOARCH
ARG GOOS
ARG BUILD_TAGS=""
# STORAGE_ENGINE=rocksdb builds the grocksdb engine (RocksDB POC,
# docs/drafts/rocksdb-poc.md): cgo on, Alpine's RocksDB 11.0.4 headers in
# the builder and the shared library in the runtime image.
ARG STORAGE_ENGINE=pebble
WORKDIR /build
RUN apk add --no-cache git make
RUN if [ "$STORAGE_ENGINE" = "rocksdb" ]; then \
      apk add --no-cache build-base rocksdb-dev snappy-dev zstd-dev lz4-dev zlib-dev bzip2-dev; \
    fi
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download
ENV STORAGE_ENGINE=${STORAGE_ENGINE}
COPY main.go .
COPY internal internal
COPY cmd cmd
COPY pkg pkg

FROM base AS build-server
ARG GOARCH
ARG GOOS
ARG BUILD_TAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    if [ "$STORAGE_ENGINE" = "rocksdb" ]; then export CGO_ENABLED=1 BUILD_TAGS="${BUILD_TAGS},rocksdb"; else export CGO_ENABLED=0; fi; \
    go build -tags "${BUILD_TAGS}" -o ledger-server .

FROM base AS build-ledgerctl
ARG GOARCH
ARG GOOS
ARG BUILD_TAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    if [ "$STORAGE_ENGINE" = "rocksdb" ]; then export CGO_ENABLED=1 BUILD_TAGS="${BUILD_TAGS},rocksdb"; else export CGO_ENABLED=0; fi; \
    go build -tags "${BUILD_TAGS}" -o ledgerctl ./cmd/ledgerctl

FROM alpine:latest
ARG STORAGE_ENGINE=pebble
RUN apk --no-cache add ca-certificates tzdata bash bash-completion && \
    sed -i 's|/bin/ash|/bin/bash|' /etc/passwd
RUN if [ "$STORAGE_ENGINE" = "rocksdb" ]; then apk add --no-cache rocksdb; fi
ENV TZ=UTC
ENV PATH=$PATH:/app
SHELL ["/bin/bash", "-c"]
WORKDIR /app
COPY --from=build-server /build/ledger-server .
COPY --from=build-ledgerctl /build/ledgerctl .
RUN ./ledgerctl completion bash > /etc/bash_completion.d/ledgerctl
ENTRYPOINT ["./ledger-server"]
