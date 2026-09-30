# Validates a Linux build of the grocksdb hello-world with the same
# base/runtime images as the ledger Dockerfile. Alpine ships RocksDB 11.0.4,
# the exact version grocksdb v1.11.0 targets; the binary links librocksdb
# dynamically and the runtime image installs the shared library.
# See docs/drafts/rocksdb-poc.md, step 1.
FROM golang:1.27-alpine AS build
RUN apk add --no-cache build-base rocksdb-dev snappy-dev zstd-dev lz4-dev zlib-dev bzip2-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd cmd
COPY spike spike
ENV CGO_ENABLED=1
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -o /out/rocksdb-hello ./cmd/hello && \
    go test -count=1 ./spike/

FROM alpine:latest
RUN apk add --no-cache rocksdb
COPY --from=build /out/rocksdb-hello /rocksdb-hello
ENTRYPOINT ["/rocksdb-hello"]
