# grocksdb v1.11.1 uses C API added in RocksDB 11.1; Alpine 3.24 ships 11.0.
FROM alpine:3.24 AS rocksdb-build
RUN apk add --no-cache build-base linux-headers cmake ninja bzip2-dev lz4-dev snappy-dev zlib-dev zstd-dev
ADD --checksum=sha256:3aba946031d27734eeea68ced7dd6f46629ba23a7b49945eb29aea68d1595311 \
    https://github.com/facebook/rocksdb/archive/refs/tags/v11.1.2.tar.gz /tmp/rocksdb.tar.gz
RUN mkdir -p /tmp/rocksdb && tar -xzf /tmp/rocksdb.tar.gz -C /tmp/rocksdb --strip-components=1 && \
    cmake -S /tmp/rocksdb -B /tmp/rocksdb/build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DPORTABLE=1 -DWITH_TESTS=OFF \
      -DWITH_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_GFLAGS=OFF -DWITH_LIBURING=OFF -DROCKSDB_BUILD_SHARED=ON \
      -DWITH_SNAPPY=ON -DWITH_ZSTD=ON -DWITH_LZ4=ON -DWITH_ZLIB=ON -DWITH_BZ2=ON \
      -DUSE_RTTI=1 -DFAIL_ON_WARNINGS=NO && \
    cmake --build /tmp/rocksdb/build --target rocksdb-shared --parallel 4 && \
    mkdir -p /usr/local/include /usr/local/lib && \
    cp -a /tmp/rocksdb/include/rocksdb /usr/local/include/ && \
    cp -a /tmp/rocksdb/build/librocksdb.so* /usr/local/lib/

# Release archives need a self-contained binary. Build the same RocksDB
# version as a static library for the musl-linked release targets.
FROM alpine:3.24 AS rocksdb-static-build
RUN apk add --no-cache build-base linux-headers cmake ninja bzip2-dev lz4-dev snappy-dev zlib-dev zstd-dev
ADD --checksum=sha256:3aba946031d27734eeea68ced7dd6f46629ba23a7b49945eb29aea68d1595311 \
    https://github.com/facebook/rocksdb/archive/refs/tags/v11.1.2.tar.gz /tmp/rocksdb.tar.gz
RUN mkdir -p /tmp/rocksdb && tar -xzf /tmp/rocksdb.tar.gz -C /tmp/rocksdb --strip-components=1 && \
    cmake -S /tmp/rocksdb -B /tmp/rocksdb/build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DPORTABLE=1 -DWITH_TESTS=OFF \
      -DWITH_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_GFLAGS=OFF -DWITH_LIBURING=OFF -DROCKSDB_BUILD_SHARED=OFF \
      -DWITH_SNAPPY=ON -DWITH_ZSTD=ON -DWITH_LZ4=ON -DWITH_ZLIB=ON -DWITH_BZ2=ON \
      -DUSE_RTTI=1 -DFAIL_ON_WARNINGS=NO && \
    cmake --build /tmp/rocksdb/build --target rocksdb --parallel 4 && \
    mkdir -p /usr/local/include /usr/local/lib && \
    cp -a /tmp/rocksdb/include/rocksdb /usr/local/include/ && \
    cp -a /tmp/rocksdb/build/librocksdb.a /usr/local/lib/

FROM golang:1.27-alpine3.24 AS release-base
ARG BUILD_TAGS="kafka nats clickhouse databricks s3 pyroscope"
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /build
RUN apk add --no-cache git make build-base bzip2-dev bzip2-static \
    lz4-dev lz4-static snappy-dev snappy-static zlib-dev zlib-static zstd-dev zstd-static
COPY --from=rocksdb-static-build /usr/local/include/rocksdb /usr/local/include/rocksdb
COPY --from=rocksdb-static-build /usr/local/lib/librocksdb.a /usr/local/lib/
ENV CGO_ENABLED=1 CGO_CFLAGS=-I/usr/local/include
ENV CGO_LDFLAGS="-L/usr/local/lib -lzstd -llz4 -lsnappy -lz -lbz2 -lstdc++ -lm"
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY main.go .
COPY internal internal
COPY cmd cmd
COPY pkg pkg
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -tags "${BUILD_TAGS}" -ldflags "-s -w -linkmode external -extldflags '-static' -X github.com/formancehq/ledger/v3/internal/pkg/version.Version=${VERSION} -X github.com/formancehq/ledger/v3/internal/pkg/version.Commit=${COMMIT} -X github.com/formancehq/ledger/v3/internal/pkg/version.BuildDate=${BUILD_DATE}" -o /out/ledger-server . && \
    go build -tags "${BUILD_TAGS}" -ldflags "-s -w -linkmode external -extldflags '-static' -X github.com/formancehq/ledger/v3/internal/pkg/version.Version=${VERSION} -X github.com/formancehq/ledger/v3/internal/pkg/version.Commit=${COMMIT} -X github.com/formancehq/ledger/v3/internal/pkg/version.BuildDate=${BUILD_DATE}" -o /out/ledgerctl ./cmd/ledgerctl

FROM scratch AS release-artifacts
COPY --from=release-base /out/ledger-server /ledger-server
COPY --from=release-base /out/ledgerctl /ledgerctl

FROM golang:1.27-alpine3.24 AS base
ARG BUILD_TAGS=""
WORKDIR /build
RUN apk add --no-cache git make build-base bzip2-dev lz4-dev snappy-dev zlib-dev zstd-dev
COPY --from=rocksdb-build /usr/local/include/rocksdb /usr/local/include/rocksdb
COPY --from=rocksdb-build /usr/local/lib/librocksdb.so* /usr/local/lib/
ENV CGO_CFLAGS=-I/usr/local/include CGO_LDFLAGS=-L/usr/local/lib LD_LIBRARY_PATH=/usr/local/lib
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download
ENV CGO_ENABLED=1
COPY main.go .
COPY internal internal
COPY cmd cmd
COPY pkg pkg
RUN go test ./internal/storage/dal -run '^TestStore_GetMetrics$' -count=1

FROM base AS build-server
ARG BUILD_TAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -tags "${BUILD_TAGS}" -o ledger-server .

FROM base AS build-ledgerctl
ARG BUILD_TAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -tags "${BUILD_TAGS}" -o ledgerctl ./cmd/ledgerctl

FROM alpine:3.24
RUN apk --no-cache add ca-certificates tzdata bash bash-completion libstdc++ libbz2 lz4-libs snappy zlib zstd-libs && \
    sed -i 's|/bin/ash|/bin/bash|' /etc/passwd
COPY --from=rocksdb-build /usr/local/lib/librocksdb.so* /usr/local/lib/
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV TZ=UTC
ENV PATH=$PATH:/app
SHELL ["/bin/bash", "-c"]
WORKDIR /app
COPY --from=build-server /build/ledger-server .
COPY --from=build-ledgerctl /build/ledgerctl .
RUN ./ledgerctl completion bash > /etc/bash_completion.d/ledgerctl
ENTRYPOINT ["./ledger-server"]
