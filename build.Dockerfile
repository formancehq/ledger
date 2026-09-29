# Keep the native runtime aligned with the RocksDB version and Alpine packages
# used by Dockerfile. GoReleaser binaries still need a musl build before release.
FROM alpine:3.24 AS rocksdb-build
RUN apk add --no-cache build-base linux-headers cmake ninja bzip2-dev lz4-dev snappy-dev zlib-dev zstd-dev
ADD --checksum=sha256:3aba946031d27734eeea68ced7dd6f46629ba23a7b49945eb29aea68d1595311 \
    https://github.com/facebook/rocksdb/archive/refs/tags/v11.1.2.tar.gz /tmp/rocksdb.tar.gz
RUN mkdir -p /tmp/rocksdb && tar -xzf /tmp/rocksdb.tar.gz -C /tmp/rocksdb --strip-components=1 && \
    cmake -S /tmp/rocksdb -B /tmp/rocksdb/build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DPORTABLE=1 -DWITH_TESTS=OFF \
      -DWITH_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_GFLAGS=OFF -DWITH_LIBURING=OFF -DROCKSDB_BUILD_SHARED=ON \
      -DUSE_RTTI=1 -DFAIL_ON_WARNINGS=NO && \
    cmake --build /tmp/rocksdb/build --target rocksdb-shared --parallel 4 && \
    mkdir -p /usr/local/lib && cp -a /tmp/rocksdb/build/librocksdb.so* /usr/local/lib/

FROM alpine:3.24
RUN apk --no-cache add ca-certificates tzdata bash bash-completion libstdc++ libbz2 lz4-libs snappy zlib zstd-libs && \
    sed -i 's|/bin/ash|/bin/bash|' /etc/passwd
COPY --from=rocksdb-build /usr/local/lib/librocksdb.so* /usr/local/lib/
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV TZ=UTC
ENV PATH=$PATH:/app
SHELL ["/bin/bash", "-c"]
WORKDIR /app
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/ledger-server .
COPY $TARGETPLATFORM/ledgerctl .
RUN ./ledgerctl completion bash > /etc/bash_completion.d/ledgerctl
ENTRYPOINT ["./ledger-server"]
