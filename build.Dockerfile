# GoReleaser imports fully static binaries from Dockerfile's release-artifacts
# stage. The image does not need a native RocksDB runtime.
FROM alpine:3.24
RUN apk --no-cache add ca-certificates tzdata bash bash-completion && \
    sed -i 's|/bin/ash|/bin/bash|' /etc/passwd
ENV TZ=UTC
ENV PATH=$PATH:/app
SHELL ["/bin/bash", "-c"]
WORKDIR /app
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/ledger-server .
COPY $TARGETPLATFORM/ledgerctl .
RUN ./ledgerctl completion bash > /etc/bash_completion.d/ledgerctl
ENTRYPOINT ["./ledger-server"]
