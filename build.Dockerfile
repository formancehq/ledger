FROM alpine:latest
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
