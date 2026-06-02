FROM golang:1.26-alpine AS base
ARG GOARCH
ARG GOOS
ARG BUILD_TAGS=""
WORKDIR /build
RUN apk add --no-cache git make
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download
ENV CGO_ENABLED=0
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
    go build -tags "${BUILD_TAGS}" -o ledger-server .

FROM base AS build-ledgerctl
ARG GOARCH
ARG GOOS
ARG BUILD_TAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -tags "${BUILD_TAGS}" -o ledgerctl ./cmd/ledgerctl

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata bash bash-completion && \
    sed -i 's|/bin/ash|/bin/bash|' /etc/passwd
ENV TZ=UTC
ENV PATH=$PATH:/app
ENV INSECURE=true
SHELL ["/bin/bash", "-c"]
WORKDIR /app
COPY --from=build-server /build/ledger-server .
COPY --from=build-ledgerctl /build/ledgerctl .
RUN ./ledgerctl completion bash > /etc/bash_completion.d/ledgerctl
ENTRYPOINT ["./ledger-server"]
