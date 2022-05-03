#FROM golang:1.18-alpine as src-alpine
#ARG VERSION=latest
#ARG APP_SHA
#RUN apk add --no-cache git gcc build-base
#COPY . /app
#WORKDIR /app
#RUN CGO_ENABLED=1 go build -o numary  \
#    -ldflags="-X github.com/numary/ledger/cmd.Version=${VERSION} \
#    -X github.com/numary/ledger/cmd.BuildDate=$(date +%s) \
#    -X github.com/numary/ledger/cmd.Commit=${APP_SHA}" ./
#
#FROM alpine
#RUN apk add --no-cache ca-certificates curl
#COPY --from=src-alpine /app/numary /usr/local/bin/numary
#EXPOSE 3068
#CMD ["numary", "server", "start"]
FROM --platform=$BUILDPLATFORM golang:1.18 AS builder
RUN apt-get update && \
    apt-get install -y gcc-aarch64-linux-gnu gcc-x86-64-linux-gnu && \
    ln -s /usr/bin/aarch64-linux-gnu-gcc /usr/bin/arm64-linux-gnu-gcc  && \
    ln -s /usr/bin/x86_64-linux-gnu-gcc /usr/bin/amd64-linux-gnu-gcc
# 1. Precompile the entire go standard library into the first Docker cache layer: useful for other projects too!
RUN CGO_ENABLED=1 GOOS=linux go install -v -installsuffix cgo -a std
ARG TARGETARCH
ARG APP_SHA
ARG VERSION
WORKDIR /go/src/github.com/numary/ledger
# get deps first so it's cached
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go mod download
COPY . .
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH \
    CC=$TARGETARCH-linux-gnu-gcc \
    go build -o numary  \
    -ldflags="-X github.com/numary/ledger/cmd.Version=${VERSION} \
    -X github.com/numary/ledger/cmd.BuildDate=$(date +%s) \
    -X github.com/numary/ledger/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
COPY --from=builder /go/src/github.com/numary/ledger/numary /usr/local/bin/numary
EXPOSE 3068
CMD ["numary", "server", "start"]