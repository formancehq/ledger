FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS base
ARG TARGETARCH
ARG BUILD_TAGS=""
WORKDIR /build
RUN apk add --no-cache git make
COPY go.mod go.sum ./
RUN go mod download
ENV GOEXPERIMENT=jsonv2
ENV CGO_ENABLED=0
ENV GOOS=linux
COPY main.go .
COPY internal internal
COPY cmd cmd

FROM base AS build-server
ARG TARGETARCH
ARG BUILD_TAGS
RUN GOARCH=$TARGETARCH go build -tags "${BUILD_TAGS}" -o ledger-v3-poc .

FROM base AS build-ledgerctl
ARG TARGETARCH
ARG BUILD_TAGS
RUN GOARCH=$TARGETARCH go build -tags "${BUILD_TAGS}" -o ledgerctl ./cmd/ledgerctl

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=UTC
ENV PATH=$PATH:/app
ENV INSECURE=true
WORKDIR /app
COPY --from=build-server /build/ledger-v3-poc .
COPY --from=build-ledgerctl /build/ledgerctl .
ENTRYPOINT ["./ledger-v3-poc"]
