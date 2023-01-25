FROM --platform=$BUILDPLATFORM golang:1.18 AS builder
# 1. Precompile the entire go standard library into the first Docker cache layer: useful for other projects too!
ARG APP_SHA
ARG VERSION
COPY . /go/src/github.com/formancehq/fctl
WORKDIR /go/src/github.com/formancehq/fctl
# get deps first so it's cached
RUN --mount=type=cache,id=gomod,target=/go/fctl/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -o fctl \
    -ldflags="-X github.com/formancehq/fctl/cmd.Version=${VERSION} \
    -X github.com/formancehq/fctl/cmd.BuildDate=$(date +%s) \
    -X github.com/formancehq/fctl/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go/src/github.com/formancehq/fctl/fctl /usr/local/bin/fctl
EXPOSE 8080
ENTRYPOINT ["fctl"]
