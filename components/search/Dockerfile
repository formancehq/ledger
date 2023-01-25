FROM --platform=$BUILDPLATFORM golang:1.18 AS builder
# 1. Precompile the entire go standard library into the first Docker cache layer: useful for other projects too!
ARG APP_SHA
ARG VERSION
WORKDIR /go/src/github.com/numary/search
# get deps first so it's cached
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,id=gomod,target=/go/bridge/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go mod download
COPY . .
RUN --mount=type=cache,id=gomod,target=/go/bridge/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -o search \
    -ldflags="-X github.com/numary/search/cmd.Version=${VERSION} \
    -X github.com/numary/search/cmd.BuildDate=$(date +%s) \
    -X github.com/numary/search/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go/src/github.com/numary/search/search /usr/local/bin/search
EXPOSE 8080
ENV OTEL_SERVICE_NAME search
ENTRYPOINT ["search"]
CMD ["serve"]
