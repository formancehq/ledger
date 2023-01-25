FROM --platform=$BUILDPLATFORM golang:1.18 AS builder
# 1. Precompile the entire go standard library into the first Docker cache layer: useful for other projects too!
ARG APP_SHA
ARG VERSION
WORKDIR /go/src/github.com/formancehq/orchestration
# get deps first so it's cached
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,id=gomod,target=/go/orchestration/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go mod download
COPY . .
RUN --mount=type=cache,id=gomod,target=/go/orchestration/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -o orchestration \
    -ldflags="-X github.com/formancehq/orchestration/cmd.Version=${VERSION} \
    -X github.com/formancehq/orchestration/cmd.BuildDate=$(date +%s) \
    -X github.com/formancehq/orchestration/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go/src/github.com/formancehq/orchestration/orchestration /usr/local/bin/orchestration
EXPOSE 8080
ENTRYPOINT ["orchestration"]
CMD ["serve"]
