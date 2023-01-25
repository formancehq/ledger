FROM --platform=$BUILDPLATFORM golang:1.18 AS builder
# 1. Precompile the entire go standard library into the first Docker cache layer: useful for other projects too!
ARG APP_SHA
ARG VERSION
WORKDIR /go/src/github.com/formancehq/go-project-template
# get deps first so it's cached
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,id=gomod,target=/go/go-project-template/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go mod download
COPY . .
RUN --mount=type=cache,id=gomod,target=/go/go-project-template/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -o go-project-template \
    -ldflags="-X github.com/formancehq/go-project-template/cmd.Version=${VERSION} \
    -X github.com/formancehq/go-project-template/cmd.BuildDate=$(date +%s) \
    -X github.com/formancehq/go-project-template/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go/src/github.com/formancehq/go-project-template/go-project-template /usr/local/bin/go-project-template
EXPOSE 8080
ENTRYPOINT ["go-project-template"]
CMD ["server"]
