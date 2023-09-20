FROM golang:1.20-alpine3.16 AS builder
ARG SEGMENT_WRITE_KEY
ARG VERSION
ARG APP_SHA

WORKDIR /src
COPY libs libs
COPY components/ledger components/ledger
WORKDIR components/ledger
RUN --mount=type=cache,mode=0755,target=/go/pkg/mod go mod download
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go build -o ledger \
    -ldflags="-X github.com/formancehq/ledger/cmd.Version=${VERSION} \
    -X github.com/formancehq/ledger/cmd.BuildDate=$(date +%s) \
    -X github.com/formancehq/ledger/cmd.Commit=${APP_SHA} \
    -X github.com/formancehq/ledger/cmd.DefaultSegmentWriteKey=${SEGMENT_WRITE_KEY}" ./

FROM alpine:3.16 as app
RUN apk update && apk add ca-certificates
COPY --from=builder /src/components/ledger/ledger /usr/local/bin/ledger
RUN chmod +x /usr/local/bin/ledger
EXPOSE 3068
ENTRYPOINT ["ledger"]
ENV OTEL_SERVICE_NAME ledger
CMD ["serve"]
