FROM golang:1.18-alpine as src-alpine
ARG VERSION=latest
ARG APP_SHA
RUN apk add --no-cache git gcc build-base
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=1 GOOS=linux go build -o numary  \
    -ldflags="-X github.com/numary/ledger/cmd.Version=${VERSION} \
    -X github.com/numary/ledger/cmd.BuildDate=$(date +%s) \
    -X github.com/numary/ledger/cmd.Commit=${APP_SHA}" ./

FROM alpine
RUN apk add --no-cache ca-certificates curl
COPY --from=src-alpine /app/numary /usr/local/bin/numary
EXPOSE 3068
CMD ["numary", "server", "start"]