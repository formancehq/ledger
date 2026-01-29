FROM golang:1.25-alpine AS compiler
RUN apk add --no-cache --update go gcc g++ make build-base sqlite
ENV CGO_ENABLED=1

FROM compiler AS builder
WORKDIR /build
RUN apk add --no-cache git make
COPY go.mod go.sum ./
COPY pkg/client/go.* pkg/client/
RUN go mod download
ENV GOEXPERIMENT=jsonv2
ENV CGO_ENABLED=1
ENV GOOS=linux
RUN go install github.com/mattn/go-sqlite3
COPY main.go .
COPY internal internal
COPY cmd cmd
# Build client binary
RUN go build -o ledgerctl ./cmd/client
# Build server binary
RUN go build -o ledger-v3-poc .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=UTC
WORKDIR /app
COPY --from=builder /build/ledger-v3-poc .
COPY --from=builder /build/ledgerctl .
ENTRYPOINT ["./ledger-v3-poc"]

