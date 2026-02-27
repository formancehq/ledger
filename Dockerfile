FROM golang:1.26-alpine AS builder
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
# Build client binary
RUN go build -o ledgerctl ./cmd/ledgerctl
# Build server binary
RUN go build -o ledger-v3-poc .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=UTC
ENV PATH=$PATH:/app
ENV INSECURE=true
WORKDIR /app
COPY --from=builder /build/ledger-v3-poc .
COPY --from=builder /build/ledgerctl .
ENTRYPOINT ["./ledger-v3-poc"]
