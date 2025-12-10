FROM golang:1.25-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
COPY pkg/client/go.* pkg/client/
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ledger-v3-poc ./cmd/server

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
ENV TZ=UTC

WORKDIR /app

COPY --from=builder /build/ledger-v3-poc .

ENTRYPOINT ["./ledger-v3-poc"]

