FROM golang:1.25-alpine AS compiler
RUN apk add --no-cache --update go gcc g++ make build-base
ENV CGO_ENABLED=1

FROM compiler AS builder
WORKDIR /build
RUN apk add --no-cache git make
COPY go.mod go.sum ./
COPY pkg/client/go.* pkg/client/
RUN go mod download
COPY main.go .
COPY internal internal
COPY cmd cmd
ENV GOEXPERIMENT=jsonv2
ENV CGO_ENABLED=1
RUN GOOS=linux go build -a -o ledger-v3-poc .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=UTC
WORKDIR /app
COPY --from=builder /build/ledger-v3-poc .
ENTRYPOINT ["./ledger-v3-poc"]

