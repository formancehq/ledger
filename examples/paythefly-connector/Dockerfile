FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o paythefly-connector .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/paythefly-connector /usr/local/bin/
ENTRYPOINT ["paythefly-connector"]
