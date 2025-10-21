# Compiler
FROM golang:1.24-trixie AS compiler

RUN apt-get update && apt-get install -y bash git gcc

WORKDIR /src/pkg/client
COPY pkg/client/go.mod pkg/client/go.sum ./
RUN go mod download

WORKDIR /src
COPY go.mod go.sum main.go ./
COPY internal internal
COPY pkg pkg
COPY cmd cmd

RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-instrumentor@latest

RUN mkdir /instrumented
RUN antithesis-go-instrumentor . /instrumented
WORKDIR /instrumented/customer
RUN go build -race -o ledger

# Runner
FROM debian:trixie

RUN apt-get update && apt-get install -y postgresql-client curl

COPY --from=compiler /instrumented/customer/ledger /bin/ledger
COPY --from=compiler /instrumented/symbols/ /symbols
COPY test/antithesis/image/entrypoint.sh /bin/entrypoint.sh

RUN chmod 777 /bin/entrypoint.sh

ENTRYPOINT ["/bin/entrypoint.sh"]
EXPOSE 8080
