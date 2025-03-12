FROM golang:1.24-alpine AS compiler
WORKDIR /src
COPY go.* .
RUN --mount=type=cache,target=$GOPATH go mod download
COPY main.go .
COPY cmd /src/cmd
COPY pkg /src/pkg
RUN --mount=type=cache,target=$GOPATH go build -o provisioner

FROM alpine:3.21
LABEL org.opencontainers.image.source=https://github.com/formancehq/ledger
COPY --from=compiler /src/provisioner /bin/provisioner
ENTRYPOINT ["/bin/provisioner"]
CMD ["--help"]