FROM golang:1.25-alpine AS compiler
WORKDIR /src
COPY --from=root pkg pkg
COPY --from=root internal internal
COPY --from=root cmd cmd
COPY --from=root go.* .
COPY --from=root *.go .

WORKDIR /src/tools/provisioner
COPY go.* .
RUN --mount=type=cache,target=$GOPATH go mod download
COPY main.go .
COPY cmd /src/tools/provisioner/cmd
COPY pkg /src/tools/provisioner/pkg
RUN --mount=type=cache,target=$GOPATH go build -o provisioner

FROM alpine:3.22
LABEL org.opencontainers.image.source=https://github.com/formancehq/ledger
COPY --from=compiler /src/tools/provisioner/provisioner /bin/provisioner
ENTRYPOINT ["/bin/provisioner"]
CMD ["--help"]