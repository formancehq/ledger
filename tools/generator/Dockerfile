FROM golang:1.25-alpine AS compiler
WORKDIR /src
COPY --from=root pkg pkg
COPY --from=root internal internal
COPY --from=root cmd cmd
COPY --from=root go.* .
COPY --from=root *.go .

WORKDIR /src/tools/generator
COPY go.* .
RUN --mount=type=cache,target=$GOPATH go mod download
COPY main.go .
COPY cmd /src/tools/generator/cmd
RUN --mount=type=cache,target=$GOPATH go build -o generator

FROM alpine:3.22
LABEL org.opencontainers.image.source=https://github.com/formancehq/ledger
COPY --from=compiler /src/tools/generator/generator /bin/generator
ENTRYPOINT ["/bin/generator"]
CMD ["--help"]