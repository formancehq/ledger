FROM public.ecr.aws/docker/library/golang:1.18 AS builder

RUN mkdir -p ${GOPATH:-/go}/src/temporalite && \
    git clone https://github.com/temporalio/temporalite.git ${GOPATH:-/go}/src/temporalite && \
    cd ${GOPATH:-/go}/src/temporalite && \
    git switch -c tmp v0.2.0 && \
    go mod download && \
    go get -d -v ./... && \
    go build -o ${GOPATH:-/go}/bin/ ${GOPATH:-/go}/src/temporalite/cmd/temporalite

FROM public.ecr.aws/debian/debian:stable-slim

COPY --from=builder ${GOPATH:-/go}/bin/temporalite /bin
EXPOSE 7233 8233

ENTRYPOINT ["/bin/temporalite", "start", "--ephemeral", "-n", "default", "--ip" , "0.0.0.0"]
