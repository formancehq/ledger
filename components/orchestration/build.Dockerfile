FROM golang:1.18 AS builder
ARG APP_SHA
ARG VERSION
WORKDIR /src
COPY . .
WORKDIR /src/components/orchestration
RUN go mod download
RUN GOOS=linux go build -o orchestration \
    -ldflags="-X $(cat go.mod |head -1|cut -d \  -f2)/cmd.Version=${VERSION} \
    -X $(cat go.mod |head -1|cut -d \  -f2)/cmd.BuildDate=$(date +%s) \
    -X $(cat go.mod |head -1|cut -d \  -f2)/cmd.Commit=${APP_SHA}" ./

FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/components/orchestration/orchestration /orchestration
EXPOSE 3068
ENV OTEL_SERVICE_NAME orchestration
ENTRYPOINT ["/orchestration"]
CMD ["serve"]
