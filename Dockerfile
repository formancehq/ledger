FROM ubuntu:20.04 as ubuntu
COPY numary /usr/local/bin/numary
EXPOSE 3068
CMD ["numary", "server", "start"]

FROM golang:1.16-alpine as src-alpine
ARG VERSION=latest
RUN apk add --no-cache git gcc build-base
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o numary ./

FROM alpine
RUN apk add --no-cache ca-certificates curl
COPY --from=src-alpine /app/numary /usr/local/bin/numary
EXPOSE 80
CMD ["numary", "server", "start"]