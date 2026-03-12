FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETARCH

RUN apk add --no-cache ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY api/ ./api/
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN mkdir out
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o /out/benchmark-operator ./cmd/benchmark-operator

FROM alpine:3.20
RUN apk add --no-cache ca-certificates

COPY --from=build /out/benchmark-operator /usr/local/bin/benchmark-operator

ENTRYPOINT ["/usr/local/bin/benchmark-operator"]
