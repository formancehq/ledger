# Build stage: always runs on the build host's native platform
FROM --platform=$BUILDPLATFORM golang:1.26 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Cross-compile natively using Go's built-in support (no QEMU needed)
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o build/operator ./cmd/operator

# Final stage: uses the target platform
FROM alpine:3

RUN apk add --no-cache ca-certificates

COPY --from=builder /workspace/build/operator /usr/bin/operator

USER 65532:65532

ENTRYPOINT ["/usr/bin/operator"]
