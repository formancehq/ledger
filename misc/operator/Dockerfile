FROM --platform=$BUILDPLATFORM golang:1.26 AS builder
ARG TARGETARCH

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o build/operator ./cmd/operator

FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /workspace/build/operator .

USER 65532:65532

ENTRYPOINT ["/operator"]
