# Packaging-only image for GoReleaser: it receives the prebuilt operator binary
# in the build context, laid out as <os>/<arch>/operator. The Dockerfile next
# to it compiles from source and stays the source of truth for local and e2e
# builds.
FROM alpine:3

RUN apk add --no-cache ca-certificates

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/operator /usr/bin/operator

USER 65532:65532

ENTRYPOINT ["/usr/bin/operator"]
