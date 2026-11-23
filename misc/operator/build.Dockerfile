# Packaging-only image for GoReleaser: it receives the prebuilt operator binary
# in the build context, laid out as <os>/<arch>/operator. CI uses this for
# release, nightly, and PR images. The adjacent Dockerfile builds from source
# for local and end-to-end builds.
FROM alpine:3

RUN apk add --no-cache ca-certificates

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/operator /usr/bin/operator

USER 65532:65532

ENTRYPOINT ["/usr/bin/operator"]
