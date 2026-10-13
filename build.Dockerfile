FROM ghcr.io/formancehq/base:22.04
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/ledger /usr/bin/ledger
ENV OTEL_SERVICE_NAME ledger
ENTRYPOINT ["/usr/bin/ledger"]
CMD ["serve"]