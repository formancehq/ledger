FROM ghcr.io/formancehq/base:scratch
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/ledger /usr/bin/ledger
ENV OTEL_SERVICE_NAME ledger
ENTRYPOINT ["/usr/bin/ledger"]
CMD ["serve"]
