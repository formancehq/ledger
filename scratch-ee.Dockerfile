FROM ghcr.io/formancehq/base:scratch
COPY ledger-ee /usr/bin/ledger
ENV OTEL_SERVICE_NAME ledger
ENTRYPOINT ["/usr/bin/ledger"]
CMD ["serve"]
