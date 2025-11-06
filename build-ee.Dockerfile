FROM ghcr.io/formancehq/base:22.04
COPY ledger-ee /usr/bin/ledger
ENV OTEL_SERVICE_NAME ledger
ENTRYPOINT ["/usr/bin/ledger"]
CMD ["serve"]
