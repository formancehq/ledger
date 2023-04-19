FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY numary /usr/bin/numary
ENV OTEL_SERVICE_NAME numary
ENTRYPOINT ["/usr/bin/numary"]
CMD ["serve", "start"]
