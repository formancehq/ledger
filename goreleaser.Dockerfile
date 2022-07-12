FROM ubuntu:jammy
RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY numary /usr/local/bin/numary
EXPOSE 3068
CMD ["numary", "server", "start"]
