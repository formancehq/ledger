FROM ubuntu:20.04
COPY numary /usr/local/bin/numary
EXPOSE 3068
CMD ["numary", "server", "start"]
