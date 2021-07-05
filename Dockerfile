FROM alpine:latest

ADD numary /usr/local/bin/numary

EXPOSE 3068

CMD ["numary", "server", "start"]