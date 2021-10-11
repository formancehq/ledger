FROM alpine

RUN apk add --update-cache curl \
   && rm -rf /var/cache/apk/*
COPY numary /usr/local/bin/numary

EXPOSE 3068

CMD ["numary", "server", "start"]
