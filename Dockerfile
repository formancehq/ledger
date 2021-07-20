FROM archlinux:latest

ADD dist/ledger_linux_amd64/numary /usr/local/bin/numary

EXPOSE 3068

CMD ["numary", "server", "start"]
