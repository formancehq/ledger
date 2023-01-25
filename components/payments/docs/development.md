# Development

## Run

A docker-compose contains all the stuff required to launch the service.

Currently, the service use MongoDB as database, and it takes few seconds to start and is not ready when the payments service try to connect to him.
You can start MongoDB before and wait before start payments service using two terminal :
```
docker compose up mongodb # Run on first terminal
```
and
```
docker compose up payments # Run on second terminal
```

Tests can be started regularly using standard go tooling, just use :
```
go test ./...
```

## Develop a connector

Want to develop a connector? [Follow this link](./tuto-connector.md)
