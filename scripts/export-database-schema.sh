#!/bin/bash

echo "Creating PG server..."
postgresContainerID=$(docker run -d --rm -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -e POSTGRES_DB=formance --net=host postgres:15-alpine)
wait-for-it -w 127.0.0.1:5432

echo "Creating bucket..."
go run main.go buckets upgrade _default --postgres-uri "postgres://root:root@127.0.0.1:5432/formance?sslmode=disable"

echo "Exporting schemas..."
docker run --rm -u root \
  -v ./docs/database:/output \
  --net=host \
  schemaspy/schemaspy:6.2.4 -u root -db formance -t pgsql11 -host 127.0.0.1 -port 5432 -p root -schemas _system,_default

docker kill "$postgresContainerID"