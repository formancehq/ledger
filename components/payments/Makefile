docker-build:
	DOCKER_BUILDKIT=1 docker build -t payments:local --build-arg BUILDPLATFORM=amd64 .

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

run-tests:
	go test -race -count=1 ./...
