BINARY_NAME=numary
PKG=./...
NUMARY_STORAGE_DRIVER="postgres"
NUMARY_STORAGE_POSTGRES_CONN_STRING="postgresql://ledger:ledger@127.0.0.1/ledger"
FAILFAST=-failfast
TIMEOUT=30s
RUN=".*"

all: lint test

build:
	go build -o $(BINARY_NAME)

install: build
	cp $(BINARY_NAME) $(shell go env GOPATH)/bin
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
		| sh -s -- -b $(shell go env GOPATH)/bin latest
	golangci-lint --version

lint:
	golangci-lint run -v --fix

test: test-sqlite test-postgres
	@go tool cover -html=coverage-sqlite.out -o coverage-sqlite.html
	@go tool cover -html=coverage-postgres.out -o coverage-postgres.html
	@echo "To open the html coverage file, use one of the following commands:"
	@echo "open coverage-sqlite.html or coverage-postgres.html on mac"
	@echo "xdg-open coverage-sqlite.html or coverage-postgres.html on linux"

test-sqlite:
	go test -tags json1 -v $(FAILFAST) -coverpkg $(PKG) -coverprofile coverage-sqlite.out -covermode atomic -run $(RUN) -timeout $(TIMEOUT) $(PKG) \
		| sed ''/PASS/s//$(shell printf "\033[32mPASS\033[0m")/'' \
		| sed ''/FAIL/s//$(shell printf "\033[31mFAIL\033[0m")/'' \
		| sed ''/RUN/s//$(shell printf "\033[34mRUN\033[0m")/''

test-postgres: postgres
	NUMARY_STORAGE_DRIVER=$(NUMARY_STORAGE_DRIVER) \
	NUMARY_STORAGE_POSTGRES_CONN_STRING=$(NUMARY_STORAGE_POSTGRES_CONN_STRING) \
	go test -tags json1 -v $(FAILFAST) -coverpkg $(PKG) -coverprofile coverage-postgres.out -covermode atomic -run $(RUN) -timeout $(TIMEOUT) $(PKG) \
		| sed ''/PASS/s//$(shell printf "\033[32mPASS\033[0m")/'' \
		| sed ''/FAIL/s//$(shell printf "\033[31mFAIL\033[0m")/'' \
		| sed ''/RUN/s//$(shell printf "\033[34mRUN\033[0m")/''

postgres:
	docker-compose up -d postgres

bench:
	go test -tags json1 -bench=. -run=^a $(PKG)

clean:
	docker-compose down -v
	go clean
	rm -f $(BINARY_NAME) $(COVERAGE_FILE)
