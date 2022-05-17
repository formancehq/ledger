test:
	go test -v -coverprofile=coverage.out -coverpkg=./... ./...

bench:
	go test -bench=. -run=^a ./...

PKG=./...

test-sqlite:
	go test -v -tags=json1 -coverpkg=$(PKG) -coverprofile=coverage.out -covermode=atomic $(PKG) \
		| sed ''/PASS/s//$(shell printf "\033[32mPASS\033[0m")/'' \
		| sed ''/FAIL/s//$(shell printf "\033[31mFAIL\033[0m")/'' \
		| sed ''/RUN/s//$(shell printf "\033[34mRUN\033[0m")/''
