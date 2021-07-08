test:
	go test -v -coverprofile=coverage.out -coverpkg=./... ./...

bench:
	go test -test.bench=. -run=^a ./...

# curl -OL https://github.com/numary/control/releases/latest/download/numary-control.tar.gz && \

fetch-control:
	mkdir -p .control
	cd .control && \
	tar -xvf numary-control.tar.gz