test:
	go test -v -coverprofile=coverage.out -coverpkg=./... ./...

bench:
	go test -test.bench=. -run=^a ./...

fetch-control:
	cd cmd/control && \
	curl -OL https://numary-control-releases.s3.eu-west-1.amazonaws.com/numary-control.tar.gz && \
	tar -zxvf numary-control.tar.gz && \
	rm numary-control.tar.gz