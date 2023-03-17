fmt:
	go fmt
	gofumpt -l -w *.go

lint:
	golangci-lint run ./...
