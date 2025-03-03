BINARY_NAME=quacfka-service
LDFLAGS="-s -w"
build:
	CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build \
		-o ${BINARY_NAME} \
		-ldflags=${LDFLAGS}

run: 
	CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build \
		-o ${BINARY_NAME} \
		-ldflags=${LDFLAGS}
	./${BINARY_NAME}

vendor:
	go mod vendor

clean:
	go clean
	rm ${BINARY_NAME}

test:
	go test ./...

test_coverage:
	go test ./... -coverprofile=coverage.out

dep:
	go mod download

vet:
	go vet

format:
	gofumpt -l -w ./

lint:
	golangci-lint run --enable-all