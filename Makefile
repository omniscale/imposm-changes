BUILD_DATE=$(shell date +%Y%m%d)
BUILD_REV=$(shell git rev-parse --short HEAD)
BUILD_VERSION=dev-$(BUILD_DATE)-$(BUILD_REV)
VERSION_LDFLAGS=-X github.com/omniscale/imposm-changes.buildVersion=$(BUILD_VERSION)
LDFLAGS=-ldflags '$(VERSION_LDFLAGS)'

imposm-changes-linux:
	GOOS=linux go build -a $(LDFLAGS) ./cmd/imposm-changes
	mkdir -p dist
	mv imposm-changes dist/imposm-changes-x64-linux-$(BUILD_VERSION)

test-coverage:
	go test -coverprofile go-osm.coverprofile -coverpkg ./... -covermode count ./...
test-coverage-html: test-coverage
	go tool cover -html go-osm.coverprofile
