# Makefile for TiDB Data Generator

# Variables
BINARY_NAME=tidb_data_generator
GO=go
GOOS?=$(shell go env GOOS)
GOARCH?=$(shell go env GOARCH)

# Build flags
LDFLAGS=-ldflags "-s -w"

# Test flags
TEST_FLAGS=-v -race -cover
INTEGRATION_TEST_FLAGS=-v -tags=integration

# Default target
.PHONY: all
all: build

# Build the binary
.PHONY: build
build:
	$(GO) build $(LDFLAGS) -o $(BINARY_NAME) main.go

# Build for different platforms
.PHONY: build-linux
build-linux:
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 main.go

.PHONY: build-darwin
build-darwin:
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 main.go

.PHONY: build-windows
build-windows:
	GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe main.go

# Run unit tests
.PHONY: test
test:
	$(GO) test $(TEST_FLAGS) ./...

# Run unit tests with coverage
.PHONY: test-coverage
test-coverage:
	$(GO) test $(TEST_FLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run integration tests (requires TiDB running)
.PHONY: test-integration
test-integration:
	$(GO) test $(INTEGRATION_TEST_FLAGS) -run "TestDatabase|TestParseTableFromDB" ./...

# Run all tests
.PHONY: test-all
test-all: test test-integration

# Run benchmarks
.PHONY: benchmark
benchmark:
	$(GO) test -bench=. -benchmem ./...

# Run specific benchmarks
.PHONY: benchmark-generate
benchmark-generate:
	$(GO) test -bench=BenchmarkGenerate -benchmem ./...

.PHONY: benchmark-insert
benchmark-insert:
	$(GO) test -bench=BenchmarkDatabase -benchmem ./...

# Clean build artifacts
.PHONY: clean
clean:
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-*
	rm -f coverage.out
	rm -f coverage.html
	rm -f *.test

# Install dependencies
.PHONY: deps
deps:
	$(GO) mod download
	$(GO) mod tidy

# Format code
.PHONY: fmt
fmt:
	$(GO) fmt ./...

# Run linter
.PHONY: lint
lint:
	golangci-lint run

# Run linter with auto-fix
.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix

# Check for security vulnerabilities
.PHONY: security
security:
	gosec ./...

# Generate documentation
.PHONY: docs
docs:
	godoc -http=:6060

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build              - Build the binary"
	@echo "  build-linux        - Build for Linux"
	@echo "  build-darwin       - Build for macOS"
	@echo "  build-windows      - Build for Windows"
	@echo "  test               - Run unit tests"
	@echo "  test-coverage      - Run tests with coverage report"
	@echo "  test-integration   - Run integration tests (requires TiDB)"
	@echo "  test-all           - Run all tests"
	@echo "  benchmark          - Run all benchmarks"
	@echo "  benchmark-generate - Run generation benchmarks"
	@echo "  benchmark-insert   - Run insertion benchmarks"
	@echo "  clean              - Clean build artifacts"
	@echo "  deps               - Install dependencies"
	@echo "  fmt                - Format code"
	@echo "  lint               - Run linter"
	@echo "  lint-fix           - Run linter with auto-fix"
	@echo "  security           - Check for security vulnerabilities"
	@echo "  docs               - Start documentation server"
	@echo "  help               - Show this help message"

# Example usage targets
.PHONY: example-json
example-json:
	./$(BINARY_NAME) -f t.create.sql -n 10

.PHONY: example-json-stats
example-json-stats:
	./$(BINARY_NAME) -f t.create.sql -n 10 -s t.stats.json

.PHONY: example-db-insert
example-db-insert:
	./$(BINARY_NAME) -H localhost -P 4000 -u root -D test -t t -n 100 -s t.stats.json -i

.PHONY: example-db-bulk
example-db-bulk:
	./$(BINARY_NAME) -H localhost -P 4000 -u root -D test -t t -n 1000 -s t.stats.json -i --bulk

.PHONY: example-db-parallel
example-db-parallel:
	./$(BINARY_NAME) -H localhost -P 4000 -u root -D test -t t -n 1000 -s t.stats.json -i --parallel --workers 4

.PHONY: example-db-auto-tune
example-db-auto-tune:
	./$(BINARY_NAME) -H localhost -P 4000 -u root -D test -t t -n 10000 -s t.stats.json -i --parallel --bulk --workers 0

# Development targets
.PHONY: dev-build
dev-build:
	$(GO) build -race -o $(BINARY_NAME) main.go

.PHONY: dev-test
dev-test:
	$(GO) test -v -race -short ./...

.PHONY: dev-run
dev-run: dev-build
	./$(BINARY_NAME) -h

# CI/CD targets
.PHONY: ci-build
ci-build: deps lint test build

.PHONY: ci-test
ci-test: deps test test-integration

.PHONY: ci-benchmark
ci-benchmark: deps benchmark

# Release targets
.PHONY: release-build
release-build: build-linux build-darwin build-windows

.PHONY: release-test
release-test: test-all benchmark

# Docker targets (if needed)
.PHONY: docker-build
docker-build:
	docker build -t tidb-data-generator .

.PHONY: docker-run
docker-run:
	docker run --rm tidb-data-generator -h

# Database setup for integration tests
.PHONY: setup-test-db
setup-test-db:
	@echo "Setting up test database..."
	@echo "Make sure TiDB is running on localhost:4000"
	@echo "This will create test tables for integration tests"

.PHONY: cleanup-test-db
cleanup-test-db:
	@echo "Cleaning up test database..."
	@echo "This will drop test tables created by integration tests" 