#!/bin/bash

# Integration Test Runner for TiDB Data Generator
# This script runs integration tests against a TiDB instance

set -e

echo "=== TiDB Data Generator Integration Tests ==="
echo

# Check if TiDB is running
echo "Checking TiDB connection..."
if ! mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT 1" > /dev/null 2>&1; then
    echo "❌ TiDB is not running on 127.0.0.1:4000"
    echo "Please start TiDB with: tiup playground"
    echo
    echo "Example:"
    echo "  tiup playground --db 1 --kv 1 --pd 1"
    exit 1
fi

echo "✅ TiDB is running"
echo

# Build the application
echo "Building application..."
go build -o tidb_data_generator main.go
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"
echo

# Run integration tests
echo "Running integration tests..."
go test -v -tags=integration ./integration_test.go main.go
TEST_EXIT_CODE=$?

echo
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ All integration tests passed!"
else
    echo "❌ Some integration tests failed"
fi

echo
echo "=== Test Summary ==="
echo "Integration tests completed with exit code: $TEST_EXIT_CODE"

exit $TEST_EXIT_CODE 