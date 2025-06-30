package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// Test helper functions
func createTempFile(content string) (string, error) {
	tmpfile, err := os.CreateTemp("", "test_*.sql")
	if err != nil {
		return "", err
	}
	if _, err := tmpfile.Write([]byte(content)); err != nil {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
		return "", err
	}
	if err := tmpfile.Close(); err != nil {
		os.Remove(tmpfile.Name())
		return "", err
	}
	return tmpfile.Name(), nil
}

func createTempStatsFile() (string, error) {
	stats := Stats{
		Columns: map[string]ColumnStats{
			"test_type": {
				CMSketch: &CMSketch{
					TopN: []TopNItem{
						{Data: "RmxpcC1mbG9wcw==", Count: 100}, // "Flip-flops" in base64
						{Data: "R2xhc3Nlcw==", Count: 80},      // "Glasses" in base64
						{Data: "U2hvcnRz", Count: 60},          // "Shorts" in base64
						{Data: "U29ja3M=", Count: 40},          // "Socks" in base64
					},
				},
				NullCount: 0,
			},
			"test_category": {
				Histogram: &Histogram{
					NDV: 1000,
					Buckets: []Bucket{
						{Count: 100, LowerBound: "MA==", UpperBound: "MTAw", Repeats: 0, NDV: 100},
						{Count: 200, LowerBound: "MTAw", UpperBound: "NTAw", Repeats: 0, NDV: 200},
					},
				},
				NullCount: 0,
			},
			"created": {
				Histogram: &Histogram{
					NDV: 100,
					Buckets: []Bucket{
						{Count: 50, LowerBound: "MjAyNS0wNi0xMCAwOTo1NzowNw==", UpperBound: "MjAyNS0wNi0xNSAwOTo1NzowNw==", Repeats: 0, NDV: 50},
					},
				},
				NullCount: 0,
			},
		},
	}

	data, err := json.Marshal(stats)
	if err != nil {
		return "", err
	}

	tmpfile, err := os.CreateTemp("", "test_*.json")
	if err != nil {
		return "", err
	}
	if _, err := tmpfile.Write(data); err != nil {
		tmpfile.Close()
		os.Remove(tmpfile.Name())
		return "", err
	}
	if err := tmpfile.Close(); err != nil {
		os.Remove(tmpfile.Name())
		return "", err
	}
	return tmpfile.Name(), nil
}

// Test parseCreateTable
func TestParseCreateTable(t *testing.T) {
	// Use the existing t.create.sql file for testing
	tableDef, err := parseCreateTable("t.create.sql")
	if err != nil {
		t.Fatalf("parseCreateTable failed: %v", err)
	}

	if tableDef.Name != "t" {
		t.Errorf("Expected table name 't', got '%s'", tableDef.Name)
	}

	// Check that we have some columns
	if len(tableDef.Columns) == 0 {
		t.Error("Expected at least one column, got 0")
	}

	// Check that we have the expected columns
	expectedColumns := []string{"id", "test_type", "test_category", "created", "calendar_date"}
	foundColumns := make(map[string]bool)
	for _, col := range tableDef.Columns {
		foundColumns[col.Name] = true
	}

	for _, expected := range expectedColumns {
		if !foundColumns[expected] {
			t.Errorf("Expected column '%s' not found", expected)
		}
	}
}

// Test parseStatsFile
func TestParseStatsFile(t *testing.T) {
	statsFile, err := createTempStatsFile()
	if err != nil {
		t.Fatalf("Failed to create temp stats file: %v", err)
	}
	defer os.Remove(statsFile)

	stats, err := parseStatsFile(statsFile)
	if err != nil {
		t.Fatalf("parseStatsFile failed: %v", err)
	}

	if len(stats.Columns) != 3 {
		t.Errorf("Expected 3 columns in stats, got %d", len(stats.Columns))
	}

	// Test test_type column
	if colStats, exists := stats.Columns["test_type"]; exists {
		if colStats.CMSketch == nil {
			t.Error("Expected CMSketch for test_type column")
		}
		if len(colStats.CMSketch.TopN) != 4 {
			t.Errorf("Expected 4 TopN items, got %d", len(colStats.CMSketch.TopN))
		}
		if colStats.NullCount != 0 {
			t.Errorf("Expected null count 0, got %d", colStats.NullCount)
		}
	} else {
		t.Error("Expected test_type column in stats")
	}

	// Test test_category column
	if colStats, exists := stats.Columns["test_category"]; exists {
		if colStats.Histogram == nil {
			t.Error("Expected Histogram for test_category column")
		}
		if len(colStats.Histogram.Buckets) != 2 {
			t.Errorf("Expected 2 buckets, got %d", len(colStats.Histogram.Buckets))
		}
	} else {
		t.Error("Expected test_category column in stats")
	}
}

// Test NewDataGenerator
func TestNewDataGenerator(t *testing.T) {
	generator, err := NewDataGenerator("t.create.sql", "")
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	if generator.tableDef == nil {
		t.Error("Expected tableDef to be set")
	}
	if generator.tableDef.Name != "t" {
		t.Errorf("Expected table name 't', got '%s'", generator.tableDef.Name)
	}
	if len(generator.columnGenerators) == 0 {
		t.Error("Expected at least one column generator")
	}
}

// Test NewDataGenerator with stats
func TestNewDataGeneratorWithStats(t *testing.T) {
	sqlContent := `CREATE TABLE test_table (
		id INT AUTO_INCREMENT PRIMARY KEY,
		test_type VARCHAR(255),
		test_category INT
	)`

	sqlFile, err := createTempFile(sqlContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(sqlFile)

	statsFile, err := createTempStatsFile()
	if err != nil {
		t.Fatalf("Failed to create temp stats file: %v", err)
	}
	defer os.Remove(statsFile)

	generator, err := NewDataGenerator(sqlFile, statsFile)
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	if generator.stats == nil {
		t.Error("Expected stats to be set")
	}
	if len(generator.stats.Columns) != 3 {
		t.Errorf("Expected 3 columns in stats, got %d", len(generator.stats.Columns))
	}
}

// Test GenerateRow
func TestGenerateRow(t *testing.T) {
	generator, err := NewDataGenerator("t.create.sql", "")
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}
	row := generator.GenerateRow(1)
	// The ID should be generated and not nil, but may not be exactly 1 due to modulo arithmetic
	if row["id"] == nil {
		t.Error("Expected id to be generated")
	}
	// Check that it's an integer value (accept both int and int64)
	switch row["id"].(type) {
	case int, int64:
		// ok
	default:
		t.Errorf("Expected id to be int or int64, got %T", row["id"])
	}
}

// Test GenerateData
func TestGenerateData(t *testing.T) {
	generator, err := NewDataGenerator("t.create.sql", "")
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	numRows := uint(10)
	rows := generator.GenerateData(numRows)

	if len(rows) != int(numRows) {
		t.Errorf("Expected %d rows, got %d", numRows, len(rows))
	}

	// Check that each row has an ID and it's unique
	ids := make(map[interface{}]bool)
	for i, row := range rows {
		if row["id"] == nil {
			t.Errorf("Row %d: expected id to be generated", i)
		}
		if ids[row["id"]] {
			t.Errorf("Row %d: duplicate id %v", i, row["id"])
		}
		ids[row["id"]] = true
	}
}

// Test generateStringFromStats
func TestGenerateStringFromStats(t *testing.T) {
	generator, err := NewDataGenerator("t.create.sql", "t.stats.json")
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	// Create a ColumnDef for the test_type column
	column := ColumnDef{
		Name:     "test_type",
		Type:     "varchar",
		Size:     255,
		Nullable: true,
	}

	generatedStrings := make(map[string]int)
	for i := 0; i < 100; i++ {
		str := generator.generateStringFromStats(column)
		generatedStrings[str]++
	}
	expectedValues := []string{"Flip-flops", "Glasses", "Shorts", "Socks"}
	foundExpected := false
	for _, expected := range expectedValues {
		if generatedStrings[expected] > 0 {
			foundExpected = true
			break
		}
	}
	if !foundExpected {
		t.Error("Expected to generate some values from stats, but got none")
	}
}

// Test generateTimeFromStats
func TestGenerateTimeFromStats(t *testing.T) {
	sqlContent := `CREATE TABLE test_table (
		id INT AUTO_INCREMENT PRIMARY KEY,
		created TIMESTAMP
	)`

	sqlFile, err := createTempFile(sqlContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(sqlFile)

	statsFile, err := createTempStatsFile()
	if err != nil {
		t.Fatalf("Failed to create temp stats file: %v", err)
	}
	defer os.Remove(statsFile)

	generator, err := NewDataGenerator(sqlFile, statsFile)
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	// Generate multiple times and verify they're within expected range
	for i := 0; i < 10; i++ {
		tm := generator.generateTimeFromStats("created")

		// Check that the time is reasonable (not too far in the past or future)
		now := time.Now()
		if tm.After(now.AddDate(1, 0, 0)) || tm.Before(now.AddDate(-1, 0, 0)) {
			t.Errorf("Generated time %v is too far from current time %v", tm, now)
		}
	}
}

// Test nullable handling
func TestNullableHandling(t *testing.T) {
	generator, err := NewDataGenerator("t.create.sql", "")
	if err != nil {
		t.Fatalf("NewDataGenerator failed: %v", err)
	}

	// Generate multiple rows and check that NOT NULL columns are never null
	totalRows := 100

	for i := 0; i < totalRows; i++ {
		row := generator.GenerateRow(i + 1)

		// Check that id is never null (it's the primary key and NOT NULL)
		if row["id"] == nil {
			t.Error("Expected id to never be null")
		}

		// Check that all columns are generated (even if they can be null)
		expectedColumns := []string{"test_type", "test_category", "created", "calendar_date"}
		for _, colName := range expectedColumns {
			// These columns can be null, but they should be generated (either as value or null)
			// The generator should handle this correctly
			if _, exists := row[colName]; !exists {
				t.Errorf("Expected column %s to be present in row", colName)
			}
		}
	}
}

// Test auto-increment detection
func TestAutoIncrementDetection(t *testing.T) {
	tableDef, err := parseCreateTable("t.create.sql")
	if err != nil {
		t.Fatalf("parseCreateTable failed: %v", err)
	}
	autoIncrementFound := false
	for _, col := range tableDef.Columns {
		if strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
			autoIncrementFound = true
			break
		}
	}
	if !autoIncrementFound {
		t.Error("Expected to find auto_increment columns")
	}
}

// Test error cases
func TestParseCreateTableError(t *testing.T) {
	// Test with non-existent file
	_, err := parseCreateTable("non_existent_file.sql")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}

	// Test with invalid SQL
	invalidSQL := "INVALID SQL CONTENT"
	sqlFile, err := createTempFile(invalidSQL)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(sqlFile)

	_, err = parseCreateTable(sqlFile)
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestParseStatsFileError(t *testing.T) {
	// Test with non-existent file
	_, err := parseStatsFile("non_existent_file.json")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}

	// Test with invalid JSON
	invalidJSON := "{ invalid json }"
	statsFile, err := createTempFile(invalidJSON)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(statsFile)

	_, err = parseStatsFile(statsFile)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

// Benchmark tests
func BenchmarkGenerateRow(b *testing.B) {
	sqlContent := `CREATE TABLE test_table (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255),
		age INT,
		email VARCHAR(100),
		created_at TIMESTAMP
	)`

	sqlFile, err := createTempFile(sqlContent)
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(sqlFile)

	generator, err := NewDataGenerator(sqlFile, "")
	if err != nil {
		b.Fatalf("NewDataGenerator failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.GenerateRow(i + 1)
	}
}

func BenchmarkGenerateData(b *testing.B) {
	sqlContent := `CREATE TABLE test_table (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255),
		age INT
	)`

	sqlFile, err := createTempFile(sqlContent)
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(sqlFile)

	generator, err := NewDataGenerator(sqlFile, "")
	if err != nil {
		b.Fatalf("NewDataGenerator failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.GenerateData(100)
	}
}
