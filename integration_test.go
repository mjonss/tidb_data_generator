package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

const (
	testHost     = "127.0.0.1"
	testPort     = 4000
	testUser     = "root"
	testPassword = ""
	testDatabase = "test"
	testTable    = "integration_test_table"
)

// TestTableSQL creates a comprehensive test table with all supported data types and indexes
const TestTableSQL = `
CREATE TABLE IF NOT EXISTS ` + testTable + ` (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tiny_int_col TINYINT NOT NULL,
    small_int_col SMALLINT NOT NULL,
    medium_int_col MEDIUMINT NOT NULL,
    int_col INT NOT NULL,
    big_int_col BIGINT NOT NULL,
    char_col CHAR(10) NOT NULL,
    varchar_col VARCHAR(255) NOT NULL,
    text_col TEXT NOT NULL,
    date_col DATE NOT NULL,
    time_col TIME NOT NULL,
    datetime_col DATETIME NOT NULL,
    timestamp_col TIMESTAMP NULL DEFAULT NULL,
    year_col YEAR NOT NULL,
    float_col FLOAT NOT NULL,
    double_col DOUBLE NOT NULL,
    decimal_col DECIMAL(10,2) NOT NULL,
    bool_col BOOLEAN NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_tiny_int (tiny_int_col),
    INDEX idx_varchar (varchar_col),
    INDEX idx_datetime (datetime_col),
    UNIQUE INDEX idx_unique_int (int_col),
    UNIQUE INDEX idx_unique_varchar (varchar_col),
    INDEX idx_composite_1 (tiny_int_col, small_int_col),
    INDEX idx_composite_2 (varchar_col, datetime_col),
    UNIQUE INDEX idx_unique_composite_1 (medium_int_col, char_col),
    UNIQUE INDEX idx_unique_composite_2 (float_col, bool_col)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

// TestNullableTableSQL creates a test table with several nullable columns
const TestNullableTableSQL = `
CREATE TABLE IF NOT EXISTS integration_nullable_test_table (
    id BIGINT NOT NULL AUTO_INCREMENT,
    nullable_int_col INT DEFAULT NULL,
    nullable_varchar_col VARCHAR(255) DEFAULT NULL,
    nullable_time_col TIME DEFAULT NULL,
    nullable_bool_col BOOLEAN DEFAULT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

// TestStatsJSON represents a sample statistics file for the test table
const TestStatsJSON = `{
    "count": 1000,
    "columns": {
        "id": {
            "null_count": 0,
            "histogram": {
                "ndv": 1000,
                "buckets": [
                    {"count": 100, "lower_bound": "MQ==", "upper_bound": "MTAw", "repeats": 0, "ndv": 100},
                    {"count": 100, "lower_bound": "MTAx", "upper_bound": "MjAw", "repeats": 0, "ndv": 100}
                ]
            }
        },
        "varchar_col": {
            "null_count": 50,
            "cm_sketch": {
                "top_n": [
                    {"data": "dGVzdF92YWx1ZV8x", "count": 100},
                    {"data": "dGVzdF92YWx1ZV8y", "count": 80},
                    {"data": "dGVzdF92YWx1ZV8z", "count": 60}
                ],
                "default_value": 10
            }
        },
        "datetime_col": {
            "null_count": 20,
            "histogram": {
                "ndv": 500,
                "buckets": [
                    {"count": 50, "lower_bound": "MjAyMy0wMS0wMSAwMDowMDowMA==", "upper_bound": "MjAyMy0wNi0zMCAyMzowMDowMA==", "repeats": 0, "ndv": 50},
                    {"count": 50, "lower_bound": "MjAyMy0wNy0wMSAwMDowMDowMA==", "upper_bound": "MjAyMy0xMi0zMSAyMzowMDowMA==", "repeats": 0, "ndv": 50}
                ]
            }
        }
    }
}`

func setupTestDatabase(t *testing.T) *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		testUser, testPassword, testHost, testPort, testDatabase)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Create test table
	_, err = db.Exec(TestTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	return db
}

func cleanupTestDatabase(t *testing.T, db *sql.DB) {
	// Drop test table
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
	if err != nil {
		t.Logf("Warning: Failed to drop test table: %v", err)
	}
	db.Close()
}

func TestIntegrationBasicGeneration(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	// Create stats file
	statsFile := "test_stats.json"
	err := os.WriteFile(statsFile, []byte(TestStatsJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create stats file: %v", err)
	}
	defer os.Remove(statsFile)

	// Test data generation from SQL file
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, statsFile)
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Generate data
	numRows := uint(100)
	rows := generator.GenerateData(numRows)

	// Verify generated data
	if len(rows) != int(numRows) {
		t.Errorf("Expected %d rows, got %d", numRows, len(rows))
	}

	// Verify each row has all required columns
	for i, row := range rows {
		if row["id"] == nil {
			t.Errorf("Row %d missing id column", i)
		}
		if row["varchar_col"] == nil && row["varchar_col"] != nil {
			t.Errorf("Row %d has unexpected varchar_col value", i)
		}
	}

	t.Logf("Successfully generated %d rows of test data", len(rows))
}

func TestIntegrationDatabaseMode(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Test table count
	count, err := getTableCount(config, testTable)
	if err != nil {
		t.Fatalf("Failed to get table count: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected empty table, got %d rows", count)
	}

	// Test effective row calculation
	effectiveRows := getEffectiveNumRows(generator, 50, 0, count)
	if effectiveRows != 50 {
		t.Errorf("Expected 50 rows, got %d", effectiveRows)
	}

	t.Logf("Database mode test completed successfully")
}

func TestIntegrationInsertData(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Get next available ID
	nextID, err := getNextAvailableID(config, tableDef)
	if err != nil {
		t.Fatalf("Failed to get next available ID: %v", err)
	}
	if nextID != 1 {
		t.Errorf("Expected next ID to be 1, got %d", nextID)
	}

	// Test small insert
	numRows := uint(10)
	batchSize := uint(5)
	numWorkers := uint(1)

	options := InsertOptions{
		BenchmarkMode: false,
		Verbose:       true,
		MaxRows:       numRows,
	}

	rate, inserted, err := generator.InsertData(config, testTable, numRows, batchSize, numWorkers, nextID, nil, options)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	if inserted != numRows {
		t.Errorf("Expected %d rows inserted, got %d", numRows, inserted)
	}

	if rate <= 0 {
		t.Errorf("Expected positive insert rate, got %f", rate)
	}

	t.Logf("Successfully inserted %d rows at %.0f rows/sec", inserted, rate)

	// Verify data was actually inserted
	var count int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", testTable)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count inserted rows: %v", err)
	}

	if count != int(numRows) {
		t.Errorf("Expected %d rows in database, got %d", numRows, count)
	}
}

func TestIntegrationAutoTuning(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Test batch size auto-tuning
	numRows := uint(100)
	batchSize, remainingRows := generator.findOptimalBatchSize(config, testTable, numRows)
	if batchSize == 0 {
		t.Errorf("Expected non-zero batch size")
	}
	if remainingRows >= numRows {
		t.Errorf("Expected some rows to be consumed during tuning, got %d remaining", remainingRows)
	}

	t.Logf("Optimal batch size: %d, remaining rows: %d", batchSize, remainingRows)

	// Test worker count auto-tuning
	workers, finalRemaining := generator.findOptimalWorkerCount(config, testTable, batchSize, remainingRows)
	if workers == 0 {
		t.Errorf("Expected non-zero worker count")
	}

	t.Logf("Optimal worker count: %d, final remaining rows: %d", workers, finalRemaining)
}

func TestIntegrationCompositeKeys(t *testing.T) {
	// Create a table with composite primary key
	compositeTableSQL := `
	CREATE TABLE IF NOT EXISTS composite_test_table (
		col1 INT NOT NULL,
		col2 VARCHAR(50) NOT NULL,
		col3 DATETIME NOT NULL,
		data_col TEXT,
		PRIMARY KEY (col1, col2, col3)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`

	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	// Create composite key table
	_, err := db.Exec(compositeTableSQL)
	if err != nil {
		t.Fatalf("Failed to create composite key table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS composite_test_table")

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, "composite_test_table")
	if err != nil {
		t.Fatalf("Failed to parse composite table from database: %v", err)
	}

	// Verify composite key detection
	if len(tableDef.PrimaryKey) != 3 {
		t.Errorf("Expected 3 primary key columns, got %d", len(tableDef.PrimaryKey))
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Get next available composite key
	nextCompositeKeys, err := getNextAvailableCompositeKey(config, tableDef)
	if err != nil {
		t.Fatalf("Failed to get next available composite key: %v", err)
	}

	if len(nextCompositeKeys) != 3 {
		t.Errorf("Expected 3 composite key values, got %d", len(nextCompositeKeys))
	}

	// Test data generation with composite keys
	numRows := uint(10)
	rows := generator.GenerateData(numRows)

	if len(rows) != int(numRows) {
		t.Errorf("Expected %d rows, got %d", numRows, len(rows))
	}

	// Verify composite key uniqueness
	keySet := make(map[string]bool)
	for i, row := range rows {
		key := fmt.Sprintf("%v|%v|%v", row["col1"], row["col2"], row["col3"])
		if keySet[key] {
			t.Errorf("Duplicate composite key found in row %d: %s", i, key)
		}
		keySet[key] = true
	}

	t.Logf("Successfully generated %d rows with unique composite keys", len(rows))
}

func TestIntegrationDataTypes(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	// Create data generator
	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Generate a single row to test all data types
	row := generator.GenerateRow(1)

	// Test integer types
	if row["tiny_int_col"] == nil {
		t.Error("tiny_int_col should not be nil")
	}
	if row["small_int_col"] == nil {
		t.Error("small_int_col should not be nil")
	}
	if row["medium_int_col"] == nil {
		t.Error("medium_int_col should not be nil")
	}
	if row["int_col"] == nil {
		t.Error("int_col should not be nil")
	}
	if row["big_int_col"] == nil {
		t.Error("big_int_col should not be nil")
	}

	// Test string types
	if row["char_col"] == nil {
		t.Error("char_col should not be nil")
	}
	if row["varchar_col"] == nil {
		t.Error("varchar_col should not be nil")
	}
	if row["text_col"] == nil {
		t.Error("text_col should not be nil")
	}

	// Test date/time types
	if row["date_col"] == nil {
		t.Error("date_col should not be nil")
	}
	if row["time_col"] == nil {
		t.Errorf("time_col should not be nil, got: %v", row["time_col"])
	}
	if row["datetime_col"] == nil {
		t.Error("datetime_col should not be nil")
	}
	if row["timestamp_col"] == nil {
		t.Log("timestamp_col is null (nullable column)")
	}
	if row["year_col"] == nil {
		t.Error("year_col should not be nil")
	}

	// Test numeric types
	if row["float_col"] == nil {
		t.Error("float_col should not be nil")
	}
	if row["double_col"] == nil {
		t.Error("double_col should not be nil")
	}
	if row["decimal_col"] == nil {
		t.Error("decimal_col should not be nil")
	}

	// Test boolean type
	if row["bool_col"] == nil {
		t.Error("bool_col should not be nil")
	}

	t.Logf("All data types generated successfully")
}

func TestIntegrationErrorHandling(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	// Test with invalid database connection
	invalidConfig := DBConfig{
		Host:     "invalid-host",
		Port:     3306,
		User:     "root",
		Password: "",
		Database: "test",
	}

	// This should fail gracefully
	_, err := getTableCount(invalidConfig, testTable)
	if err == nil {
		t.Error("Expected error with invalid database connection")
	}

	// Test with non-existent table
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	_, err = getTableCount(config, "non_existent_table")
	if err == nil {
		t.Error("Expected error with non-existent table")
	}

	t.Logf("Error handling tests completed successfully")
}

func TestIntegrationNullGeneration(t *testing.T) {
	db := setupTestDatabase(t)
	defer cleanupTestDatabase(t, db)

	// Create the nullable test table
	_, err := db.Exec(TestNullableTableSQL)
	if err != nil {
		t.Fatalf("Failed to create nullable test table: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS integration_nullable_test_table")

	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, "integration_nullable_test_table")
	if err != nil {
		t.Fatalf("Failed to parse nullable test table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	numRows := uint(1500)
	// rows := generator.GenerateData(numRows) // No need to keep this variable

	// Insert the generated data
	nextID := 1
	options := InsertOptions{
		BenchmarkMode: false,
		Verbose:       false,
		MaxRows:       numRows,
	}
	_, inserted, err := generator.InsertData(config, "integration_nullable_test_table", numRows, 100, 1, nextID, nil, options)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	if inserted != numRows {
		t.Errorf("Expected %d rows inserted, got %d", numRows, inserted)
	}

	// Query the table and count NULLs in each nullable column
	type nullCounts struct {
		intNulls     int
		varcharNulls int
		timeNulls    int
		boolNulls    int
	}
	var counts nullCounts
	rowQuery := `SELECT COUNT(*) FROM integration_nullable_test_table WHERE nullable_int_col IS NULL`
	db.QueryRow(rowQuery).Scan(&counts.intNulls)
	rowQuery = `SELECT COUNT(*) FROM integration_nullable_test_table WHERE nullable_varchar_col IS NULL`
	db.QueryRow(rowQuery).Scan(&counts.varcharNulls)
	rowQuery = `SELECT COUNT(*) FROM integration_nullable_test_table WHERE nullable_time_col IS NULL`
	db.QueryRow(rowQuery).Scan(&counts.timeNulls)
	rowQuery = `SELECT COUNT(*) FROM integration_nullable_test_table WHERE nullable_bool_col IS NULL`
	db.QueryRow(rowQuery).Scan(&counts.boolNulls)

	// Assert that at least some rows have NULLs in each nullable column
	if counts.intNulls == 0 {
		t.Error("Expected some NULLs in nullable_int_col, got 0")
	}
	if counts.varcharNulls == 0 {
		t.Error("Expected some NULLs in nullable_varchar_col, got 0")
	}
	if counts.timeNulls == 0 {
		t.Error("Expected some NULLs in nullable_time_col, got 0")
	}
	if counts.boolNulls == 0 {
		t.Error("Expected some NULLs in nullable_bool_col, got 0")
	}

	t.Logf("NULL counts: int=%d, varchar=%d, time=%d, bool=%d", counts.intNulls, counts.varcharNulls, counts.timeNulls, counts.boolNulls)
}

func TestMain(m *testing.M) {
	// Check if TiDB is running
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		testUser, testPassword, testHost, testPort, testDatabase)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Warning: Could not connect to TiDB at %s:%d. Make sure TiDB is running with 'tiup playground'", testHost, testPort)
		log.Printf("Skipping integration tests")
		os.Exit(0)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Printf("Warning: Could not ping TiDB at %s:%d. Make sure TiDB is running with 'tiup playground'", testHost, testPort)
		log.Printf("Skipping integration tests")
		os.Exit(0)
	}

	log.Printf("TiDB connection successful, running integration tests")
	os.Exit(m.Run())
}
