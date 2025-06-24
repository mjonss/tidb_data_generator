package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// Integration test configuration
const (
	testHost     = "localhost"
	testPort     = 4000
	testUser     = "root"
	testPassword = ""
	testDatabase = "test"
	testTable    = "integration_test"
)

// Test database connection
func TestDatabaseConnection(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("Skipping integration test - cannot connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping integration test - cannot ping database: %v", err)
	}
}

// Test table creation and parsing from database
func TestParseTableFromDB(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	// Create test table
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("Skipping integration test - cannot connect to database: %v", err)
	}
	defer db.Close()

	// Create test table
	createTableSQL := `CREATE TABLE IF NOT EXISTS integration_test (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(100),
		age INT,
		salary DECIMAL(10,2),
		is_active BOOLEAN DEFAULT TRUE,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Parse table from database
	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Fatalf("Failed to parse table from database: %v", err)
	}

	if tableDef.Name != testTable {
		t.Errorf("Expected table name '%s', got '%s'", testTable, tableDef.Name)
	}

	expectedColumns := []struct {
		name     string
		dataType string
		nullable bool
	}{
		{"id", "int", false},
		{"name", "varchar", false},
		{"email", "varchar", true},
		{"age", "int", true},
		{"salary", "decimal", true},
		{"is_active", "boolean", true},
		{"created_at", "timestamp", true},
		{"updated_at", "timestamp", true},
	}

	if len(tableDef.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(tableDef.Columns))
	}

	for i, expected := range expectedColumns {
		if i >= len(tableDef.Columns) {
			break
		}
		col := tableDef.Columns[i]
		if col.Name != expected.name {
			t.Errorf("Column %d: expected name '%s', got '%s'", i, expected.name, col.Name)
		}
		// Accept tinyint as boolean
		if expected.dataType == "boolean" {
			if !strings.Contains(col.Type, "tinyint") && !strings.Contains(col.Type, "boolean") {
				t.Errorf("Column %d: expected type containing 'tinyint' or 'boolean', got '%s'", i, col.Type)
			}
		} else {
			if !strings.Contains(col.Type, expected.dataType) {
				t.Errorf("Column %d: expected type containing '%s', got '%s'", i, expected.dataType, col.Type)
			}
		}
		if col.Nullable != expected.nullable {
			t.Errorf("Column %d: expected nullable %v, got %v", i, expected.nullable, col.Nullable)
		}
	}
}

// Test data generation from database schema
func TestDataGenerationFromDB(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Generate test data
	numRows := 10
	rows := generator.GenerateData(numRows)

	if len(rows) != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, len(rows))
	}

	// Verify data types
	for i, row := range rows {
		if row["id"] != i+1 {
			t.Errorf("Row %d: expected id %d, got %v", i, i+1, row["id"])
		}

		// Check that name is a non-empty string
		if name, ok := row["name"].(string); ok {
			if len(name) == 0 {
				t.Errorf("Row %d: expected non-empty name", i)
			}
		} else {
			t.Errorf("Row %d: expected name to be a string", i)
		}

		// Check that age is an integer
		if age, ok := row["age"].(int); ok {
			if age < 0 || age > 10000 {
				t.Errorf("Row %d: expected reasonable age, got %d", i, age)
			}
		} else if row["age"] != nil {
			t.Errorf("Row %d: expected age to be an integer or nil", i)
		}

		// Check that salary is a float
		if salary, ok := row["salary"].(float64); ok {
			if salary < 0 || salary > 1000 {
				t.Errorf("Row %d: expected reasonable salary, got %f", i, salary)
			}
		} else if row["salary"] != nil {
			t.Errorf("Row %d: expected salary to be a float or nil", i)
		}

		// Check that is_active is a boolean
		if isActive, ok := row["is_active"].(bool); ok {
			// Boolean value is fine
			_ = isActive
		} else if row["is_active"] != nil {
			t.Errorf("Row %d: expected is_active to be a boolean or nil", i)
		}
	}
}

// Test database insertion (small scale)
func TestDatabaseInsertion(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Test standard insertion
	numRows := 5
	err = generator.InsertDataToDB(config, testTable, numRows)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + testTable).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

// Test bulk insertion
func TestBulkInsertion(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Test bulk insertion
	numRows := 10
	err = generator.InsertDataToDBBulk(config, testTable, numRows)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + testTable).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

// Test parallel insertion (small scale)
func TestParallelInsertion(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Test parallel insertion
	numRows := 20
	numWorkers := 2
	err = generator.InsertDataToDBParallel(config, testTable, numRows, numWorkers)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + testTable).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

// Test parallel bulk insertion
func TestParallelBulkInsertion(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Test parallel bulk insertion
	numRows := 30
	numWorkers := 2
	err = generator.InsertDataToDBBulkParallel(config, testTable, numRows, numWorkers)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM " + testTable).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

// Test data quality and constraints
func TestDataQuality(t *testing.T) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		t.Skipf("Skipping test - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		t.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	// Insert test data
	numRows := 50
	err = generator.InsertDataToDBBulk(config, testTable, numRows)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify data quality
	rows, err := db.Query("SELECT id, name, email, age, salary, is_active FROM " + testTable + " ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	prevID := -1
	for rows.Next() {
		var id int
		var name, email sql.NullString
		var age sql.NullInt64
		var salary sql.NullFloat64
		var isActive sql.NullBool

		err := rows.Scan(&id, &name, &email, &age, &salary, &isActive)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Verify IDs are sequential (difference of 1)
		if prevID != -1 && id != prevID+1 {
			t.Errorf("Expected sequential IDs, got %d after %d", id, prevID)
		}
		prevID = id

		// Verify name is not null (NOT NULL constraint)
		if !name.Valid || len(name.String) == 0 {
			t.Errorf("Row %d: name should not be null or empty", id)
		}

		// Verify age is reasonable if not null
		if age.Valid {
			if age.Int64 < 0 || age.Int64 > 10000 {
				t.Errorf("Row %d: age %d is not reasonable", id, age.Int64)
			}
		}

		// Verify salary is reasonable if not null
		if salary.Valid {
			if salary.Float64 < 0 || salary.Float64 > 1000 {
				t.Errorf("Row %d: salary %f is not reasonable", id, salary.Float64)
			}
		}

		// Verify is_active is a boolean
		if isActive.Valid {
			_ = isActive.Bool // Accept both true and false
		}
	}

	if prevID+1 < 50 {
		t.Errorf("Expected at least 50 rows, got %d", prevID+1)
	}
}

// Test cleanup
func TestCleanup(t *testing.T) {
	// This test runs last to clean up the test table
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("Skipping cleanup - cannot connect to database: %v", err)
	}
	defer db.Close()

	// Drop test table
	_, err = db.Exec("DROP TABLE IF EXISTS " + testTable)
	if err != nil {
		t.Logf("Warning: failed to drop test table: %v", err)
	}
}

// Benchmark database operations
func BenchmarkDatabaseInsertion(b *testing.B) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		b.Skipf("Skipping benchmark - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		b.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		b.Fatalf("Failed to clear table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = generator.InsertDataToDBBulk(config, testTable, 100)
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}
}

func BenchmarkParallelBulkInsertion(b *testing.B) {
	config := DBConfig{
		Host:     testHost,
		Port:     testPort,
		User:     testUser,
		Password: testPassword,
		Database: testDatabase,
	}

	tableDef, err := parseTableFromDB(config, testTable)
	if err != nil {
		b.Skipf("Skipping benchmark - cannot parse table: %v", err)
	}

	generator, err := NewDataGeneratorFromTableDef(tableDef, "")
	if err != nil {
		b.Fatalf("Failed to create data generator: %v", err)
	}

	// Clear existing data
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM " + testTable)
	if err != nil {
		b.Fatalf("Failed to clear table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = generator.InsertDataToDBBulkParallel(config, testTable, 100, 4)
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}
}
