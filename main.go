package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Column definition structure
type ColumnDef struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
}

// Table definition structure
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

// Statistics structures
type Stats struct {
	Columns map[string]ColumnStats `json:"columns"`
}

type ColumnStats struct {
	Histogram *Histogram `json:"histogram"`
	CMSketch  *CMSketch  `json:"cm_sketch"`
	NullCount int        `json:"null_count"`
}

type Histogram struct {
	NDV     int      `json:"ndv"`
	Buckets []Bucket `json:"buckets"`
}

type Bucket struct {
	Count      int    `json:"count"`
	LowerBound string `json:"lower_bound"`
	UpperBound string `json:"upper_bound"`
	Repeats    int    `json:"repeats"`
	NDV        int    `json:"ndv"`
}

type CMSketch struct {
	TopN         []TopNItem `json:"top_n"`
	DefaultValue int        `json:"default_value"`
}

type TopNItem struct {
	Data  string `json:"data"`
	Count int    `json:"count"`
}

// Generic table row structure
type TableRow map[string]interface{}

// Data generator
type DataGenerator struct {
	tableDef         *TableDef
	stats            *Stats
	rand             *rand.Rand
	columnGenerators map[string]func() interface{}
}

// Database connection structure
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewDataGenerator(sqlFile, statsFile string) (*DataGenerator, error) {
	// Parse SQL file
	tableDef, err := parseCreateTable(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL file: %w", err)
	}

	// Parse stats file (optional)
	var stats *Stats
	if statsFile != "" {
		stats, err = parseStatsFile(statsFile)
		if err != nil {
			log.Printf("Warning: failed to parse stats file: %v", err)
		}
	}

	generator := &DataGenerator{
		tableDef:         tableDef,
		stats:            stats,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
		columnGenerators: make(map[string]func() interface{}),
	}

	// Initialize column generators
	generator.initializeColumnGenerators()

	return generator, nil
}

func parseCreateTable(sqlFile string) (*TableDef, error) {
	data, err := os.ReadFile(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	content := string(data)

	// Extract table name
	tableNameRegex := regexp.MustCompile(`CREATE TABLE \x60?(\w+)\x60?\s*\(`)
	matches := tableNameRegex.FindStringSubmatch(content)
	if len(matches) < 2 {
		return nil, fmt.Errorf("could not extract table name from SQL")
	}
	tableName := matches[1]

	// Split content into lines and process each line
	lines := strings.Split(content, "\n")
	var columns []ColumnDef

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "`") {
			continue
		}
		// Look for column definition pattern: `column_name` type [constraints]
		columnMatch := regexp.MustCompile(`\x60?(\w+)\x60?\s+([^,]+)`).FindStringSubmatch(line)
		if len(columnMatch) >= 3 {
			colName := columnMatch[1]
			colDef := strings.TrimSpace(columnMatch[2])
			// Remove trailing comma if present
			colDef = strings.TrimSuffix(colDef, ",")
			column := parseColumnDefinition(colName, colDef)
			columns = append(columns, column)
		}
	}

	return &TableDef{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func parseColumnDefinition(name, definition string) ColumnDef {
	// Extract data type
	typeRegex := regexp.MustCompile(`(\w+)(?:\([^)]*\))?`)
	typeMatch := typeRegex.FindStringSubmatch(definition)
	dataType := "varchar"
	if len(typeMatch) >= 2 {
		dataType = strings.ToLower(typeMatch[1])
	}

	// Check if nullable
	nullable := !strings.Contains(strings.ToUpper(definition), "NOT NULL")

	// Extract default value
	defaultRegex := regexp.MustCompile(`DEFAULT\s+([^,\s]+)`)
	defaultMatch := defaultRegex.FindStringSubmatch(definition)
	defaultValue := ""
	if len(defaultMatch) >= 2 {
		defaultValue = defaultMatch[1]
	}

	return ColumnDef{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Default:  defaultValue,
	}
}

func parseStatsFile(statsFile string) (*Stats, error) {
	data, err := os.ReadFile(statsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read stats file: %w", err)
	}

	var stats Stats
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to parse stats JSON: %w", err)
	}

	return &stats, nil
}

func (dg *DataGenerator) initializeColumnGenerators() {
	for _, column := range dg.tableDef.Columns {
		dg.columnGenerators[column.Name] = dg.createColumnGenerator(column)
	}
}

func (dg *DataGenerator) createColumnGenerator(column ColumnDef) func() interface{} {
	// Extract base type name (remove precision/scale info)
	baseType := column.Type
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = baseType[:idx]
	}

	switch baseType {
	case "int", "integer", "bigint":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			return dg.rand.Intn(10000)
		}
	case "varchar", "char", "text":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			// Try to use stats if available
			if dg.stats != nil {
				if _, exists := dg.stats.Columns[column.Name]; exists {
					return dg.generateStringFromStats(column.Name)
				}
			}
			// Fallback to random string
			return fmt.Sprintf("value_%d", dg.rand.Intn(1000))
		}
	case "timestamp", "datetime":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			// Try to use stats if available
			if dg.stats != nil {
				if _, exists := dg.stats.Columns[column.Name]; exists {
					return dg.generateTimeFromStats(column.Name)
				}
			}
			// Fallback to random time
			return time.Now().Add(time.Duration(dg.rand.Int63n(365*24*60*60)) * time.Second)
		}
	case "date":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			// Generate random date
			baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			randomDays := dg.rand.Intn(365 * 5) // 5 years range
			return baseTime.AddDate(0, 0, randomDays).Format("2006-01-02")
		}
	case "float", "double", "decimal":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			return dg.rand.Float64() * 1000
		}
	case "boolean", "bool":
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			return dg.rand.Float32() > 0.5
		}
	default:
		return func() interface{} {
			if column.Nullable && dg.rand.Float32() < 0.1 { // 10% null chance
				return nil
			}
			return fmt.Sprintf("unknown_type_%d", dg.rand.Intn(100))
		}
	}
}

func (dg *DataGenerator) generateStringFromStats(columnName string) string {
	columnStats := dg.stats.Columns[columnName]
	if columnStats.CMSketch != nil && len(columnStats.CMSketch.TopN) > 0 {
		// Use frequency distribution from CMSketch
		totalWeight := 0
		for _, item := range columnStats.CMSketch.TopN {
			totalWeight += item.Count
		}

		r := dg.rand.Intn(totalWeight)
		currentWeight := 0

		for _, item := range columnStats.CMSketch.TopN {
			currentWeight += item.Count
			if r < currentWeight {
				// Decode base64 value
				decoded, err := base64.StdEncoding.DecodeString(item.Data)
				if err == nil && len(decoded) > 1 {
					value := string(decoded[1:])
					// Remove all null bytes, whitespace, and ensure valid UTF-8
					value = strings.ReplaceAll(value, "\x00", "")
					value = strings.TrimSpace(value)
					value = strings.ToValidUTF8(value, "")
					return value
				}
				break
			}
		}
	}

	// Fallback
	return fmt.Sprintf("generated_%s_%d", columnName, dg.rand.Intn(1000))
}

func (dg *DataGenerator) generateTimeFromStats(columnName string) time.Time {
	columnStats := dg.stats.Columns[columnName]
	if columnStats.Histogram != nil && len(columnStats.Histogram.Buckets) > 0 {
		// Use histogram distribution
		totalCount := 0
		for _, bucket := range columnStats.Histogram.Buckets {
			totalCount += bucket.Count
		}

		r := dg.rand.Intn(totalCount)
		currentCount := 0

		for _, bucket := range columnStats.Histogram.Buckets {
			currentCount += bucket.Count
			if r < currentCount {
				// Decode and parse time bounds
				lowerBound, err := base64.StdEncoding.DecodeString(bucket.LowerBound)
				if err == nil {
					lowerTime, err := time.Parse("2006-01-02 15:04:05", string(lowerBound))
					if err == nil {
						upperBound, err := base64.StdEncoding.DecodeString(bucket.UpperBound)
						if err == nil {
							upperTime, err := time.Parse("2006-01-02 15:04:05", string(upperBound))
							if err == nil {
								duration := upperTime.Sub(lowerTime)
								randomDuration := time.Duration(dg.rand.Int63n(int64(duration)))
								return lowerTime.Add(randomDuration)
							}
						}
					}
				}
				break
			}
		}
	}

	// Fallback
	return time.Now().Add(time.Duration(dg.rand.Int63n(365*24*60*60)) * time.Second)
}

func (dg *DataGenerator) GenerateRow(id int) TableRow {
	row := make(TableRow)

	for _, column := range dg.tableDef.Columns {
		if column.Name == "id" {
			row[column.Name] = id
		} else {
			generator := dg.columnGenerators[column.Name]
			row[column.Name] = generator()
		}
	}

	return row
}

func (dg *DataGenerator) GenerateData(numRows int) []TableRow {
	rows := make([]TableRow, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = dg.GenerateRow(i + 1)
	}
	return rows
}

func parseTableFromDB(config DBConfig, tableName string) (*TableDef, error) {
	// Create DSN (Data Source Name)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Query table structure
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := db.Query(query, config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table structure: %w", err)
	}
	defer rows.Close()

	var columns []ColumnDef
	for rows.Next() {
		var (
			columnName   string
			dataType     string
			isNullable   string
			defaultValue sql.NullString
			charLength   sql.NullInt64
			numPrecision sql.NullInt64
			numScale     sql.NullInt64
		)

		if err := rows.Scan(&columnName, &dataType, &isNullable, &defaultValue,
			&charLength, &numPrecision, &numScale); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		// Build full data type string
		dataTypeLower := strings.ToLower(dataType)
		if charLength.Valid {
			dataTypeLower = fmt.Sprintf("%s(%d)", dataTypeLower, charLength.Int64)
		} else if numPrecision.Valid {
			if numScale.Valid {
				dataTypeLower = fmt.Sprintf("%s(%d,%d)", dataTypeLower, numPrecision.Int64, numScale.Int64)
			} else {
				dataTypeLower = fmt.Sprintf("%s(%d)", dataTypeLower, numPrecision.Int64)
			}
		}

		column := ColumnDef{
			Name:     columnName,
			Type:     dataTypeLower,
			Nullable: isNullable == "YES",
			Default:  defaultValue.String,
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	return &TableDef{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func NewDataGeneratorFromTableDef(tableDef *TableDef, statsFile string) (*DataGenerator, error) {
	// Parse stats file (optional)
	var stats *Stats
	if statsFile != "" {
		var err error
		stats, err = parseStatsFile(statsFile)
		if err != nil {
			log.Printf("Warning: failed to parse stats file: %v", err)
		}
	}

	generator := &DataGenerator{
		tableDef:         tableDef,
		stats:            stats,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
		columnGenerators: make(map[string]func() interface{}),
	}

	// Initialize column generators
	generator.initializeColumnGenerators()

	return generator, nil
}

func (dg *DataGenerator) InsertDataToDB(config DBConfig, tableName string, numRows int) error {
	// Create DSN (Data Source Name)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Build INSERT statement
	columnNames := make([]string, 0, len(dg.tableDef.Columns))
	placeholders := make([]string, 0, len(dg.tableDef.Columns))

	for _, column := range dg.tableDef.Columns {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", column.Name))
		placeholders = append(placeholders, "?")
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	fmt.Fprintf(os.Stderr, "Inserting %d rows into table %s...\n", numRows, tableName)

	// Prepare statement
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Generate and insert data in batches
	batchSize := 1000
	for i := 0; i < numRows; i += batchSize {
		end := i + batchSize
		if end > numRows {
			end = numRows
		}

		// Start transaction for batch
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Prepare statement for this transaction
		txStmt := tx.Stmt(stmt)

		for j := i; j < end; j++ {
			row := dg.GenerateRow(j + 1)

			// Convert row to interface slice for query
			values := make([]interface{}, 0, len(dg.tableDef.Columns))
			for _, column := range dg.tableDef.Columns {
				values = append(values, row[column.Name])
			}

			// Execute insert
			_, err := txStmt.Exec(values...)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to insert row %d: %w", j+1, err)
			}
		}

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		fmt.Fprintf(os.Stderr, "Inserted %d rows...\n", end)
	}

	fmt.Fprintf(os.Stderr, "Successfully inserted %d rows into table %s\n", numRows, tableName)
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage:")
		fmt.Println("  From SQL file: go run main.go <sql_file> <num_rows> [stats_file]")
		fmt.Println("  From database: go run main.go --db <host:port> <user> <password> <database> <table> <num_rows> [stats_file]")
		fmt.Println("  Insert to database: go run main.go --insert --db <host:port> <user> <password> <database> <table> <num_rows> [stats_file]")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println("  go run main.go t.create.sql 1000")
		fmt.Println("  go run main.go t.create.sql 1000 t.stats.json")
		fmt.Println("  go run main.go --db localhost:4000 root password testdb mytable 1000")
		fmt.Println("  go run main.go --db localhost:4000 root password testdb mytable 1000 t.stats.json")
		fmt.Println("  go run main.go --insert --db localhost:4000 root password testdb mytable 1000 t.stats.json")
		os.Exit(1)
	}

	var tableDef *TableDef
	var err error
	var insertMode bool

	if os.Args[1] == "--insert" {
		insertMode = true
		// Remove --insert from args and shift everything
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}

	if os.Args[1] == "--db" {
		// Database mode
		if len(os.Args) < 8 || len(os.Args) > 9 {
			fmt.Println("Database mode requires: --db <host:port> <user> <password> <database> <table> <num_rows> [stats_file]")
			os.Exit(1)
		}

		// Parse host:port
		hostPort := strings.Split(os.Args[2], ":")
		if len(hostPort) != 2 {
			fmt.Println("Invalid host:port format. Use format like 'localhost:4000'")
			os.Exit(1)
		}
		host := hostPort[0]
		port, _ := strconv.Atoi(hostPort[1])
		if port == 0 {
			fmt.Printf("Invalid port number: %s\n", hostPort[1])
			os.Exit(1)
		}

		config := DBConfig{
			Host:     host,
			Port:     port,
			User:     os.Args[3],
			Password: os.Args[4],
			Database: os.Args[5],
		}
		tableName := os.Args[6]
		numRowsStr := os.Args[7]
		statsFile := ""
		if len(os.Args) == 9 {
			statsFile = os.Args[8]
		}

		tableDef, err = parseTableFromDB(config, tableName)
		if err != nil {
			log.Fatalf("Failed to parse table from database: %v", err)
		}

		numRows, err := strconv.Atoi(numRowsStr)
		if err != nil {
			log.Fatalf("Invalid number of rows: %v", err)
		}

		generator, err := NewDataGeneratorFromTableDef(tableDef, statsFile)
		if err != nil {
			log.Fatalf("Failed to create data generator: %v", err)
		}

		if insertMode {
			// Insert data directly to database
			if err := generator.InsertDataToDB(config, tableName, numRows); err != nil {
				log.Fatalf("Failed to insert data: %v", err)
			}
		} else {
			// Generate data and output as JSON
			rows := generator.GenerateData(numRows)
			output, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				log.Fatalf("Failed to marshal data: %v", err)
			}
			fmt.Println(string(output))
		}

	} else {
		// File mode (existing functionality)
		if len(os.Args) < 3 || len(os.Args) > 4 {
			fmt.Println("File mode requires: <sql_file> <num_rows> [stats_file]")
			os.Exit(1)
		}

		sqlFile := os.Args[1]
		numRowsStr := os.Args[2]
		statsFile := ""
		if len(os.Args) == 4 {
			statsFile = os.Args[3]
		}

		numRows, err := strconv.Atoi(numRowsStr)
		if err != nil {
			log.Fatalf("Invalid number of rows: %v", err)
		}

		generator, err := NewDataGenerator(sqlFile, statsFile)
		if err != nil {
			log.Fatalf("Failed to create data generator: %v", err)
		}

		// Generate data
		rows := generator.GenerateData(numRows)

		// Output as JSON
		output, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal data: %v", err)
		}

		fmt.Println(string(output))
	}
}
