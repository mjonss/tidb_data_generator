package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
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
	Extra    string
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

	// Extract extra information (like auto_increment)
	extra := ""
	if strings.Contains(strings.ToUpper(definition), "AUTO_INCREMENT") {
		extra = "auto_increment"
	}

	return ColumnDef{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Default:  defaultValue,
		Extra:    extra,
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

	// Calculate null probability from stats if available
	nullProbability := 0.0
	if dg.stats != nil {
		if columnStats, exists := dg.stats.Columns[column.Name]; exists {
			// If we have stats, calculate null probability based on actual data
			// For now, we'll use a small probability if null_count is 0, or a higher one if it's > 0
			if columnStats.NullCount > 0 {
				nullProbability = 0.1 // 10% if there were nulls in original data
			} else {
				nullProbability = 0.0 // 0% if no nulls in original data
			}
		} else {
			// No stats available, use default behavior for nullable columns
			if column.Nullable {
				nullProbability = 0.1 // 10% default for nullable columns
			}
		}
	} else {
		// No stats file, use default behavior for nullable columns
		if column.Nullable {
			nullProbability = 0.1 // 10% default for nullable columns
		}
	}

	switch baseType {
	case "int", "integer", "bigint":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			return dg.rand.Intn(10000)
		}
	case "varchar", "char", "text":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
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
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			// Try to use stats if available
			if dg.stats != nil {
				if _, exists := dg.stats.Columns[column.Name]; exists {
					timeValue := dg.generateTimeFromStats(column.Name)
					// Format datetime without microseconds for database compatibility
					return timeValue.Format("2006-01-02 15:04:05")
				}
			}
			// Fallback to random time
			randomTime := time.Now().Add(time.Duration(dg.rand.Int63n(365*24*60*60)) * time.Second)
			return randomTime.Format("2006-01-02 15:04:05")
		}
	case "date":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			// Generate random date
			baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			randomDays := dg.rand.Intn(365 * 5) // 5 years range
			return baseTime.AddDate(0, 0, randomDays).Format("2006-01-02")
		}
	case "float", "double", "decimal":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			return dg.rand.Float64() * 1000
		}
	case "boolean", "bool":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			return dg.rand.Float32() > 0.5
		}
	default:
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
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
								// Check if duration is valid (positive)
								if duration > 0 {
									randomDuration := time.Duration(dg.rand.Int63n(int64(duration)))
									return lowerTime.Add(randomDuration)
								} else {
									// If duration is zero or negative, return the lower time
									return lowerTime
								}
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
			NUMERIC_SCALE,
			EXTRA
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
			extra        string
		)

		if err := rows.Scan(&columnName, &dataType, &isNullable, &defaultValue,
			&charLength, &numPrecision, &numScale, &extra); err != nil {
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
			Extra:    extra,
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
	// Create DSN (Data Source Name) with performance optimizations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Configure connection pool for better performance
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Build INSERT statement - skip auto-increment columns
	columnNames := make([]string, 0, len(dg.tableDef.Columns))
	placeholders := make([]string, 0, len(dg.tableDef.Columns))

	for _, column := range dg.tableDef.Columns {
		// Skip auto-increment columns
		if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", column.Name))
		placeholders = append(placeholders, "?")
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	fmt.Fprintf(os.Stderr, "Inserting %d rows into table %s...\n", numRows, tableName)

	// Use larger batch size for better performance
	batchSize := 5000
	startTime := time.Now()

	// Generate and insert data in batches
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
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare statement: %w", err)
		}

		// Generate all rows for this batch first
		rows := make([][]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			row := dg.GenerateRow(j + 1)

			// Convert row to interface slice for query - skip auto-increment columns
			values := make([]interface{}, 0, len(dg.tableDef.Columns))
			for _, column := range dg.tableDef.Columns {
				// Skip auto-increment columns
				if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
					continue
				}
				values = append(values, row[column.Name])
			}
			rows = append(rows, values)
		}

		// Execute all inserts in this batch
		for _, values := range rows {
			_, err := stmt.Exec(values...)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("failed to insert row: %w", err)
			}
		}

		stmt.Close()

		// Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		elapsed := time.Since(startTime)
		rate := float64(end) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "Inserted %d rows... (%.0f rows/sec)\n", end, rate)
	}

	totalTime := time.Since(startTime)
	totalRate := float64(numRows) / totalTime.Seconds()
	fmt.Fprintf(os.Stderr, "Successfully inserted %d rows into table %s in %.2fs (%.0f rows/sec)\n",
		numRows, tableName, totalTime.Seconds(), totalRate)
	return nil
}

// Alternative method using bulk INSERT with multiple VALUES
func (dg *DataGenerator) InsertDataToDBBulk(config DBConfig, tableName string, numRows int) error {
	// Create DSN (Data Source Name) with performance optimizations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Configure connection pool for better performance
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Build INSERT statement - skip auto-increment columns
	columnNames := make([]string, 0, len(dg.tableDef.Columns))
	placeholders := make([]string, 0, len(dg.tableDef.Columns))

	for _, column := range dg.tableDef.Columns {
		// Skip auto-increment columns
		if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", column.Name))
		placeholders = append(placeholders, "?")
	}

	fmt.Fprintf(os.Stderr, "Inserting %d rows into table %s using bulk inserts...\n", numRows, tableName)

	// Use larger batch size for bulk inserts
	batchSize := 10000
	startTime := time.Now()

	// Generate and insert data in batches
	for i := 0; i < numRows; i += batchSize {
		end := i + batchSize
		if end > numRows {
			end = numRows
		}

		// Build bulk INSERT statement
		valueGroups := make([]string, 0, end-i)
		allValues := make([]interface{}, 0, (end-i)*len(placeholders))

		for j := i; j < end; j++ {
			row := dg.GenerateRow(j + 1)
			valueGroups = append(valueGroups, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))

			// Convert row to interface slice for query - skip auto-increment columns
			for _, column := range dg.tableDef.Columns {
				// Skip auto-increment columns
				if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
					continue
				}
				allValues = append(allValues, row[column.Name])
			}
		}

		bulkInsertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
			tableName,
			strings.Join(columnNames, ", "),
			strings.Join(valueGroups, ", "))

		// Execute bulk insert
		_, err := db.Exec(bulkInsertSQL, allValues...)
		if err != nil {
			return fmt.Errorf("failed to execute bulk insert: %w", err)
		}

		elapsed := time.Since(startTime)
		rate := float64(end) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "Inserted %d rows... (%.0f rows/sec)\n", end, rate)
	}

	totalTime := time.Since(startTime)
	totalRate := float64(numRows) / totalTime.Seconds()
	fmt.Fprintf(os.Stderr, "Successfully inserted %d rows into table %s in %.2fs (%.0f rows/sec)\n",
		numRows, tableName, totalTime.Seconds(), totalRate)
	return nil
}

func main() {
	// Define command-line flags
	var (
		// Database connection flags
		host     = flag.String("host", "localhost", "Database host")
		port     = flag.Int("port", 3306, "Database port")
		user     = flag.String("user", "root", "Database user")
		password = flag.String("password", "", "Database password")
		database = flag.String("database", "", "Database name")

		// Short versions
		hostShort     = flag.String("H", "localhost", "Database host (short)")
		portShort     = flag.Int("P", 3306, "Database port (short)")
		userShort     = flag.String("u", "root", "Database user (short)")
		passwordShort = flag.String("p", "", "Database password (short)")
		databaseShort = flag.String("D", "", "Database name (short)")

		// Other flags
		table        = flag.String("table", "", "Table name")
		tableShort   = flag.String("t", "", "Table name (short)")
		stats        = flag.String("stats", "", "Stats file path")
		statsShort   = flag.String("s", "", "Stats file path (short)")
		sqlFile      = flag.String("sql", "", "SQL file path")
		sqlFileShort = flag.String("f", "", "SQL file path (short)")
		numRows      = flag.Int("rows", 0, "Number of rows to generate")
		numRowsShort = flag.Int("n", 0, "Number of rows to generate (short)")
		insert       = flag.Bool("insert", false, "Insert data directly to database instead of outputting JSON")
		insertShort  = flag.Bool("i", false, "Insert data directly to database (short)")
		bulkInsert   = flag.Bool("bulk", false, "Use bulk INSERT for faster database insertion")

		// Help
		help      = flag.Bool("help", false, "Show help message")
		helpShort = flag.Bool("h", false, "Show help message (short)")
	)

	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  Database Connection:\n")
		fmt.Fprintf(os.Stderr, "    --host, -H <host>        Database host (default: localhost)\n")
		fmt.Fprintf(os.Stderr, "    --port, -P <port>        Database port (default: 3306)\n")
		fmt.Fprintf(os.Stderr, "    --user, -u <user>        Database user (default: root)\n")
		fmt.Fprintf(os.Stderr, "    --password, -p <pass>    Database password\n")
		fmt.Fprintf(os.Stderr, "    --database, -D <db>      Database name\n")
		fmt.Fprintf(os.Stderr, "  Data Generation:\n")
		fmt.Fprintf(os.Stderr, "    --table, -t <table>      Table name (required for database mode)\n")
		fmt.Fprintf(os.Stderr, "    --sql, -f <file>         SQL file path (required for file mode)\n")
		fmt.Fprintf(os.Stderr, "    --rows, -n <num>         Number of rows to generate (required)\n")
		fmt.Fprintf(os.Stderr, "    --stats, -s <file>       Stats file path (optional)\n")
		fmt.Fprintf(os.Stderr, "    --insert, -i             Insert data directly to database\n")
		fmt.Fprintf(os.Stderr, "    --bulk                   Use bulk INSERT for faster insertion\n")
		fmt.Fprintf(os.Stderr, "    --help, -h               Show this help message\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from SQL file\n")
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from database\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data directly to database (standard method)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using bulk INSERT (faster)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk\n", os.Args[0])
	}

	flag.Parse()

	// Check for help
	if *help || *helpShort {
		flag.Usage()
		os.Exit(0)
	}

	// Resolve conflicting flags (long form takes precedence)
	finalHost := *host
	if *hostShort != "localhost" {
		finalHost = *hostShort
	}

	finalPort := *port
	if *portShort != 3306 {
		finalPort = *portShort
	}

	finalUser := *user
	if *userShort != "root" {
		finalUser = *userShort
	}

	finalPassword := *password
	if *passwordShort != "" {
		finalPassword = *passwordShort
	}

	finalDatabase := *database
	if *databaseShort != "" {
		finalDatabase = *databaseShort
	}

	finalTable := *table
	if *tableShort != "" {
		finalTable = *tableShort
	}

	finalStats := *stats
	if *statsShort != "" {
		finalStats = *statsShort
	}

	finalSQLFile := *sqlFile
	if *sqlFileShort != "" {
		finalSQLFile = *sqlFileShort
	}

	finalNumRows := *numRows
	if *numRowsShort != 0 {
		finalNumRows = *numRowsShort
	}

	finalInsert := *insert || *insertShort
	finalBulkInsert := *bulkInsert

	// Validate required parameters
	if finalNumRows <= 0 {
		fmt.Fprintf(os.Stderr, "Error: Number of rows (-n/--rows) is required and must be > 0\n")
		flag.Usage()
		os.Exit(1)
	}

	// Determine mode and validate parameters
	var tableDef *TableDef
	var err error

	if finalSQLFile != "" {
		// File mode
		if finalTable != "" || finalHost != "localhost" || finalPort != 3306 || finalUser != "root" || finalPassword != "" || finalDatabase != "" {
			fmt.Fprintf(os.Stderr, "Error: Database parameters should not be specified when using SQL file mode\n")
			flag.Usage()
			os.Exit(1)
		}

		tableDef, err = parseCreateTable(finalSQLFile)
		if err != nil {
			log.Fatalf("Failed to parse SQL file: %v", err)
		}

		generator, err := NewDataGenerator(finalSQLFile, finalStats)
		if err != nil {
			log.Fatalf("Failed to create data generator: %v", err)
		}

		// Generate data
		rows := generator.GenerateData(finalNumRows)

		// Output as JSON
		output, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal data: %v", err)
		}

		fmt.Println(string(output))

	} else if finalTable != "" && finalDatabase != "" {
		// Database mode
		if finalSQLFile != "" {
			fmt.Fprintf(os.Stderr, "Error: SQL file should not be specified when using database mode\n")
			flag.Usage()
			os.Exit(1)
		}

		config := DBConfig{
			Host:     finalHost,
			Port:     finalPort,
			User:     finalUser,
			Password: finalPassword,
			Database: finalDatabase,
		}

		tableDef, err = parseTableFromDB(config, finalTable)
		if err != nil {
			log.Fatalf("Failed to parse table from database: %v", err)
		}

		generator, err := NewDataGeneratorFromTableDef(tableDef, finalStats)
		if err != nil {
			log.Fatalf("Failed to create data generator: %v", err)
		}

		if finalInsert {
			// Insert data directly to database
			if finalBulkInsert {
				if err := generator.InsertDataToDBBulk(config, finalTable, finalNumRows); err != nil {
					log.Fatalf("Failed to insert data: %v", err)
				}
			} else {
				if err := generator.InsertDataToDB(config, finalTable, finalNumRows); err != nil {
					log.Fatalf("Failed to insert data: %v", err)
				}
			}
		} else {
			// Generate data and output as JSON
			rows := generator.GenerateData(finalNumRows)
			output, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				log.Fatalf("Failed to marshal data: %v", err)
			}
			fmt.Println(string(output))
		}

	} else {
		// Invalid mode
		fmt.Fprintf(os.Stderr, "Error: Must specify either SQL file (-f/--sql) or database table (-t/--table) with database (-D/--database)\n")
		flag.Usage()
		os.Exit(1)
	}
}
