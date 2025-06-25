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
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Global verbose flag
var verboseMode bool

// Debug helper function
func debugPrint(format string, args ...interface{}) {
	if verboseMode {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

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
	Name       string
	Columns    []ColumnDef
	PrimaryKey []string
	UniqueKeys [][]string // Each unique key is a list of column names
}

// Statistics structures
type Stats struct {
	Count   int                    `json:"count"`
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
		// Skip primary key columns - they will be set directly in GenerateRow
		isPrimaryKey := false
		if len(dg.tableDef.PrimaryKey) == 1 && column.Name == dg.tableDef.PrimaryKey[0] {
			isPrimaryKey = true
		}

		if !isPrimaryKey {
			dg.columnGenerators[column.Name] = dg.createColumnGenerator(column)
		}
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
	case "int", "integer", "bigint", "smallint", "mediumint":
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
	case "boolean", "bool", "tinyint":
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

	// Identify all unique/primary key columns
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)

	// For integer single-column PK/UK, generate sequentially
	for _, colSet := range keyCols {
		if len(colSet) == 1 {
			colName := colSet[0]
			colType := ""
			for _, c := range dg.tableDef.Columns {
				if c.Name == colName {
					colType = c.Type
					break
				}
			}
			if strings.Contains(colType, "int") {
				row[colName] = id
			} else if isStringType(colType) {
				// For string unique columns, generate deterministic unique value
				row[colName] = fmt.Sprintf("%s_%d", colName, id)
			}
		}
	}

	// Generate other columns
	for _, column := range dg.tableDef.Columns {
		if _, ok := row[column.Name]; ok {
			continue // already set
		}
		generator := dg.columnGenerators[column.Name]
		row[column.Name] = generator()
	}

	return row
}

// Helper to check if a type is a string type
func isStringType(typ string) bool {
	baseType := typ
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = baseType[:idx]
	}
	baseType = strings.ToLower(baseType)
	return baseType == "varchar" || baseType == "char" || baseType == "text"
}

func (dg *DataGenerator) GenerateData(numRows int) []TableRow {
	rows := make([]TableRow, 0, numRows)
	// Track unique values for all unique/primary key columns/tuples
	uniqueSets := make([]map[string]struct{}, 0)
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)
	for range keyCols {
		uniqueSets = append(uniqueSets, make(map[string]struct{}))
	}

	nextInt := 1
	for len(rows) < numRows {
		row := make(TableRow)
		// For integer single-column PK/UK, generate sequentially
		for _, colSet := range keyCols {
			if len(colSet) == 1 {
				colName := colSet[0]
				colType := ""
				for _, c := range dg.tableDef.Columns {
					if c.Name == colName {
						colType = c.Type
						break
					}
				}
				if strings.Contains(colType, "int") {
					row[colName] = nextInt
				}
			}
		}
		// Generate other columns
		for _, column := range dg.tableDef.Columns {
			if _, ok := row[column.Name]; ok {
				continue // already set
			}
			generator := dg.columnGenerators[column.Name]
			row[column.Name] = generator()
		}
		// Check uniqueness
		isUnique := true
		for i, colSet := range keyCols {
			key := ""
			for _, col := range colSet {
				key += fmt.Sprintf("|%v", row[col])
			}
			if _, exists := uniqueSets[i][key]; exists {
				isUnique = false
				break
			}
		}
		if isUnique {
			for i, colSet := range keyCols {
				key := ""
				for _, col := range colSet {
					key += fmt.Sprintf("|%v", row[col])
				}
				uniqueSets[i][key] = struct{}{}
			}
			rows = append(rows, row)
			nextInt++
		}
		// else: retry
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

	// Query primary key and unique keys
	pkQuery := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`
	pkRows, err := db.Query(pkQuery, config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer pkRows.Close()
	primaryKey := []string{}
	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err == nil {
			primaryKey = append(primaryKey, col)
		}
	}

	uniqueQuery := `
		SELECT INDEX_NAME, COLUMN_NAME
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`
	uniqueRows, err := db.Query(uniqueQuery, config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query unique keys: %w", err)
	}
	defer uniqueRows.Close()
	uniqueKeyMap := map[string][]string{}
	for uniqueRows.Next() {
		var idx, col string
		if err := uniqueRows.Scan(&idx, &col); err == nil {
			uniqueKeyMap[idx] = append(uniqueKeyMap[idx], col)
		}
	}
	uniqueKeys := [][]string{}
	for _, cols := range uniqueKeyMap {
		uniqueKeys = append(uniqueKeys, cols)
	}

	return &TableDef{
		Name:       tableName,
		Columns:    columns,
		PrimaryKey: primaryKey,
		UniqueKeys: uniqueKeys,
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

// Get the next available value for a single-column integer primary key
func getNextAvailableID(config DBConfig, tableDef *TableDef) (int, error) {
	if len(tableDef.PrimaryKey) != 1 {
		// Only support single-column PK for now
		debugPrint("[DEBUG] getNextAvailableID: not a single-column PK\n")
		return 1, nil
	}
	pkCol := tableDef.PrimaryKey[0]
	var pkType string
	for _, col := range tableDef.Columns {
		if col.Name == pkCol {
			pkType = col.Type
			break
		}
	}
	if !(strings.Contains(pkType, "int")) {
		// Only support integer PK for now
		debugPrint("[DEBUG] getNextAvailableID: PK is not integer type (%s)\n", pkType)
		return 1, nil
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		debugPrint("[DEBUG] getNextAvailableID: failed to connect: %v\n", err)
		return 1, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Try to get the maximum PK value from the table
	var maxID sql.NullInt64
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", pkCol, tableDef.Name)
	debugPrint("[DEBUG] getNextAvailableID: executing query: %s\n", query)
	err = db.QueryRow(query).Scan(&maxID)
	if err != nil {
		debugPrint("[DEBUG] getNextAvailableID: query failed: %v\n", err)
		return 1, nil
	}
	debugPrint("[DEBUG] getNextAvailableID: maxID.Valid=%v, maxID.Int64=%d\n", maxID.Valid, maxID.Int64)
	if maxID.Valid {
		return int(maxID.Int64) + 1, nil
	}
	return 1, nil
}

// Simplified insert method for benchmarking (no progress output)
func (dg *DataGenerator) InsertDataToDBBenchmark(config DBConfig, tableName string, numRows int) error {
	// Create DSN (Data Source Name) with performance optimizations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

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

	// Get the next available ID to avoid duplicate key errors
	nextID, err := getNextAvailableID(config, dg.tableDef)
	if err != nil {
		return fmt.Errorf("failed to get next available ID: %w", err)
	}

	// Use larger batch size for better performance
	batchSize := 5000

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
			row := dg.GenerateRow(nextID + j)

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
	}

	return nil
}

// Standard insert method (non-parallel)
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

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

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

	// Get the next available ID to avoid duplicate key errors
	nextID, err := getNextAvailableID(config, dg.tableDef)
	if err != nil {
		return fmt.Errorf("failed to get next available ID: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Starting with ID: %d\n", nextID)

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
			row := dg.GenerateRow(nextID + j)

			// Debug: show first few generated IDs
			if j < 5 {
				debugPrint("[DEBUG] Generated row %d with ID: %v\n", j, row[dg.tableDef.PrimaryKey[0]])
			}

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

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

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

	// Get the next available ID to avoid duplicate key errors
	nextID, err := getNextAvailableID(config, dg.tableDef)
	if err != nil {
		return fmt.Errorf("failed to get next available ID: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Starting with ID: %d\n", nextID)

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
			row := dg.GenerateRow(nextID + j)
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

// Parallel insert method using multiple goroutines
func (dg *DataGenerator) InsertDataToDBParallel(config DBConfig, tableName string, numRows int, numWorkers int) error {
	fmt.Fprintf(os.Stderr, "Inserting %d rows into table %s using %d parallel workers...\n", numRows, tableName, numWorkers)

	// Get the next available ID to avoid duplicate key errors
	nextID, err := getNextAvailableID(config, dg.tableDef)
	if err != nil {
		return fmt.Errorf("failed to get next available ID: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Starting with ID: %d\n", nextID)

	// Create DSN (Data Source Name) with performance optimizations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

	// Configure connection pool for parallel operations
	db.SetMaxOpenConns(numWorkers * 2)
	db.SetMaxIdleConns(numWorkers)
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

	// Use smaller batch size for better progress reporting
	batchSize := 1000
	startTime := time.Now()

	// Create channels for coordination
	jobs := make(chan int, numRows)
	results := make(chan error, numWorkers)
	progress := make(chan int, numWorkers) // Channel for progress updates
	var wg sync.WaitGroup

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own database connection
			workerDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to connect: %w", workerID, err)
				return
			}
			defer workerDB.Close()

			// Set session time zone to UTC
			_, err = workerDB.Exec("SET time_zone = 'UTC'")
			if err != nil {
				results <- fmt.Errorf("worker %d failed to set session time_zone: %w", workerID, err)
				return
			}

			// Prepare statement for this worker
			stmt, err := workerDB.Prepare(insertSQL)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to prepare statement: %w", workerID, err)
				return
			}
			defer stmt.Close()

			// Process jobs
			for startRow := range jobs {
				endRow := startRow + batchSize
				if endRow > numRows {
					endRow = numRows
				}

				// Start transaction for this batch
				tx, err := workerDB.Begin()
				if err != nil {
					results <- fmt.Errorf("worker %d failed to begin transaction: %w", workerID, err)
					return
				}

				// Prepare statement for this transaction
				txStmt := tx.Stmt(stmt)

				// Generate and insert rows for this batch
				for rowID := startRow; rowID < endRow; rowID++ {
					row := dg.GenerateRow(nextID + rowID)

					// Convert row to interface slice for query - skip auto-increment columns
					values := make([]interface{}, 0, len(dg.tableDef.Columns))
					for _, column := range dg.tableDef.Columns {
						// Skip auto-increment columns
						if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
							continue
						}
						values = append(values, row[column.Name])
					}

					// Execute insert
					_, err := txStmt.Exec(values...)
					if err != nil {
						tx.Rollback()
						results <- fmt.Errorf("worker %d failed to insert row %d: %w", workerID, rowID+1, err)
						return
					}
				}

				// Commit transaction
				if err := tx.Commit(); err != nil {
					results <- fmt.Errorf("worker %d failed to commit transaction: %w", workerID, err)
					return
				}

				// Send progress update
				progress <- endRow - startRow
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i := 0; i < numRows; i += batchSize {
			jobs <- i
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
		close(progress)
	}()

	// Monitor progress and collect errors
	completedRows := 0
	lastReportedRows := 0
	progressTicker := time.NewTicker(1 * time.Second) // Show progress every 1 second
	defer progressTicker.Stop()

	// Create a channel to signal when all work is done
	done := make(chan bool)

	// Start progress monitoring goroutine
	go func() {
		for {
			select {
			case <-progressTicker.C:
				if completedRows > 0 && completedRows != lastReportedRows {
					elapsed := time.Since(startTime)
					rate := float64(completedRows) / elapsed.Seconds()
					fmt.Fprintf(os.Stderr, "Completed %d rows... (%.0f rows/sec)\n", completedRows, rate)
					lastReportedRows = completedRows
				}
			case <-done:
				return
			}
		}
	}()

	// Collect progress updates and errors
	for {
		select {
		case batchSize, ok := <-progress:
			if !ok {
				// Progress channel closed, check for errors
				for err := range results {
					if err != nil {
						done <- true
						return fmt.Errorf("parallel insert failed: %w", err)
					}
				}
				done <- true
				goto finished
			}
			completedRows += batchSize
			if completedRows > numRows {
				completedRows = numRows
			}
		case err := <-results:
			if err != nil {
				done <- true
				return fmt.Errorf("parallel insert failed: %w", err)
			}
		}
	}

	// Final progress report
	if completedRows > 0 && completedRows != lastReportedRows {
		elapsed := time.Since(startTime)
		rate := float64(completedRows) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "Completed %d rows... (%.0f rows/sec)\n", completedRows, rate)
	}

finished:
	totalTime := time.Since(startTime)
	totalRate := float64(numRows) / totalTime.Seconds()
	fmt.Fprintf(os.Stderr, "Successfully inserted %d rows into table %s in %.2fs (%.0f rows/sec)\n",
		numRows, tableName, totalTime.Seconds(), totalRate)
	return nil
}

// Parallel bulk insert method
func (dg *DataGenerator) InsertDataToDBBulkParallel(config DBConfig, tableName string, numRows int, numWorkers int) error {
	fmt.Fprintf(os.Stderr, "Inserting %d rows into table %s using %d parallel workers with bulk inserts...\n", numRows, tableName, numWorkers)

	// Get the next available ID to avoid duplicate key errors
	nextID, err := getNextAvailableID(config, dg.tableDef)
	if err != nil {
		return fmt.Errorf("failed to get next available ID: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Starting with ID: %d\n", nextID)

	// Create DSN (Data Source Name) with performance optimizations
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

	// Configure connection pool for parallel operations
	db.SetMaxOpenConns(numWorkers * 2)
	db.SetMaxIdleConns(numWorkers)
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

	// Use smaller batch size for better progress reporting
	batchSize := 1000
	startTime := time.Now()

	// Create channels for coordination
	jobs := make(chan int, numRows)
	results := make(chan error, numWorkers)
	progress := make(chan int, numWorkers) // Channel for progress updates
	var wg sync.WaitGroup

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own database connection
			workerDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to connect: %w", workerID, err)
				return
			}
			defer workerDB.Close()

			// Set session time zone to UTC
			_, err = workerDB.Exec("SET time_zone = 'UTC'")
			if err != nil {
				results <- fmt.Errorf("worker %d failed to set session time_zone: %w", workerID, err)
				return
			}

			// Process jobs
			for startRow := range jobs {
				endRow := startRow + batchSize
				if endRow > numRows {
					endRow = numRows
				}

				// Build bulk INSERT statement for this batch
				valueGroups := make([]string, 0, endRow-startRow)
				allValues := make([]interface{}, 0, (endRow-startRow)*len(placeholders))

				for rowID := startRow; rowID < endRow; rowID++ {
					row := dg.GenerateRow(nextID + rowID)
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
				_, err := workerDB.Exec(bulkInsertSQL, allValues...)
				if err != nil {
					results <- fmt.Errorf("worker %d failed to execute bulk insert: %w", workerID, err)
					return
				}

				// Send progress update
				progress <- endRow - startRow
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i := 0; i < numRows; i += batchSize {
			jobs <- i
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
		close(progress)
	}()
	// Monitor progress and collect errors
	completedRows := 0
	lastReportedRows := 0
	progressTicker := time.NewTicker(1 * time.Second) // Show progress every 1 second
	defer progressTicker.Stop()

	// Create a channel to signal when all work is done
	done := make(chan bool)

	// Start progress monitoring goroutine
	go func() {
		for {
			select {
			case <-progressTicker.C:
				if completedRows > 0 && completedRows != lastReportedRows {
					elapsed := time.Since(startTime)
					rate := float64(completedRows) / elapsed.Seconds()
					fmt.Fprintf(os.Stderr, "Completed %d rows... (%.0f rows/sec)\n", completedRows, rate)
					lastReportedRows = completedRows
				}
			case <-done:
				return
			}
		}
	}()

	// Collect progress updates and errors
	for {
		select {
		case batchSize, ok := <-progress:
			if !ok {
				// Progress channel closed, check for errors
				for err := range results {
					if err != nil {
						done <- true
						return fmt.Errorf("parallel bulk insert failed: %w", err)
					}
				}
				done <- true
				goto finished
			}
			completedRows += batchSize
			if completedRows > numRows {
				completedRows = numRows
			}
		case err := <-results:
			if err != nil {
				done <- true
				return fmt.Errorf("parallel bulk insert failed: %w", err)
			}
		}
	}

	// Final progress report
	if completedRows > 0 && completedRows != lastReportedRows {
		elapsed := time.Since(startTime)
		rate := float64(completedRows) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "Completed %d rows... (%.0f rows/sec)\n", completedRows, rate)
	}

finished:
	totalTime := time.Since(startTime)
	totalRate := float64(numRows) / totalTime.Seconds()
	fmt.Fprintf(os.Stderr, "Successfully inserted %d rows into table %s in %.2fs (%.0f rows/sec)\n",
		numRows, tableName, totalTime.Seconds(), totalRate)
	return nil
}

// Auto-tuning parallel insert method
func (dg *DataGenerator) InsertDataToDBParallelAutoTune(config DBConfig, tableName string, numRows int) error {
	fmt.Fprintf(os.Stderr, "Auto-tuning parallel insert for %d rows...\n", numRows)

	benchmarkDuration := 2 * time.Second
	maxWorkers := 512 // Restored to 512 for very large machines
	bestWorkers := 1
	bestPerformance := 0.0

	for workers := 1; workers <= maxWorkers; workers *= 2 {
		fmt.Fprintf(os.Stderr, "Testing with %d workers for %v...\n", workers, benchmarkDuration)

		// Get the next available ID for this benchmark to avoid conflicts
		nextID, err := getNextAvailableID(config, dg.tableDef)
		if err != nil {
			return fmt.Errorf("failed to get next available ID: %w", err)
		}

		// Benchmark with this number of workers for the full duration
		startTime := time.Now()
		stopTime := startTime.Add(benchmarkDuration)
		rowsInserted := 0
		currentID := nextID

		// Run benchmark for the full duration using the real table
		for time.Now().Before(stopTime) {
			// Insert a batch of 100 rows using the real table
			err = dg.insertBatchToRealTable(config, tableName, 100, workers, currentID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  Benchmark failed with %d workers: %v\n", workers, err)
				break
			}
			rowsInserted += 100
			currentID += 100 // Increment ID for next batch
		}

		elapsed := time.Since(startTime)
		performance := float64(rowsInserted) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "  %d workers: %.0f rows/sec\n", workers, performance)

		if performance > bestPerformance {
			bestPerformance = performance
			bestWorkers = workers
		} else if workers > 1 && performance < bestPerformance*0.8 {
			// Performance is degrading significantly, stop testing
			break
		}
	}

	fmt.Fprintf(os.Stderr, "Best performance: %d workers (%.0f rows/sec)\n", bestWorkers, bestPerformance)
	fmt.Fprintf(os.Stderr, "Inserting %d rows with %d workers...\n", numRows, bestWorkers)
	return dg.InsertDataToDBParallel(config, tableName, numRows, bestWorkers)
}

// Auto-tuning parallel bulk insert method
func (dg *DataGenerator) InsertDataToDBBulkParallelAutoTune(config DBConfig, tableName string, numRows int) error {
	fmt.Fprintf(os.Stderr, "Auto-tuning parallel bulk insert for %d rows...\n", numRows)

	benchmarkDuration := 2 * time.Second
	maxWorkers := 512 // Restored to 512 for very large machines
	bestWorkers := 1
	bestPerformance := 0.0

	for workers := 1; workers <= maxWorkers; workers *= 2 {
		fmt.Fprintf(os.Stderr, "Testing with %d workers for %v...\n", workers, benchmarkDuration)

		// Get the next available ID for this benchmark to avoid conflicts
		nextID, err := getNextAvailableID(config, dg.tableDef)
		if err != nil {
			return fmt.Errorf("failed to get next available ID: %w", err)
		}

		// Benchmark with this number of workers for the full duration
		startTime := time.Now()
		stopTime := startTime.Add(benchmarkDuration)
		rowsInserted := 0
		currentID := nextID

		// Run benchmark for the full duration using the real table
		for time.Now().Before(stopTime) {
			// Insert a batch of 100 rows using the real table
			err = dg.insertBatchToRealTableBulk(config, tableName, 100, workers, currentID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  Benchmark failed with %d workers: %v\n", workers, err)
				break
			}
			rowsInserted += 100
			currentID += 100 // Increment ID for next batch
		}

		elapsed := time.Since(startTime)
		performance := float64(rowsInserted) / elapsed.Seconds()
		fmt.Fprintf(os.Stderr, "  %d workers: %.0f rows/sec\n", workers, performance)

		if performance > bestPerformance {
			bestPerformance = performance
			bestWorkers = workers
		} else if workers > 1 && performance < bestPerformance*0.8 {
			// Performance is degrading significantly, stop testing
			break
		}
	}

	fmt.Fprintf(os.Stderr, "Best performance: %d workers (%.0f rows/sec)\n", bestWorkers, bestPerformance)
	fmt.Fprintf(os.Stderr, "Inserting %d rows with %d workers...\n", numRows, bestWorkers)
	return dg.InsertDataToDBBulkParallel(config, tableName, numRows, bestWorkers)
}

// Simple method to insert a batch to real table (uses specified start ID)
func (dg *DataGenerator) insertBatchToRealTable(config DBConfig, tableName string, numRows int, numWorkers int, startID int) error {
	// Create DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(numWorkers * 2)
	db.SetMaxIdleConns(numWorkers)
	db.SetConnMaxLifetime(time.Hour)

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

	// Use smaller batch size for better progress reporting
	batchSize := 100

	// Track unique values for unique key columns across all workers
	var uniqueMutex sync.Mutex
	uniqueSets := make([]map[string]struct{}, 0)
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)
	for range keyCols {
		uniqueSets = append(uniqueSets, make(map[string]struct{}))
	}

	// Pre-generate all rows with uniqueness check to avoid race conditions
	allRows := make([][]interface{}, 0, numRows)
	rowID := startID

	for i := 0; i < numRows; i++ {
		// Generate row with uniqueness check
		var row TableRow
		maxRetries := 100
		for retry := 0; retry < maxRetries; retry++ {
			row = dg.GenerateRow(rowID)

			// Check uniqueness for unique key columns
			uniqueMutex.Lock()
			isUnique := true
			for i, colSet := range keyCols {
				key := ""
				for _, col := range colSet {
					key += fmt.Sprintf("|%v", row[col])
				}
				if _, exists := uniqueSets[i][key]; exists {
					isUnique = false
					break
				}
			}

			if isUnique {
				// Mark as used
				for i, colSet := range keyCols {
					key := ""
					for _, col := range colSet {
						key += fmt.Sprintf("|%v", row[col])
					}
					uniqueSets[i][key] = struct{}{}
				}
				uniqueMutex.Unlock()
				break
			}
			uniqueMutex.Unlock()

			if retry == maxRetries-1 {
				return fmt.Errorf("failed to generate unique row after %d retries", maxRetries)
			}
		}

		// Convert row to interface slice for query - skip auto-increment columns
		values := make([]interface{}, 0, len(dg.tableDef.Columns))
		for _, column := range dg.tableDef.Columns {
			// Skip auto-increment columns
			if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
				continue
			}
			values = append(values, row[column.Name])
		}
		allRows = append(allRows, values)
		rowID++
	}

	// Create channels for coordination
	jobs := make(chan int, numRows)
	results := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own database connection
			workerDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to connect: %w", workerID, err)
				return
			}
			defer workerDB.Close()

			// Set session time zone to UTC
			_, err = workerDB.Exec("SET time_zone = 'UTC'")
			if err != nil {
				results <- fmt.Errorf("worker %d failed to set session time_zone: %w", workerID, err)
				return
			}

			// Prepare statement for this worker
			stmt, err := workerDB.Prepare(insertSQL)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to prepare statement: %w", workerID, err)
				return
			}
			defer stmt.Close()

			// Process jobs
			for startRow := range jobs {
				endRow := startRow + batchSize
				if endRow > numRows {
					endRow = numRows
				}

				// Start transaction for this batch
				tx, err := workerDB.Begin()
				if err != nil {
					results <- fmt.Errorf("worker %d failed to begin transaction: %w", workerID, err)
					return
				}

				// Prepare statement for this transaction
				txStmt := tx.Stmt(stmt)

				// Insert pre-generated rows for this batch
				for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
					// Execute insert
					_, err := txStmt.Exec(allRows[rowIdx]...)
					if err != nil {
						tx.Rollback()
						results <- fmt.Errorf("worker %d failed to insert row %d: %w", workerID, startID+rowIdx, err)
						return
					}
				}

				// Commit transaction
				if err := tx.Commit(); err != nil {
					results <- fmt.Errorf("worker %d failed to commit transaction: %w", workerID, err)
					return
				}
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i := 0; i < numRows; i += batchSize {
			jobs <- i
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect errors
	for err := range results {
		if err != nil {
			return fmt.Errorf("parallel insert failed: %w", err)
		}
	}

	return nil
}

// Simple method to insert a batch to real table using bulk insert (uses specified start ID)
func (dg *DataGenerator) insertBatchToRealTableBulk(config DBConfig, tableName string, numRows int, numWorkers int, startID int) error {
	// Create DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set session time_zone: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(numWorkers * 2)
	db.SetMaxIdleConns(numWorkers)
	db.SetConnMaxLifetime(time.Hour)

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

	// Track unique values for unique key columns across all workers
	var uniqueMutex sync.Mutex
	uniqueSets := make([]map[string]struct{}, 0)
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)
	for range keyCols {
		uniqueSets = append(uniqueSets, make(map[string]struct{}))
	}

	// Pre-generate all rows with uniqueness check to avoid race conditions
	allRows := make([][]interface{}, 0, numRows)
	rowID := startID

	for i := 0; i < numRows; i++ {
		// Generate row with uniqueness check
		var row TableRow
		maxRetries := 100
		for retry := 0; retry < maxRetries; retry++ {
			row = dg.GenerateRow(rowID)

			// Check uniqueness for unique key columns
			uniqueMutex.Lock()
			isUnique := true
			for i, colSet := range keyCols {
				key := ""
				for _, col := range colSet {
					key += fmt.Sprintf("|%v", row[col])
				}
				if _, exists := uniqueSets[i][key]; exists {
					isUnique = false
					break
				}
			}

			if isUnique {
				// Mark as used
				for i, colSet := range keyCols {
					key := ""
					for _, col := range colSet {
						key += fmt.Sprintf("|%v", row[col])
					}
					uniqueSets[i][key] = struct{}{}
				}
				uniqueMutex.Unlock()
				break
			}
			uniqueMutex.Unlock()

			if retry == maxRetries-1 {
				return fmt.Errorf("failed to generate unique row after %d retries", maxRetries)
			}
		}

		// Convert row to interface slice for query - skip auto-increment columns
		values := make([]interface{}, 0, len(dg.tableDef.Columns))
		for _, column := range dg.tableDef.Columns {
			// Skip auto-increment columns
			if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
				continue
			}
			values = append(values, row[column.Name])
		}
		allRows = append(allRows, values)
		rowID++
	}

	// Use smaller batch size for better progress reporting
	batchSize := 100

	// Create channels for coordination
	jobs := make(chan int, numRows)
	results := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own database connection
			workerDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to connect: %w", workerID, err)
				return
			}
			defer workerDB.Close()

			// Set session time zone to UTC
			_, err = workerDB.Exec("SET time_zone = 'UTC'")
			if err != nil {
				results <- fmt.Errorf("worker %d failed to set session time_zone: %w", workerID, err)
				return
			}

			// Process jobs
			for startRow := range jobs {
				endRow := startRow + batchSize
				if endRow > numRows {
					endRow = numRows
				}

				// Build bulk INSERT statement for this batch
				valueGroups := make([]string, 0, endRow-startRow)
				allValues := make([]interface{}, 0, (endRow-startRow)*len(placeholders))

				for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
					valueGroups = append(valueGroups, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
					allValues = append(allValues, allRows[rowIdx]...)
				}

				bulkInsertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
					tableName,
					strings.Join(columnNames, ", "),
					strings.Join(valueGroups, ", "))

				// Execute bulk insert
				_, err := workerDB.Exec(bulkInsertSQL, allValues...)
				if err != nil {
					results <- fmt.Errorf("worker %d failed to execute bulk insert: %w", workerID, err)
					return
				}
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i := 0; i < numRows; i += batchSize {
			jobs <- i
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect errors
	for err := range results {
		if err != nil {
			return fmt.Errorf("parallel bulk insert failed: %w", err)
		}
	}

	return nil
}

// Helper function to get effective number of rows
func getEffectiveNumRows(generator *DataGenerator, commandLineRows int, maxRows int) int {
	effectiveRows := commandLineRows
	if commandLineRows <= 0 && generator.stats != nil && generator.stats.Count > 0 {
		fmt.Fprintf(os.Stderr, "Using row count from stats file: %d\n", generator.stats.Count)
		effectiveRows = generator.stats.Count
	}

	// Apply max rows limit if specified
	if maxRows > 0 && effectiveRows > maxRows {
		fmt.Fprintf(os.Stderr, "Limiting rows to maximum: %d (requested: %d)\n", maxRows, effectiveRows)
		effectiveRows = maxRows
	}

	return effectiveRows
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
		maxRows      = flag.Int("max-rows", 0, "Maximum number of rows to generate (0=no limit)")
		insert       = flag.Bool("insert", false, "Insert data directly to database instead of outputting JSON")
		insertShort  = flag.Bool("i", false, "Insert data directly to database (short)")
		bulkInsert   = flag.Bool("bulk", false, "Use bulk INSERT for faster database insertion")
		workers      = flag.Int("workers", 1, "Number of parallel workers (default: 1=serial, >1=parallel, 0=auto-tune)")

		// Help
		help      = flag.Bool("help", false, "Show help message")
		helpShort = flag.Bool("h", false, "Show help message (short)")
		verbose   = flag.Bool("verbose", false, "Enable verbose debug output")
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
		fmt.Fprintf(os.Stderr, "    --rows, -n <num>         Number of rows to generate (required, or use count from stats file)\n")
		fmt.Fprintf(os.Stderr, "    --max-rows <num>         Maximum number of rows to generate (0=no limit)\n")
		fmt.Fprintf(os.Stderr, "    --stats, -s <file>       Stats file path (optional)\n")
		fmt.Fprintf(os.Stderr, "    --insert, -i             Insert data directly to database\n")
		fmt.Fprintf(os.Stderr, "    --bulk                   Use bulk INSERT for faster insertion\n")
		fmt.Fprintf(os.Stderr, "    --workers <num>          Number of parallel workers (default: 1=serial, >1=parallel, 0=auto-tune)\n")
		fmt.Fprintf(os.Stderr, "    --verbose                Enable verbose debug output\n")
		fmt.Fprintf(os.Stderr, "    --help, -h               Show this help message\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from SQL file\n")
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from database\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data directly to database (serial, 1 worker)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using bulk INSERT (faster)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using parallel workers (8 workers)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --workers 8\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using parallel workers with bulk INSERT (fastest)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk --workers 8\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using auto-tuning (finds optimal worker count)\n")
		fmt.Fprintf(os.Stderr, "  %s -H localhost -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk --workers 0\n", os.Args[0])
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

	finalMaxRows := *maxRows

	finalInsert := *insert || *insertShort
	finalBulkInsert := *bulkInsert
	finalWorkers := *workers
	finalVerbose := *verbose

	// Set global verbose flag
	verboseMode = finalVerbose

	// Debug print for resolved row count
	debugPrint("[DEBUG] finalNumRows: %d\n", finalNumRows)

	// Validate required parameters
	if finalNumRows <= 0 && finalStats == "" {
		fmt.Fprintf(os.Stderr, "Error: Number of rows (-n/--rows) is required and must be > 0 when no stats file is provided\n")
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

		// Get effective number of rows (from stats if available)
		effectiveNumRows := getEffectiveNumRows(generator, finalNumRows, finalMaxRows)
		debugPrint("[DEBUG] effectiveNumRows: %d\n", effectiveNumRows)

		// Generate data
		rows := generator.GenerateData(effectiveNumRows)

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

		// Get effective number of rows (from stats if available)
		effectiveNumRows := getEffectiveNumRows(generator, finalNumRows, finalMaxRows)
		debugPrint("[DEBUG] effectiveNumRows: %d\n", effectiveNumRows)

		if finalInsert {
			// Insert data directly to database
			if finalWorkers == 0 {
				// Auto-tuning mode
				if finalBulkInsert {
					if err := generator.InsertDataToDBBulkParallelAutoTune(config, finalTable, effectiveNumRows); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				} else {
					if err := generator.InsertDataToDBParallelAutoTune(config, finalTable, effectiveNumRows); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				}
			} else if finalWorkers == 1 {
				// Serial processing (1 worker)
				if finalBulkInsert {
					if err := generator.InsertDataToDBBulk(config, finalTable, effectiveNumRows); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				} else {
					if err := generator.InsertDataToDB(config, finalTable, effectiveNumRows); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				}
			} else {
				// Parallel processing (multiple workers)
				if finalBulkInsert {
					if err := generator.InsertDataToDBBulkParallel(config, finalTable, effectiveNumRows, finalWorkers); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				} else {
					if err := generator.InsertDataToDBParallel(config, finalTable, effectiveNumRows, finalWorkers); err != nil {
						log.Fatalf("Failed to insert data: %v", err)
					}
				}
			}
		} else {
			// Generate data and output as JSON
			rows := generator.GenerateData(effectiveNumRows)
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
