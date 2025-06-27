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
	"sync"
	"time"

	"flag"

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
	Size     int // Maximum size/length for string columns
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
	Count   uint                   `json:"count"`
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

	seed := time.Now().UnixNano()

	log.Printf("Using seed: %d", seed)

	generator := &DataGenerator{
		tableDef:         tableDef,
		stats:            stats,
		rand:             rand.New(rand.NewSource(seed)),
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

	// TODO: handle FOREIGN KEYS
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
	// Extract data type and size
	typeRegex := regexp.MustCompile(`(\w+)(?:\((\d+)\))?`)
	typeMatch := typeRegex.FindStringSubmatch(definition)
	dataType := "varchar"
	size := 0
	if len(typeMatch) >= 2 {
		dataType = strings.ToLower(typeMatch[1])
		if len(typeMatch) >= 3 && typeMatch[2] != "" {
			if sizeVal, err := strconv.Atoi(typeMatch[2]); err == nil {
				size = sizeVal
			}
		}
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
		Size:     size,
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
	case "year":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			// MySQL YEAR is 1901-2155, but for realism use 2000-2030
			year := 2000 + dg.rand.Intn(31)
			return year
		}
	case "varchar", "char", "text":
		return func() interface{} {
			if dg.rand.Float32() < float32(nullProbability) {
				return nil
			}
			// Try to use stats if available
			if dg.stats != nil {
				if _, exists := dg.stats.Columns[column.Name]; exists {
					return dg.generateStringFromStats(column)
				}
			}
			// Fallback to random string - respect column size
			fallbackValue := fmt.Sprintf("value_%d", dg.rand.Intn(1000))
			if column.Size > 0 && len(fallbackValue) > column.Size {
				fallbackValue = fallbackValue[:column.Size]
			}
			return fallbackValue
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

func (dg *DataGenerator) generateStringFromStats(column ColumnDef) string {
	columnStats := dg.stats.Columns[column.Name]
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
					// Truncate to column size if specified
					if column.Size > 0 && len(value) > column.Size {
						value = value[:column.Size]
					}
					return value
				}
				break
			}
		}
	}

	// Fallback - generate string within size limit
	fallbackValue := fmt.Sprintf("generated_%s_%d", column.Name, dg.rand.Intn(1000))
	if column.Size > 0 && len(fallbackValue) > column.Size {
		fallbackValue = fallbackValue[:column.Size]
	}
	return fallbackValue
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

// Helper: base36 encoding for compact unique strings
func base36(n int) string {
	const chars = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return "0"
	}
	res := ""
	for n > 0 {
		res = string(chars[n%36]) + res
		n /= 36
	}
	return res
}

// Helper: get min/max for MySQL integer types
func getIntTypeRange(typ string) (min, max int64) {
	baseType := typ
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = baseType[:idx]
	}
	switch strings.ToLower(baseType) {
	case "tinyint":
		return -128, 127
	case "smallint":
		return -32768, 32767
	case "mediumint":
		return -8388608, 8388607
	case "int", "integer":
		return -2147483648, 2147483647
	case "bigint":
		return -9223372036854775808, 9223372036854775807
	default:
		return -2147483648, 2147483647 // default to int
	}
}

// Helper: generate unique values for composite key columns using mixed-radix counting
func generateCompositeKeyValues(id int, colTypes []string, colMins, colRanges []int64) []int64 {
	vals := make([]int64, len(colTypes))
	remainder := int64(id)
	for i := len(colTypes) - 1; i >= 0; i-- {
		if colRanges[i] > 0 {
			vals[i] = colMins[i] + (remainder % colRanges[i])
			remainder = remainder / colRanges[i]
		} else {
			vals[i] = colMins[i]
		}
	}
	return vals
}

func (dg *DataGenerator) GenerateRow(id int) TableRow {
	row := make(TableRow)

	// Identify all unique/primary key columns
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)

	usedCols := map[string]bool{}

	for _, colSet := range keyCols {
		if len(colSet) == 1 {
			colName := colSet[0]
			colType := ""
			colSize := 0
			for _, c := range dg.tableDef.Columns {
				if c.Name == colName {
					colType = c.Type
					colSize = c.Size
					break
				}
			}
			if strings.Contains(colType, "int") {
				min, max := getIntTypeRange(colType)
				val := int64(id)
				if max > min {
					rangeSize := max - min + 1
					if rangeSize > 0 {
						val = min + (val % rangeSize)
					}
				}
				row[colName] = val
			} else if isStringType(colType) {
				val := base36(id)
				if colSize > 0 && len(val) > colSize {
					val = val[:colSize]
				}
				row[colName] = val
			}
			usedCols[colName] = true
		} else if len(colSet) > 1 {
			// Mixed-radix for composite keys
			colTypes := make([]string, len(colSet))
			colMins := make([]int64, len(colSet))
			colRanges := make([]int64, len(colSet))
			colSizes := make([]int, len(colSet))
			for i, colName := range colSet {
				for _, c := range dg.tableDef.Columns {
					if c.Name == colName {
						colTypes[i] = c.Type
						colSizes[i] = c.Size
						if strings.Contains(c.Type, "int") {
							min, max := getIntTypeRange(c.Type)
							colMins[i] = min
							colRanges[i] = max - min + 1
						} else {
							colMins[i] = 0
							colRanges[i] = 0
						}
						break
					}
				}
			}
			vals := generateCompositeKeyValues(id, colTypes, colMins, colRanges)
			for i, colName := range colSet {
				if strings.Contains(colTypes[i], "int") {
					row[colName] = vals[i]
				} else if isStringType(colTypes[i]) {
					val := base36(int(vals[i]))
					if colSizes[i] > 0 && len(val) > colSizes[i] {
						val = val[:colSizes[i]]
					}
					row[colName] = val
				} else {
					row[colName] = vals[i]
				}
				usedCols[colName] = true
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

// Helper: extract representative values for a column from stats
func (dg *DataGenerator) getColumnValueSet(colName string, max uint) []interface{} {
	if dg.stats == nil {
		return nil
	}
	colStats, ok := dg.stats.Columns[colName]
	if !ok {
		return nil
	}
	values := []interface{}{}
	// Use TopN if available
	if colStats.CMSketch != nil && len(colStats.CMSketch.TopN) > 0 {
		for _, item := range colStats.CMSketch.TopN {
			decoded, err := base64.StdEncoding.DecodeString(item.Data)
			if err == nil && len(decoded) > 1 {
				val := string(decoded[1:])
				val = strings.ReplaceAll(val, "\x00", "")
				val = strings.TrimSpace(val)
				val = strings.ToValidUTF8(val, "")
				values = append(values, val)
				if len(values) >= int(max) {
					return values
				}
			}
		}
	}
	// Use histogram buckets if available
	if colStats.Histogram != nil && len(colStats.Histogram.Buckets) > 0 {
		for _, bucket := range colStats.Histogram.Buckets {
			// Use lower_bound as a representative value
			decoded, err := base64.StdEncoding.DecodeString(bucket.LowerBound)
			if err == nil {
				val := string(decoded)
				val = strings.ReplaceAll(val, "\x00", "")
				val = strings.TrimSpace(val)
				val = strings.ToValidUTF8(val, "")
				values = append(values, val)
				if len(values) >= int(max) {
					return values
				}
			}
		}
	}
	// Use NDV to generate synthetic values if needed
	if colStats.Histogram != nil && colStats.Histogram.NDV > 0 && len(values) < int(max) {
		for i := len(values); i < int(max) && i < colStats.Histogram.NDV; i++ {
			values = append(values, fmt.Sprintf("%s_%d", colName, i))
		}
	}
	return values
}

// Generate unique combinations for composite keys using stats
func (dg *DataGenerator) generateCompositeKeyCombinations(keyCols []string, numRows uint) [][]interface{} {
	valueSets := make([][]interface{}, len(keyCols))
	for i, col := range keyCols {
		valueSets[i] = dg.getColumnValueSet(col, numRows)
		if len(valueSets[i]) == 0 {
			// Fallback: generate synthetic values
			for j := uint(0); j < numRows; j++ {
				valueSets[i] = append(valueSets[i], fmt.Sprintf("%s_%d", col, j))
			}
		}
	}
	// Generate cartesian product up to numRows
	combos := [][]interface{}{}
	var generate func(idx int, current []interface{})
	generate = func(idx int, current []interface{}) {
		if len(combos) >= int(numRows) {
			return
		}
		if idx == len(valueSets) {
			combo := make([]interface{}, len(current))
			copy(combo, current)
			combos = append(combos, combo)
			return
		}
		for _, v := range valueSets[idx] {
			generate(idx+1, append(current, v))
		}
	}
	generate(0, []interface{}{})
	return combos
}

// Update GenerateData to use composite key combos from stats
func (dg *DataGenerator) GenerateData(numRows uint) []TableRow {
	rows := make([]TableRow, 0, numRows)
	uniqueSets := make([]map[string]struct{}, 0)
	keyCols := make([][]string, 0)
	if len(dg.tableDef.PrimaryKey) > 0 {
		keyCols = append(keyCols, dg.tableDef.PrimaryKey)
	}
	keyCols = append(keyCols, dg.tableDef.UniqueKeys...)
	for range keyCols {
		uniqueSets = append(uniqueSets, make(map[string]struct{}))
	}

	// Precompute composite key combos if possible
	compositeCombos := map[string][][]interface{}{}
	for _, colSet := range keyCols {
		if len(colSet) > 1 {
			combos := dg.generateCompositeKeyCombinations(colSet, numRows)
			compositeCombos[strings.Join(colSet, ",")] = combos
		}
	}

	nextInt := 1
	comboIdx := map[string]uint{}
	for len(rows) < int(numRows) {
		row := make(TableRow)
		for _, colSet := range keyCols {
			if len(colSet) == 1 {
				colName := colSet[0]
				colType := ""
				colSize := 0
				for _, c := range dg.tableDef.Columns {
					if c.Name == colName {
						colType = c.Type
						colSize = c.Size
						break
					}
				}
				if strings.Contains(colType, "int") {
					row[colName] = nextInt
				} else if isStringType(colType) {
					var uniqueValue string
					if colSize > 0 && colSize < 5 {
						uniqueValue = fmt.Sprintf("%c%d", 'A'+(nextInt%26), nextInt)
						if len(uniqueValue) > colSize {
							uniqueValue = uniqueValue[:colSize]
						}
					} else if colSize > 0 && colSize < 10 {
						uniqueValue = fmt.Sprintf("%d", nextInt)
						if len(uniqueValue) > colSize {
							uniqueValue = uniqueValue[:colSize]
						}
					} else if colSize > 0 && colSize < 20 {
						prefix := colName[:min(3, len(colName))]
						uniqueValue = fmt.Sprintf("%s%d", prefix, nextInt)
						if len(uniqueValue) > colSize {
							uniqueValue = uniqueValue[:colSize]
						}
					} else {
						uniqueValue = fmt.Sprintf("%s_%d", colName, nextInt)
						if colSize > 0 && len(uniqueValue) > colSize {
							uniqueValue = uniqueValue[:colSize]
						}
					}
					row[colName] = uniqueValue
				}
			} else if len(colSet) > 1 {
				key := strings.Join(colSet, ",")
				combos := compositeCombos[key]
				idx := comboIdx[key]
				if idx >= uint(len(combos)) {
					continue // exhausted combos
				}
				for i, colName := range colSet {
					row[colName] = combos[idx][i]
				}
				comboIdx[key] = idx + 1
			}
		}
		for _, column := range dg.tableDef.Columns {
			if _, ok := row[column.Name]; ok {
				continue
			}
			generator := dg.columnGenerators[column.Name]
			row[column.Name] = generator()
		}
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
			Size:     0, // Default to 0 for non-string columns
			Nullable: isNullable == "YES",
			Default:  defaultValue.String,
			Extra:    extra,
		}
		// Set size for string columns
		if charLength.Valid {
			column.Size = int(charLength.Int64)
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

// Get the next available values for composite primary keys
func getNextAvailableCompositeKey(config DBConfig, tableDef *TableDef) (map[string]int, error) {
	if len(tableDef.PrimaryKey) == 0 {
		return nil, nil
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Set session time zone to UTC
	_, err = db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return nil, fmt.Errorf("failed to set session time_zone: %w", err)
	}

	// Build query to get max values for all primary key columns
	maxQueries := make([]string, len(tableDef.PrimaryKey))
	for i, pkCol := range tableDef.PrimaryKey {
		maxQueries[i] = fmt.Sprintf("MAX(`%s`)", pkCol)
	}
	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(maxQueries, ", "), tableDef.Name)

	// Execute query
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query max values: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned from max query")
	}

	// Scan results
	values := make([]sql.NullInt64, len(tableDef.PrimaryKey))
	valuePtrs := make([]interface{}, len(tableDef.PrimaryKey))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan max values: %w", err)
	}

	// Build result map
	result := make(map[string]int)
	for i, pkCol := range tableDef.PrimaryKey {
		if values[i].Valid {
			result[pkCol] = int(values[i].Int64) + 1
		} else {
			result[pkCol] = 1
		}
	}

	return result, nil
}

// Get the next available value for a single-column integer primary key
func getNextAvailableID(config DBConfig, tableDef *TableDef) (int, error) {
	if len(tableDef.PrimaryKey) != 1 {
		// For composite keys, return 1 and let the composite key logic handle it
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
		return 1, err
	}
	debugPrint("[DEBUG] getNextAvailableID: maxID.Valid=%v, maxID.Int64=%d\n", maxID.Valid, maxID.Int64)
	if maxID.Valid {
		return int(maxID.Int64) + 1, nil
	}
	return 1, nil
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

// Get table count
func getTableCount(config DBConfig, table string) (uint, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Try to get the maximum PK value from the table
	query := fmt.Sprintf("SELECT count(*) FROM %s", table)
	var count uint
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Helper function to get effective number of rows
func getEffectiveNumRows(generator *DataGenerator, commandLineRows uint, maxRows uint, tableCount uint) uint {
	if generator.stats != nil && generator.stats.Count > 0 {
		commandLineRows = uint(generator.stats.Count)
	}

	if commandLineRows > 0 {
		if maxRows > 0 {
			if tableCount > 0 {
				if tableCount >= maxRows {
					return 0
				}
				return maxRows - tableCount
			}
			if commandLineRows > maxRows {
				return maxRows
			}
		}
		return commandLineRows
	}

	// Fallback
	return 1000
}

// buildInsertStatement builds a single INSERT statement for a batch of rows
func (dg *DataGenerator) buildInsertStatement(
	tableName string,
	startRow, endRow uint,
	nextID int,
	nextCompositeKeys map[string]int,
) (string, []interface{}) {
	// Build INSERT statement - skip auto-increment columns
	columnNames := make([]string, 0, len(dg.tableDef.Columns))
	placeholders := make([]string, 0, len(dg.tableDef.Columns))
	for _, column := range dg.tableDef.Columns {
		if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", column.Name))
		placeholders = append(placeholders, "?")
	}

	// Build bulk INSERT statement for this batch
	valueGroups := make([]string, 0, endRow-startRow)
	allValues := make([]interface{}, 0, int(endRow-startRow)*len(placeholders))

	for rowID := int(startRow); rowID < int(endRow); rowID++ {
		var row TableRow
		if len(dg.tableDef.PrimaryKey) == 1 {
			row = dg.GenerateRow(nextID + rowID)
		} else if len(dg.tableDef.PrimaryKey) > 1 {
			row = dg.GenerateRowWithCompositeKey(nextID+rowID, nextCompositeKeys)
		} else {
			row = dg.GenerateRow(nextID + rowID)
		}

		valueGroups = append(valueGroups, fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))

		// Convert row to interface slice for query - skip auto-increment columns
		for _, column := range dg.tableDef.Columns {
			if strings.Contains(strings.ToLower(column.Extra), "auto_increment") {
				continue
			}
			allValues = append(allValues, row[column.Name])
		}
	}
	debugPrint("[DEBUG] buildInsertStatement: valueGroups: %d\n", len(valueGroups))
	debugPrint("[DEBUG] buildInsertStatement: allValues: %d\n", len(allValues))

	bulkInsertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(valueGroups, ", "))

	debugPrint("[DEBUG] buildInsertStatement: bulkInsertSQL: %d\n", len(bulkInsertSQL))
	return bulkInsertSQL, allValues
}

func progressMonitor(rowsToInsert, maxRows uint) chan uint {
	updateCompletedRows := make(chan uint, 10)

	// Start progress monitoring goroutine
	go func() {
		progressTicker := time.NewTicker(1 * time.Second)
		defer progressTicker.Stop()
		lastReportedCount := uint(0)
		completedRows := maxRows - rowsToInsert
		startTime := time.Now()
		lastTime := startTime
		for {
			select {
			case <-progressTicker.C:
				elapsed := time.Since(startTime)
				rateTotal := float64(completedRows) / elapsed.Seconds()
				now := time.Now()
				rateNow := float64(completedRows-lastReportedCount) / now.Sub(lastTime).Seconds()
				fmt.Printf("\rCompleted %d rows... (%.0f rows/sec, tot %.0f rows/sec %d remaining)", completedRows, rateNow, rateTotal, maxRows-completedRows)
				lastTime = now
				lastReportedCount = completedRows
			case addCount, ok := <-updateCompletedRows:
				if !ok {
					fmt.Printf("\n")
					return
				}
				completedRows += addCount
			}
		}
	}()
	return updateCompletedRows
}

// Generate row with composite key handling
func (dg *DataGenerator) GenerateRowWithCompositeKey(id int, nextCompositeKeys map[string]int) TableRow {
	row := make(TableRow)

	// Set composite primary key values
	if len(dg.tableDef.PrimaryKey) > 1 {
		// For composite keys, we need to generate unique combinations
		// Use the id to generate different combinations
		for i, pkCol := range dg.tableDef.PrimaryKey {
			var colType string
			var colSize int
			for _, c := range dg.tableDef.Columns {
				if c.Name == pkCol {
					colType = c.Type
					colSize = c.Size
					break
				}
			}

			if strings.Contains(colType, "int") {
				min, max := getIntTypeRange(colType)
				// Use the next available value plus offset for uniqueness
				baseValue := int64(nextCompositeKeys[pkCol])
				val := baseValue + int64(id) + int64(i*1000000) // Add offset to ensure uniqueness
				if max > min {
					rangeSize := max - min + 1
					if rangeSize > 0 {
						val = min + (val % rangeSize)
					}
				}
				row[pkCol] = val
			} else if isStringType(colType) {
				val := base36(id + i*1000000)
				if colSize > 0 && len(val) > colSize {
					val = val[:colSize]
				}
				row[pkCol] = val
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

// Find optimal batch size by testing different batch sizes
func (dg *DataGenerator) findOptimalBatchSize(config DBConfig, tableName string, rows uint) (uint, uint) {
	log.Printf("Testing batch sizes...\n")

	batchSizes := []uint{1, 50, 1000, 10000}
	//batchSizes := []uint{1, 5, 10, 20, 50, 100, 500, 1000, 5000, 10000}

	bestBatchSize := uint(1)
	bestPerformance := 0.0

	for _, batchSize := range batchSizes {
		log.Printf("batch size: %d\n", batchSize)

		if batchSize > rows {
			batchSize = rows
			log.Printf("Decreased last batch size: %d\n", batchSize)
		}
		if batchSize == 0 {
			continue
		}
		// Get the next available values for primary keys
		var nextID int
		var nextCompositeKeys map[string]int
		var err error

		if len(dg.tableDef.PrimaryKey) == 1 {
			nextID, err = getNextAvailableID(config, dg.tableDef)
			if err != nil {
				continue
			}
		} else if len(dg.tableDef.PrimaryKey) > 1 {
			nextCompositeKeys, err = getNextAvailableCompositeKey(config, dg.tableDef)
			if err != nil {
				continue
			}
			nextID = 1000000 // Use high ID to avoid conflicts
		} else {
			nextID = 1000000 // Use high ID to avoid conflicts
		}

		// Use a high start ID to avoid conflicts with existing data
		startID := nextID + 1000000

		options := InsertOptions{
			BenchmarkMode: true,
			MaxDuration:   2 * time.Second,
			Verbose:       verboseMode,
		}

		performance, rowsDone, err := dg.InsertData(config, tableName, rows, batchSize, 1, startID, nextCompositeKeys, options)
		if err != nil {
			log.Printf("Failed to insert data: %v", err)
			continue
		}

		rows -= rowsDone
		log.Printf("Batch size %d: %.0f rows/sec (this run: %d, remaining: %d)\n", batchSize, performance, rowsDone, rows)
		if performance > bestPerformance {
			bestPerformance = performance
			bestBatchSize = batchSize
		}
	}

	return bestBatchSize, rows
}

// Find optimal worker count with a given batch size
func (dg *DataGenerator) findOptimalWorkerCount(config DBConfig, tableName string, batchSize uint, rows uint) (uint, uint) {
	log.Printf("Testing worker counts with batch size %d and %d rows...\n", batchSize, rows)

	maxWorkers := uint(8)
	bestWorkers := uint(1)
	bestPerformance := 0.0

	for workers := uint(1); workers <= maxWorkers; workers *= 2 {
		if rows == 0 {
			break
		}
		// Get the next available values for primary keys
		var nextID int
		var nextCompositeKeys map[string]int
		var err error

		if len(dg.tableDef.PrimaryKey) == 1 {
			nextID, err = getNextAvailableID(config, dg.tableDef)
			if err != nil {
				continue
			}
		} else if len(dg.tableDef.PrimaryKey) > 1 {
			nextCompositeKeys, err = getNextAvailableCompositeKey(config, dg.tableDef)
			if err != nil {
				continue
			}
			nextID = 1000000 // Use high ID to avoid conflicts
		} else {
			nextID = 1000000 // Use high ID to avoid conflicts
		}

		// Use a high start ID to avoid conflicts with existing data
		startID := nextID + 1000000

		options := InsertOptions{
			BenchmarkMode: true,
			MaxDuration:   2 * time.Second,
			Verbose:       verboseMode,
		}

		performance, rowsDone, err := dg.InsertData(config, tableName, rows, batchSize, workers, startID, nextCompositeKeys, options)
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}
		rows -= rowsDone

		log.Printf("  %d workers: %.0f rows/sec (this run: %d, remaining: %d)\n", workers, performance, rowsDone, rows)
		if performance > bestPerformance {
			bestPerformance = performance
			bestWorkers = workers
		} else if workers > 1 && performance < bestPerformance*0.8 {
			break
		}
	}

	return bestWorkers, rows
}

// InsertOptions controls the behavior of InsertData
type InsertOptions struct {
	BenchmarkMode bool          // If true, run in benchmark mode with time limits
	MaxDuration   time.Duration // Maximum time to run (for benchmark mode)
	MaxRows       uint          // Maximum number of rows to insert
	Verbose       bool          // Enable verbose output
}

type Jobs struct {
	StartRow uint
	EndRow   uint
}

// InsertData is a unified function that handles both real insertion and benchmarking
func (dg *DataGenerator) InsertData(
	config DBConfig,
	tableName string,
	numRows uint,
	batchSize uint,
	numWorkers uint,
	nextID int,
	nextCompositeKeys map[string]int,
	options InsertOptions,
) (float64, uint, error) {
	startTime := time.Now()

	log.Printf("[DEBUG] InsertData: starting with %d rows, batchSize=%d, workers=%d, options=%#v\n",
		numRows, batchSize, numWorkers, options)
	debugPrint("[DEBUG] InsertData: starting with %d rows, batchSize=%d, workers=%d, options=%#v\n",
		numRows, batchSize, numWorkers, options)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&interpolateParams=false",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// Create channels for coordination
	jobs := make(chan Jobs, 10)
	results := make(chan error, numWorkers)
	var progressChan chan uint
	var wg sync.WaitGroup

	if options.BenchmarkMode {
		progressChan = make(chan uint, numWorkers)
	} else {
		progressChan = progressMonitor(numRows, options.MaxRows)
	}

	// Start worker goroutines
	if numWorkers < 1 {
		numWorkers = 1
	}
	for w := uint(0); w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID uint) {
			defer wg.Done()

			// Each worker gets its own database connection
			workerDB, err := sql.Open("mysql", dsn)
			if err != nil {
				results <- fmt.Errorf("worker %d failed to connect: %w", workerID, err)
				return
			}
			defer workerDB.Close()

			// Configure connection pool for parallel operations
			workerDB.SetMaxOpenConns(2)
			workerDB.SetMaxIdleConns(1)
			workerDB.SetConnMaxLifetime(time.Hour)

			// Process jobs
			for job := range jobs {
				// Build bulk INSERT statement for this batch
				bulkInsertSQL, allValues := dg.buildInsertStatement(tableName, job.StartRow, job.EndRow, nextID, nextCompositeKeys)

				// Execute bulk insert
				_, err = workerDB.Exec(bulkInsertSQL, allValues...)
				if err != nil {
					results <- fmt.Errorf("worker %d failed to execute bulk insert: %w len all values: %d startRow: %d endRow: %d len sql %d", workerID, err, len(allValues), job.StartRow, job.EndRow, len(bulkInsertSQL))
					return
				}

				// Send progress update
				progressChan <- job.EndRow - job.StartRow
				if options.MaxDuration > 0 && time.Since(startTime) > options.MaxDuration {
					break
				}
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for i := uint(0); i < numRows; i += batchSize {
			endRow := i + batchSize
			if endRow > numRows {
				endRow = numRows
			}
			jobs <- Jobs{StartRow: i, EndRow: endRow}
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
		close(progressChan)
	}()

	// Monitor progress and collect errors
	completedRows := uint(0)

	// Collect progress updates and errors
	for {
		select {
		case rowsInserted, ok := <-progressChan:
			if !ok {
				// Progress channel closed, check for errors
				for err := range results {
					if err != nil {
						return 0.0, completedRows, fmt.Errorf("parallel insert failed: %w", err)
					}
				}
				goto finished
			}
			// Since workers send incremental counts, we just add them
			completedRows += rowsInserted
			if completedRows > numRows {
				completedRows = numRows
			}
		case err := <-results:
			if err != nil {
				return 0.0, completedRows, fmt.Errorf("parallel insert failed: %w", err)
			}
		}
	}

finished:
	totalTime := time.Since(startTime)
	totalRate := float64(completedRows) / totalTime.Seconds()

	if !options.BenchmarkMode {
		log.Printf("Successfully inserted %d rows into table %s in %.2fs (%.0f rows/sec)\n",
			completedRows, tableName, totalTime.Seconds(), totalRate)
	}

	return totalRate, completedRows, nil
}

func main() {
	// Define command-line flags
	var (
		// Database connection flags
		host     = flag.String("host", "127.0.0.1", "Database host")
		port     = flag.Int("port", 3306, "Database port")
		user     = flag.String("user", "root", "Database user")
		password = flag.String("password", "", "Database password")
		database = flag.String("database", "", "Database name")

		// Short versions
		hostShort     = flag.String("H", "127.0.0.1", "Database host (short)")
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
		numRows      = flag.Uint("rows", 0, "Number of rows to generate")
		numRowsShort = flag.Uint("n", 0, "Number of rows to generate (short)")
		maxRows      = flag.Uint("max-table-rows", 0, "Do not exceed table row count (0=no limit)")
		insert       = flag.Bool("insert", false, "Insert data directly to database instead of outputting JSON")
		insertShort  = flag.Bool("i", false, "Insert data directly to database (short)")
		bulkInsert   = flag.Uint("bulk", 0, "Bulk insert batch size (default: 0=auto-tune, >0=use specific batch size)")
		workers      = flag.Uint("workers", 0, "Number of parallel workers (default: 0=auto-tune, 1=serial, >1=parallel)")

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
		fmt.Fprintf(os.Stderr, "    --host, -H <host>        Database host (default: 127.0.0.1)\n")
		fmt.Fprintf(os.Stderr, "    --port, -P <port>        Database port (default: 3306)\n")
		fmt.Fprintf(os.Stderr, "    --user, -u <user>        Database user (default: root)\n")
		fmt.Fprintf(os.Stderr, "    --password, -p <pass>    Database password\n")
		fmt.Fprintf(os.Stderr, "    --database, -D <db>      Database name\n")
		fmt.Fprintf(os.Stderr, "  Data Generation:\n")
		fmt.Fprintf(os.Stderr, "    --table, -t <table>      Table name (required for database mode)\n")
		fmt.Fprintf(os.Stderr, "    --sql, -f <file>         SQL file path (required for file mode)\n")
		fmt.Fprintf(os.Stderr, "    --rows, -n <num>         Number of rows to generate (required, or use count from stats file)\n")
		fmt.Fprintf(os.Stderr, "    --max-table-rows <num>   Do not exceed table row count (0=no limit)\n")
		fmt.Fprintf(os.Stderr, "    --stats, -s <file>       Stats file path (optional)\n")
		fmt.Fprintf(os.Stderr, "    --insert, -i             Insert data directly to database\n")
		fmt.Fprintf(os.Stderr, "    --bulk <size>            Bulk insert batch size (default: 0=auto-tune, >0=use specific batch size)\n")
		fmt.Fprintf(os.Stderr, "    --workers <num>          Number of parallel workers (default: 0=auto-tune, 1=serial, >1=parallel)\n")
		fmt.Fprintf(os.Stderr, "    --verbose                Enable verbose debug output\n")
		fmt.Fprintf(os.Stderr, "    --help, -h               Show this help message\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from SQL file\n")
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -f t.create.sql -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Generate JSON from database\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data directly to database (auto-tune batch size and workers)\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using serial processing (1 worker)\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --workers 1\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using parallel workers (8 workers)\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --workers 8\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using individual INSERT statements (no bulk)\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk=1\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  \n")
		fmt.Fprintf(os.Stderr, "  # Insert data using specific bulk batch size (5000 rows per batch)\n")
		fmt.Fprintf(os.Stderr, "  %s -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk=5000\n", os.Args[0])
	}

	flag.Parse()

	// Check for help
	if *help || *helpShort {
		flag.Usage()
		os.Exit(0)
	}

	// Resolve conflicting flags (long form takes precedence)
	finalHost := *host
	if *hostShort != "127.0.0.1" {
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
		if finalTable != "" || finalHost != "127.0.0.1" || finalPort != 3306 || finalUser != "root" || finalPassword != "" || finalDatabase != "" {
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
		effectiveNumRows := getEffectiveNumRows(generator, finalNumRows, finalMaxRows, 0)
		debugPrint("[DEBUG] effectiveNumRows: %d\n", effectiveNumRows)

		// Generate data
		rows := generator.GenerateData(effectiveNumRows)

		// Output as JSON
		output, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal data: %v", err)
		}

		fmt.Println(string(output))
		return

	}
	if finalTable != "" && finalDatabase != "" {
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

		tableCount, err := getTableCount(config, finalTable)
		if err != nil {
			log.Fatalf("Failed to get table count: %v", err)
		}
		debugPrint("[DEBUG] tableCount: %d\n", tableCount)

		// Get effective number of rows (from stats if available)
		effectiveNumRows := getEffectiveNumRows(generator, finalNumRows, finalMaxRows, tableCount)
		log.Printf("rows to insert: %d\n", effectiveNumRows)
		if effectiveNumRows == 0 {
			log.Printf("No rows to insert, table %s has %d rows\n", finalTable, tableCount)
			return
		}

		if !finalInsert {
			// Generate data and output as JSON
			rows := generator.GenerateData(effectiveNumRows)
			output, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				log.Fatalf("Failed to marshal data: %v", err)
			}
			fmt.Println(string(output))
			return
		}

		// Insert data directly to database
		// Find optimal batch size if not specified
		batchSize := finalBulkInsert
		rowsToInsert := effectiveNumRows
		if batchSize == 0 {
			batchSize, effectiveNumRows = generator.findOptimalBatchSize(config, finalTable, effectiveNumRows)
			log.Printf("Optimal batch size: %d rows (rows done: %d, remaining: %d)\n", batchSize, rowsToInsert-effectiveNumRows, effectiveNumRows)
		}
		workers := finalWorkers
		if workers == 0 {
			workers, effectiveNumRows = generator.findOptimalWorkerCount(config, finalTable, batchSize, effectiveNumRows)
			log.Printf("Optimal worker count: %d (rows done: %d, remaining: %d)\n", workers, rowsToInsert-effectiveNumRows, effectiveNumRows)
		}

		// Get the next available values for primary keys
		var nextID int
		var nextCompositeKeys map[string]int

		if len(tableDef.PrimaryKey) == 1 {
			nextID, err = getNextAvailableID(config, tableDef)
			if err != nil {
				log.Fatalf("Failed to get next available ID: %v", err)
			}
			log.Printf("Starting with ID: %d\n", nextID)
		} else if len(tableDef.PrimaryKey) > 1 {
			nextCompositeKeys, err = getNextAvailableCompositeKey(config, tableDef)
			if err != nil {
				log.Fatalf("Failed to get next available composite key: %v", err)
			}
			log.Printf("Starting with composite keys: %v\n", nextCompositeKeys)
			nextID = 1
		} else {
			nextID = 1
			log.Printf("No primary key found, starting from ID: %d\n", nextID)
		}

		_, _, err = generator.InsertData(config, finalTable, effectiveNumRows, batchSize, workers,
			nextID, nextCompositeKeys,
			InsertOptions{
				BenchmarkMode: false,
				Verbose:       finalVerbose,
				MaxRows:       rowsToInsert,
			})
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}
		return
	}

	// Invalid mode
	fmt.Fprintf(os.Stderr, "Error: Must specify either SQL file (-f/--sql) or database table (-t/--table) with database (-D/--database)\n")
	flag.Usage()
	os.Exit(1)
}
