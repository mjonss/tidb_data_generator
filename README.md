# TiDB Data Generator

A Golang program that generates and inserts data for database tables based on TiDB statistics exported in JSON format. Supports both SQL file parsing and direct database table analysis.

## Features

- **Dual Mode Operation**: Generate data from SQL files or analyze existing database tables
- **Statistical Data Generation**: Generates data matching the statistical distribution from TiDB statistics
- **Auto-tuning**: Automatically finds optimal batch sizes and worker counts for maximum insert performance
- **Composite Key Support**: Handles tables with composite primary keys using mixed-radix counting
- **Parallel Processing**: Multi-worker parallel inserts with configurable concurrency
- **Progress Monitoring**: Real-time progress reporting during data insertion
- **Flexible Output**: Generate JSON data or insert directly to database
- **Multiple Data Types**: Supports integers, strings, timestamps, datetimes, dates, floats, booleans
- **Smart Key Generation**: Uses histogram buckets for timestamp generation and frequency distribution for categorical data

## Installation

```bash
go mod tidy
go build -o tidb_data_generator main.go
```

## Usage

### Command Line Options

```bash
./tidb_data_generator [OPTIONS]
```

#### Database Connection Options
- `--host, -H <host>` - Database host (default: 127.0.0.1)
- `--port, -P <port>` - Database port (default: 3306)
- `--user, -u <user>` - Database user (default: root)
- `--password, -p <pass>` - Database password
- `--database, -D <db>` - Database name

#### Data Generation Options
- `--table, -t <table>` - Table name (required for database mode)
- `--sql, -f <file>` - SQL file path (required for file mode)
- `--rows, -n <num>` - Number of rows to generate (required, or use count from stats file)
- `--max-table-rows <num>` - Do not exceed table row count (0=no limit)
- `--stats, -s <file>` - Stats file path (optional)

#### Insert Options
- `--insert, -i` - Insert data directly to database instead of outputting JSON
- `--bulk <size>` - Bulk insert batch size (default: 0=auto-tune, >0=use specific batch size)
- `--workers <num>` - Number of parallel workers (default: 0=auto-tune, 1=serial, >1=parallel)
- `--verbose` - Enable verbose debug output

## Examples

### Generate JSON from SQL file
```bash
# Generate 1000 rows from SQL file
./tidb_data_generator -f t.create.sql -n 1000

# Use statistics for better data distribution
./tidb_data_generator -f t.create.sql -n 1000 -s t.stats.json

# Use row count from statistics file
./tidb_data_generator -f t.create.sql -s t.stats.json
```

### Generate JSON from database table
```bash
# Generate 1000 rows from existing table structure
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000

# Use statistics for better data distribution
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json
```

### Insert data directly to database

#### Auto-tuning (recommended)
```bash
# Auto-tune batch size and worker count for maximum performance
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i
```

#### Manual configuration
```bash
# Serial processing (1 worker)
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --workers 1

# Parallel processing (8 workers)
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --workers 8

# Individual INSERT statements (no bulk)
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk=1

# Specific bulk batch size (5000 rows per batch)
./tidb_data_generator -H 127.0.0.1 -P 4000 -u root -D test -t mytable -n 1000 -s t.stats.json -i --bulk=5000
```

## Table Structure Support

The program supports various table structures including:

### Simple Primary Key
```sql
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
)
```

### Composite Primary Key
```sql
CREATE TABLE `fraud_assessment_heads` (
  `make_order_process_id` bigint NOT NULL,
  `system_id` smallint NOT NULL,
  `status` varchar(50) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`make_order_process_id`, `system_id`)
)
```

### No Primary Key
```sql
CREATE TABLE `logs` (
  `message` text,
  `level` varchar(20),
  `timestamp` datetime DEFAULT NULL
)
```

## Data Generation Strategy

### Primary Key Handling
- **Single-column PK**: Sequential integers starting from next available value
- **Composite PK**: Mixed-radix counting to generate unique combinations
- **No PK**: Random data generation without uniqueness constraints

### Statistical Data Generation
- **Histogram-based**: Uses TiDB histogram buckets for timestamp/datetime generation
- **Frequency-based**: Uses CM sketch TopN data for categorical values
- **Fallback**: Generates synthetic data when statistics are unavailable

### Auto-tuning Process
1. **Batch Size Tuning**: Tests batch sizes 1, 50, 1000, 10000 to find optimal performance
2. **Worker Count Tuning**: Tests 1, 2, 4, 8 workers to find optimal concurrency
3. **Performance Measurement**: Uses actual insert rates to determine best configuration

## Statistics Format

The program expects TiDB statistics in JSON format with:
- Column statistics including histograms and CM sketches
- Base64-encoded data values in `lower_bound` and `upper_bound` fields
- Frequency counts for categorical data in TopN arrays
- NDV (Number of Distinct Values) information

## Output

### JSON Mode
Outputs generated data as JSON array with each row containing the table's column values.

### Database Mode
- Real-time progress reporting during insertion
- Performance statistics (rows/sec) upon completion
- Automatic handling of table constraints and data types

## Performance Features

- **Connection Pooling**: Optimized database connection management
- **Bulk Inserts**: Configurable batch sizes for optimal performance
- **Parallel Processing**: Multi-worker concurrent inserts
- **Memory Efficient**: Streaming data generation without loading all rows into memory
- **Timeout Handling**: Graceful handling of long-running operations

## Error Handling

- **Duplicate Key Prevention**: Smart key generation to avoid conflicts
- **Data Type Validation**: Ensures generated data matches column constraints
- **Connection Recovery**: Robust database connection handling
- **Progress Tracking**: Detailed error reporting with context

## Limitations

- Currently supports MySQL/TiDB databases
- Foreign key constraints are not validated during generation
- Some complex data types may use fallback generation
- Auto-tuning requires sufficient data for accurate benchmarking
