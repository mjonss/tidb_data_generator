# TiDB Data Generator

A Golang program that generates data for database tables based on TiDB statistics exported in JSON format.

## Features

- Generates data matching the statistical distribution from TiDB statistics
- Decodes base64-encoded data values from statistics
- Supports various data types: integers, strings, timestamps, datetimes
- Uses histogram buckets for timestamp generation
- Uses frequency distribution for categorical data

## Usage

```bash
go run main.go <stats_file> <num_rows>
```

### Example

```bash
go run main.go t.stats.json 1000
```

This will generate 1000 rows of data based on the statistics in `t.stats.json` and output them as JSON.

## Table Structure

The program is designed for tables with the following structure:

```sql
CREATE TABLE `t` (
  `id` int NOT NULL AUTO_INCREMENT,
  `test_type` varchar(255) DEFAULT NULL,
  `test_category` int DEFAULT NULL,
  `created` timestamp NULL DEFAULT NULL,
  `calendar_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_type_cat` (`test_type`,`test_category`),
  KEY `idx_created` (`created`)
)
```

## Data Generation Strategy

- **id**: Sequential integers starting from 1
- **test_type**: Generated based on frequency distribution from CM sketch (Flip-flops, Glasses, Shorts, Socks)
- **test_category**: Random integers from 1-24 (based on NDV from statistics)
- **created**: Generated using histogram buckets with base64-decoded timestamp ranges
- **calendar_date**: Random datetime values (no histogram data available)

## Statistics Format

The program expects TiDB statistics in JSON format with:
- Column statistics including histograms and CM sketches
- Base64-encoded data values in `lower_bound` and `upper_bound` fields
- Frequency counts for categorical data

## Output

The program outputs generated data as JSON array with each row containing:
- `id`: Integer ID
- `test_type`: String category
- `test_category`: Integer category
- `created`: ISO timestamp string
- `calendar_date`: ISO datetime string (nullable) 