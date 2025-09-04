# Spring-DuckDB-CUR2.0

A Spring Boot application that integrates with DuckDB to analyze AWS Cost and Usage Reports (CUR) 2.0 data. This application provides REST endpoints to query cost data by regions, calculate 30-day costs, and analyze resource utilization.

## Prerequisites

- Java 21 or higher
- Git
- An AWS CUR 2.0 CSV file (should be named `cur2.csv`)

## Getting Started

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd Spring-DuckDB-CUR2.0
```

### 2. Place the CUR2 CSV File

**Important**: You need to place your AWS CUR 2.0 CSV file in the correct location for the application to work:

1. Navigate to `src/main/resources/static/`
2. Place your CSV file and rename it to `cur2.csv`

```
src/main/resources/static/cur2.csv
```

**Note**: The CSV file is already added to `.gitignore` so it won't be committed to the repository.

### 3. Run the Application

#### Option A: Using Gradle Wrapper (Recommended)

```bash
# On Windows
.\gradlew bootRun

# On Unix/macOS
./gradlew bootRun
```

#### Option B: Using Gradle (if installed globally)

```bash
gradle bootRun
```

The application will start on `http://localhost:8080`.

## DuckDB Setup

### How DuckDB is Configured

This application uses DuckDB as an embedded analytical database to process AWS CUR data efficiently. Here's how it's set up:

#### 1. **Connection Management**
- **Single Connection Pattern**: The application maintains one persistent DuckDB connection through `DuckDBConnectionManager`
- **Persistent Database**: DuckDB data is stored in `./duckdb.db` (configured in `application.properties`)
- **Thread-Safe**: The connection manager ensures thread-safe access to the database

#### 2. **Data Loading Strategy**
The application supports two approaches for working with CSV data:

**A. On-the-fly CSV Loading**
- Directly queries the CSV file without loading it into DuckDB tables
- Good for one-time queries or when you don't want to persist data

**B. Persistent Table Loading**
- Loads the CSV into a DuckDB table (`cur2_data`) at application startup
- Faster for multiple queries as data is already in memory
- Automatically creates table schema based on CSV structure

#### 3. **Database Files**
- `duckdb.db` - Main database file (ignored by Git)
- `duckdb.db.wal` - Write-Ahead Log file (ignored by Git)

These files are automatically created in the project root when you first run the application.

## Available Endpoints

The application provides several REST endpoints for querying CUR data:

### Health Check
```
GET /duckdb/health
```
Verifies if DuckDB connection is healthy.

### Data Query Endpoints

#### Get All CSV Data (On-the-fly)
```
GET /duckdb/csv
```
Returns up to 500 rows from the CSV file (loads CSV on-demand).

#### Get All Persistent Table Data
```
GET /duckdb/persistent
```
Returns up to 500 rows from the persistent DuckDB table.

#### Get Data by Region
```
GET /duckdb/region/{regionCode}
```
Example: `GET /duckdb/region/us-east-2`

Returns all rows for a specific AWS region.

#### Get 30-Day Costs for US-East-2 Region
```
GET /duckdb/costs/region/us-east-2
```
Returns 30-day cost breakdown for all resources in the us-east-2 region.

#### Get Total Cost Summary for US-East-2 Region
```
GET /duckdb/costs/region/us-east-2/total
```
Returns aggregated cost summary between January 1, 2023, and today for us-east-2 region.

Response format:
```json
{
  "accounts": 1,
  "complianceIssueCount": 1213,
  "resources": 2446,
  "totalCost": {
    "USD": 1313.15
  }
}
```

#### Get Resource Cost and Discount Information
```
GET /duckdb/resource/cost-discount-info
```
Returns cost and Reserved Instance (RI) discount information for all resources.
I am still working on this endpoint to enhance its functionality. This is not fully working yet.

## Building the Application

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test
```

## Troubleshooting

### Common Issues

1. **"Extension not found" error**: 
   - This typically occurs with DuckDB extension loading
   - The application is configured to use a persistent database which should resolve this

2. **"Connection was closed" error**:
   - Ensure the DuckDB connection manager is properly initialized
   - Check that the database file path is accessible

3. **CSV file not found**:
   - Verify the `cur2.csv` file is placed in `src/main/resources/static/`
   - Check file permissions and formatting

4. **No data returned from endpoints**:
   - Ensure the CSV file contains data in the expected format
   - Check application logs for detailed error messages
