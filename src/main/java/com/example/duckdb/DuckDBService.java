package com.example.duckdb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class DuckDBService {
    private static final Logger logger = LoggerFactory.getLogger(DuckDBService.class);
    private final DuckDBConnectionManager connectionManager;

    @Autowired
    public DuckDBService(DuckDBConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        try {
            logger.info("Loading CSV into persistent DuckDB table on service instantiation.");
            loadCsvToPersistentTable();
            logger.info("CSV loaded into persistent table successfully.");
        } catch (Exception e) {
            logger.error("Failed to load CSV into persistent table on service instantiation.", e);
        }
    }

    public ResultSet runQuery(String sql) throws SQLException {
        Connection conn = connectionManager.getConnection();
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    public boolean isHealthy() {
        try {
            Connection conn = connectionManager.getConnection();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            logger.info("DuckDB health check passed.");
            return true;
        } catch (SQLException e) {
            logger.error("DuckDB health check failed.", e);
            return false;
        }
    }

    public List<Map<String, Object>> getAllRowsFromCsv(String csvPath) {
        logger.info("Querying up to 500 rows from CSV at path: {}", csvPath);
        List<Map<String, Object>> rows = new ArrayList<>();
        String sql = "SELECT * FROM read_csv_auto('" + csvPath.replace("'", "''") + "') LIMIT 500";
        Connection conn = connectionManager.getConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                }
                rows.add(row);
            }
            logger.info("CSV query returned {} rows.", rows.size());
        } catch (SQLException e) {
            logger.error("Error querying CSV file at path: {}", csvPath, e);
            return rows;
        }
        return rows;
    }

    public List<Map<String, Object>> getAllRowsFromInternalCsv() {
        String csvPath;
        try {
            ClassPathResource resource = new ClassPathResource("static/cur2.csv");
            csvPath = resource.getFile().getAbsolutePath();
        } catch (Exception e) {
            logger.error("Internal CSV file not found or inaccessible.", e);
            return new ArrayList<>();
        }
        return getAllRowsFromCsv(csvPath);
    }

    /**
     * Loads the internal CSV file into a persistent DuckDB table named 'cur2'.
     * Safe to call multiple times.
     */
    public void loadCsvToPersistentTable() {
        String csvPath;
        try {
            ClassPathResource resource = new ClassPathResource("static/cur2.csv");
            csvPath = resource.getFile().getAbsolutePath();
        } catch (Exception e) {
            logger.error("CSV file not found or inaccessible when loading to persistent table.", e);
            throw new RuntimeException("CSV file not found or inaccessible.", e);
        }
        String sql = "CREATE TABLE IF NOT EXISTS cur2 AS SELECT * FROM read_csv_auto('" + csvPath.replace("'", "''") + "')";
        Connection conn = connectionManager.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            logger.info("Persistent table 'cur2' created or already exists.");
        } catch (SQLException e) {
            logger.error("Failed to load CSV into persistent table.", e);
            throw new RuntimeException("Failed to load CSV into persistent table.", e);
        }
    }

    /**
     * Queries all rows from the persistent DuckDB table 'cur2'.
     */
    public List<Map<String, Object>> getAllRowsFromPersistentTable() {
        logger.info("Querying up to 500 rows from persistent DuckDB table 'cur2'.");
        List<Map<String, Object>> rows = new ArrayList<>();
        String sql = "SELECT * FROM cur2 LIMIT 500";
        Connection conn = connectionManager.getConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                }
                rows.add(row);
            }
            logger.info("Persistent table query returned {} rows.", rows.size());
        } catch (SQLException e) {
            logger.error("Error querying persistent table 'cur2'.", e);
            return rows;
        }
        return rows;
    }
}
