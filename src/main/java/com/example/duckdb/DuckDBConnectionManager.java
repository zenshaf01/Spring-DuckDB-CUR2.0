package com.example.duckdb;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Getter
@Component
public class DuckDBConnectionManager {
    private final Connection connection;

    public DuckDBConnectionManager(@Value("${duckdb.file}") String dbFile) {
        String duckdbUrl = "jdbc:duckdb:" + dbFile;
        try {
            this.connection = DriverManager.getConnection(duckdbUrl);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize DuckDB connection", e);
        }
    }

}
