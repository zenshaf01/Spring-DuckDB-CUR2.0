package com.example.duckdb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/duckdb")
public class DuckDBController {
    private static final Logger logger = LoggerFactory.getLogger(DuckDBController.class);
    private final DuckDBService duckDBService;

    @Autowired
    public DuckDBController(DuckDBService duckDBService) {
        this.duckDBService = duckDBService;
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        logger.info("Received request for DuckDB health check.");
        boolean healthy = duckDBService.isHealthy();
        if (healthy) {
            logger.info("DuckDB is healthy.");
            return ResponseEntity.ok("DuckDB is healthy");
        } else {
            logger.warn("DuckDB is not healthy.");
            return ResponseEntity.status(503).body("DuckDB is not healthy");
        }
    }

    @GetMapping(value = "/csv", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getCsvData() {
        logger.info("Received request for CSV data from internal file.");
        List<Map<String, Object>> rows = duckDBService.getAllRowsFromInternalCsv();
        if (rows.isEmpty()) {
            logger.warn("No data found or unable to read CSV file.");
            return ResponseEntity.status(404).body("No data found or unable to read CSV file.");
        }
        logger.info("Returning {} rows from CSV file.", rows.size());
        return ResponseEntity.ok(rows);
    }

    @GetMapping(value = "/persistent", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getPersistentTableData() {
        logger.info("Received request for persistent table data.");
        List<Map<String, Object>> rows = duckDBService.getAllRowsFromPersistentTable();
        if (rows.isEmpty()) {
            logger.warn("No data found in persistent table or table not loaded.");
            return ResponseEntity.status(404).body("No data found in persistent table or table not loaded.");
        }
        logger.info("Returning {} rows from persistent table.", rows.size());
        return ResponseEntity.ok(rows);
    }
}
