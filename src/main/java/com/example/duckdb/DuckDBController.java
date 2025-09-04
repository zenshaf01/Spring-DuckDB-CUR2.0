package com.example.duckdb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST controller for DuckDB operations
 */
@RestController
@RequestMapping("/duckdb")
public class DuckDBController {
    private static final Logger logger = LoggerFactory.getLogger(DuckDBController.class);
    private final DuckDBService duckDBService;

    @Autowired
    public DuckDBController(DuckDBService duckDBService) {
        this.duckDBService = duckDBService;
    }

    /**
     * Health check endpoint to verify DuckDB connection
     * @return ResponseEntity with health status
     */
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

    /**
     * Get all rows from the in memory CSV file
     * @return ResponseEntity with the rows or 404 if none found
     */
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

    /**
     * Get all rows from the persistent table
     * @return ResponseEntity with the rows or 404 if none found
     */
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

    /**
     * Get rows by region code
     * @param regionCode the region code to filter by
     * @return ResponseEntity with the rows or 404 if none found
     */
    @GetMapping(value = "/region/{regionCode}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getRowsByRegion(@PathVariable String regionCode) {
        logger.info("Received request for rows from region: {}", regionCode);
        List<Map<String, Object>> rows = duckDBService.getRowsByRegionCode(regionCode);
        if (rows.isEmpty()) {
            logger.warn("No data found for region code: {}", regionCode);
            return ResponseEntity.status(404).body("No data found for region code: " + regionCode);
        }
        logger.info("Returning {} rows for region code {}.", rows.size(), regionCode);
        return ResponseEntity.ok(rows);
    }

    /**
     * Get 30-day costs for all resource IDs in region us-east-2 (This is because Social Pie has stuff in this region)
     * @return ResponseEntity with the cost data or 404 if none found
     */
    @GetMapping(value = "/costs/region/us-east-2", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> get30DayCostsForUsEast2Resources() {
        logger.info("Received request for 30-day costs for all resource IDs in region us-east-2.");
        List<Map<String, Object>> results = duckDBService.get30DayCostsForUsEast2Resources();
        if (results.isEmpty()) {
            logger.warn("No cost data found for resource IDs in region us-east-2.");
            return ResponseEntity.status(404).body("No cost data found for resource IDs in region us-east-2.");
        }
        logger.info("Returning 30-day cost data for {} resource IDs in region us-east-2.", results.size());
        return ResponseEntity.ok(results);
    }

    /**
     * Get the total cost for all resource IDs in region us-east-2 between 2023-01-01 and today
     * @return ResponseEntity with the total cost data or 404 if none found
     */
    @GetMapping(value = "/costs/region/us-east-2/total", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getCostSummaryForUsEast2ResourcesBetweenDates() {
        logger.info("Received request for cost summary for all resource IDs in region us-east-2 between 2023-01-01 and today.");
        String fromDate = "2023-01-01";
        String untilDate = java.time.LocalDate.now().toString();
        Map<String, Object> result = duckDBService.getCostSummaryForUsEast2ResourcesBetweenDates(fromDate, untilDate);
        if (result.isEmpty() || !result.containsKey("accounts")) {
            logger.warn("No cost summary data found for resource IDs in region us-east-2 between {} and {}.", fromDate, untilDate);
            return ResponseEntity.status(404).body("No cost summary data found for resource IDs in region us-east-2 between " + fromDate + " and " + untilDate);
        }
        logger.info("Returning cost summary for region us-east-2 between {} and {}: {}", fromDate, untilDate, result);
        return ResponseEntity.ok(result);
    }

    /**
     * Endpoint to get RI discount status and 30-day cost for all resource IDs and account IDs from persisted CSV.
     * No request body needed - automatically gets all IDs from the database.
     */
    @GetMapping(value = "/resource/cost-discount-info", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getAllResourceCostAndDiscountInfo() {
        logger.info("Received request for RI discount and 30-day cost info for all resources from persisted CSV.");
        List<Map<String, Object>> results = duckDBService.getAllResourceCostAndDiscountInfo();
        if (results.isEmpty()) {
            logger.warn("No cost/discount data found for resources in persisted CSV.");
            return ResponseEntity.status(404).body("No cost/discount data found for resources in persisted CSV.");
        }
        logger.info("Returning cost/discount info for {} resources.", results.size());
        return ResponseEntity.ok(results);
    }
}
