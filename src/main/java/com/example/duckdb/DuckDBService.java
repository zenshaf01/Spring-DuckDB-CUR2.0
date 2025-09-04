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

    /**
     * Health check method to verify DuckDB connection
     * @return true if connection is healthy, false otherwise
     */
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

    /**
     * Queries up to 500 rows from a CSV file at the given path using DuckDB's read_csv_auto function.
     * @param csvPath Absolute path to the CSV file
     * @return List of rows represented as maps of column names to values
     */
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

    /**
     * Queries up to 500 rows from the persistent DuckDB table 'cur2' for a specific region code.
     */
    public List<Map<String, Object>> getRowsByRegionCode(String regionCode) {
        logger.info("Querying up to 500 rows from persistent DuckDB table 'cur2' for region code: {}", regionCode);
        List<Map<String, Object>> rows = new ArrayList<>();
        String sql = "SELECT * FROM cur2 WHERE product_from_region_code = ?";
        Connection conn = connectionManager.getConnection();
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, regionCode);
            try (ResultSet rs = pstmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
                logger.info("Region query returned {} rows for region code {}.", rows.size(), regionCode);
            }
        } catch (SQLException e) {
            logger.error("Error querying persistent table 'cur2' for region code: {}", regionCode, e);
            return rows;
        }
        return rows;
    }

    /**
     * Returns the past 30-day cost for all resource IDs in us-east-2 region.
     * Each result contains line_item_resource_id, cost_last_30_days, and line_item_currency_code.
     * Limits to 500 rows for performance.
     */
    public List<Map<String, Object>> get30DayCostsForUsEast2Resources() {
        logger.info("Querying 30-day cost for all resource IDs in region us-east-2.");
        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT line_item_resource_id, line_item_currency_code, SUM(line_item_unblended_cost) AS cost_last_30_days " +
                     "FROM cur2 " +
                     "WHERE product_from_region_code = ? " +
                     "AND line_item_usage_end_date BETWEEN (CURRENT_DATE - INTERVAL 30 DAY) AND CURRENT_DATE " +
                     "GROUP BY line_item_resource_id, line_item_currency_code LIMIT 2";
        Connection conn = connectionManager.getConnection();
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "us-east-2");
            try (ResultSet rs = pstmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
                logger.info("30-day cost query returned {} resource IDs.", results.size());
            }
        } catch (SQLException e) {
            logger.error("Error querying 30-day cost for us-east-2 resources.", e);
            return results;
        }
        return results;
    }

    /**
     * Returns the total cost for all resource IDs in us-east-2 region between two dates.
     * Each result contains line_item_resource_id, total_cost, and line_item_currency_code.
     * Limits to 500 rows for performance.
     */
    public List<Map<String, Object>> getTotalCostForUsEast2ResourcesBetweenDates(String fromDate, String untilDate) {
        logger.info("Querying total cost for all resource IDs in region us-east-2 between {} and {}.", fromDate, untilDate);
        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT line_item_resource_id, line_item_currency_code, SUM(line_item_unblended_cost) AS total_cost " +
                     "FROM cur2 " +
                     "WHERE product_from_region_code = ? " +
                     "AND line_item_usage_start_date >= ? " +
                     "AND line_item_usage_end_date <= ? " +
                     "GROUP BY line_item_resource_id, line_item_currency_code";
        Connection conn = connectionManager.getConnection();
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "us-east-2");
            pstmt.setString(2, fromDate);
            pstmt.setString(3, untilDate);
            try (ResultSet rs = pstmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
                logger.info("Total cost query returned {} resource IDs.", results.size());
            }
        } catch (SQLException e) {
            logger.error("Error querying total cost for us-east-2 resources between dates.", e);
            return results;
        }
        return results;
    }

    /**
     * Returns a summary of costs and counts for all resources in us-east-2 region between two dates.
     * Response includes accounts, complianceIssueCount, resources, and totalCost per currency.
     */
    public Map<String, Object> getCostSummaryForUsEast2ResourcesBetweenDates(String fromDate, String untilDate) {
        logger.info("Querying cost summary for all resource IDs in region us-east-2 between {} and {}.", fromDate, untilDate);
        Map<String, Object> result = new HashMap<>();
        String sql = "SELECT COUNT(DISTINCT line_item_usage_account_id) AS accounts, " +
                     "COUNT(*) AS complianceIssueCount, " +
                     "COUNT(DISTINCT line_item_resource_id) AS resources, " +
                     "line_item_currency_code, SUM(line_item_unblended_cost) AS totalCost " +
                     "FROM cur2 " +
                     "WHERE product_from_region_code = ? " +
                     "AND line_item_usage_start_date >= ? " +
                     "AND line_item_usage_end_date <= ? " +
                     "GROUP BY line_item_currency_code";
        Connection conn = connectionManager.getConnection();
        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "us-east-2");
            pstmt.setString(2, fromDate);
            pstmt.setString(3, untilDate);
            try (ResultSet rs = pstmt.executeQuery()) {
                Map<String, Double> totalCostMap = new HashMap<>();
                int accounts = 0;
                int complianceIssueCount = 0;
                int resources = 0;
                boolean firstRow = true;
                while (rs.next()) {
                    if (firstRow) {
                        accounts = rs.getInt("accounts");
                        complianceIssueCount = rs.getInt("complianceIssueCount");
                        resources = rs.getInt("resources");
                        firstRow = false;
                    }
                    String currency = rs.getString("line_item_currency_code");
                    double cost = rs.getDouble("totalCost");
                    totalCostMap.put(currency, cost);
                }
                result.put("accounts", accounts);
                result.put("complianceIssueCount", complianceIssueCount);
                result.put("resources", resources);
                result.put("totalCost", totalCostMap);
                logger.info("Cost summary query returned: accounts={}, complianceIssueCount={}, resources={}, totalCost={}", accounts, complianceIssueCount, resources, totalCostMap);
            }
        } catch (SQLException e) {
            logger.error("Error querying cost summary for us-east-2 resources between dates.", e);
        }
        return result;
    }

    /**
     * Returns RI discount status (past 5 days) and 30-day cost for each resource ID.
     * Accepts a list of resource IDs and account IDs.
     * Returns a list of resource cost/discount objects matching the required response structure.
     */
    public List<Map<String, Object>> getResourceCostAndDiscountInfo(List<String> resourceIds, List<String> accountIds) {
        List<Map<String, Object>> results = new ArrayList<>();
        if (resourceIds == null || resourceIds.isEmpty()) return results;
        String accountIdPlaceholders = String.join(",", java.util.Collections.nCopies(accountIds.size(), "?"));
        String resourceIdPlaceholders = String.join(",", java.util.Collections.nCopies(resourceIds.size(), "?"));
        Connection conn = connectionManager.getConnection();
        for (String resourceId : resourceIds) {
            Map<String, Object> resourceInfo = new HashMap<>();
            // 1. Check RI Discount in past 5 days
            String riSql = "SELECT COUNT(*) > 0 AS hasDiscount FROM cur2 WHERE line_item_line_item_type = 'DiscountedUsage' AND line_item_resource_id = ? AND line_item_usage_end_date BETWEEN (CURRENT_DATE - INTERVAL 5 DAY) AND CURRENT_DATE AND line_item_usage_account_id IN (" + accountIdPlaceholders + ")";
            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(riSql)) {
                pstmt.setString(1, resourceId);
                for (int i = 0; i < accountIds.size(); i++) pstmt.setString(i + 2, accountIds.get(i));
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) resourceInfo.put("hasRightSizingRecommendations", rs.getBoolean("hasDiscount"));
                }
            } catch (SQLException e) { resourceInfo.put("hasRightSizingRecommendations", false); }
            // 2. Get 30-day cost
            String costSql = "SELECT SUM(line_item_unblended_cost) AS cost, line_item_currency_code FROM cur2 WHERE line_item_resource_id = ? AND line_item_usage_end_date BETWEEN (CURRENT_DATE - INTERVAL 30 DAY) AND CURRENT_DATE AND line_item_usage_account_id IN (" + accountIdPlaceholders + ") GROUP BY line_item_currency_code";
            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(costSql)) {
                pstmt.setString(1, resourceId);
                for (int i = 0; i < accountIds.size(); i++) pstmt.setString(i + 2, accountIds.get(i));
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        resourceInfo.put("cost", rs.getDouble("cost"));
                        resourceInfo.put("currency", rs.getString("line_item_currency_code"));
                    }
                }
            } catch (SQLException e) { resourceInfo.put("cost", 0.0); resourceInfo.put("currency", "USD"); }
            // 3. Get other fields from most recent row
            String metaSql = "SELECT * FROM cur2 WHERE line_item_resource_id = ? AND line_item_usage_account_id IN (" + accountIdPlaceholders + ") ORDER BY line_item_usage_end_date DESC LIMIT 1";
            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(metaSql)) {
                pstmt.setString(1, resourceId);
                for (int i = 0; i < accountIds.size(); i++) pstmt.setString(i + 2, accountIds.get(i));
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        resourceInfo.put("account", rs.getString("line_item_usage_account_id"));
                        resourceInfo.put("instanceId", resourceId);
                        resourceInfo.put("instanceType", rs.getString("product_instance_type"));
                        resourceInfo.put("name", rs.getString("resourcename"));
                        resourceInfo.put("platform", rs.getString("platform"));
                        resourceInfo.put("region", rs.getString("product_from_region_code"));
                        resourceInfo.put("tenancy", rs.getString("tenancy"));
                        resourceInfo.put("termInMos", rs.getInt("term_in_months"));
                        resourceInfo.put("type", rs.getString("resourcetype"));
                        resourceInfo.put("uid", rs.getString("uid"));
                        resourceInfo.put("datasource", rs.getString("datasource"));
                        Map<String, Object> allUpfront = new HashMap<>();
                        allUpfront.put("hourlyCost", rs.getDouble("allupfront_hourlycost"));
                        allUpfront.put("upfrontCharge", rs.getDouble("allupfront_upfrontcharge"));
                        resourceInfo.put("allUpfront", allUpfront);
                        Map<String, Object> noUpfront = new HashMap<>();
                        noUpfront.put("hourlyCost", rs.getDouble("noupfront_hourlycost"));
                        noUpfront.put("upfrontCharge", rs.getDouble("noupfront_upfrontcharge"));
                        resourceInfo.put("noUpfront", noUpfront);
                        Map<String, Object> partialUpfront = new HashMap<>();
                        partialUpfront.put("hourlyCost", rs.getDouble("partialupfront_hourlycost"));
                        partialUpfront.put("upfrontCharge", rs.getDouble("partialupfront_upfrontcharge"));
                        resourceInfo.put("partialUpfront", partialUpfront);
                    }
                }
            } catch (SQLException e) { /* skip meta fields if error */ }
            results.add(resourceInfo);
        }
        return results;
    }

    /**
     * Returns RI discount status (past 5 days) and 30-day cost for all resource IDs from the persisted CSV.
     * Automatically gets all account IDs and resource IDs from the cur2 table.
     */
    public List<Map<String, Object>> getAllResourceCostAndDiscountInfo() {
        logger.info("Getting all account IDs and resource IDs from persisted CSV for cost/discount analysis.");

        // First, get all distinct account IDs and resource IDs
        List<String> accountIds = new ArrayList<>();
        List<String> resourceIds = new ArrayList<>();

        String distinctSql = "SELECT DISTINCT line_item_usage_account_id, line_item_resource_id FROM cur2 WHERE line_item_resource_id IS NOT NULL AND line_item_usage_account_id IS NOT NULL";
        Connection conn = connectionManager.getConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(distinctSql)) {
            while (rs.next()) {
                String accountId = rs.getString("line_item_usage_account_id");
                String resourceId = rs.getString("line_item_resource_id");
                if (!accountIds.contains(accountId)) accountIds.add(accountId);
                if (!resourceIds.contains(resourceId)) resourceIds.add(resourceId);
            }
        } catch (SQLException e) {
            logger.error("Error getting account IDs and resource IDs from persisted table.", e);
            return new ArrayList<>();
        }

        logger.info("Found {} account IDs and {} resource IDs in persisted CSV.", accountIds.size(), resourceIds.size());
        return getResourceCostAndDiscountInfo(resourceIds, accountIds);
    }
}
