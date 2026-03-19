package com.dom.benchmarking.swingbench.benchmarks.tpcctransactions;

import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.dom.benchmarking.swingbench.kernel.SwingBenchException.UNRECOVERABLEERROR;

/**
 * Base class for TPC-C transactions.
 * Existing methods (metrics, think time, info, etc.) are assumed present.
 * This patch adds common helpers used across multiple transactions to reduce duplication.
 */
public abstract class TPCTransaction  extends DatabaseTransaction {

    private static final String SQL_COUNT_WAREHOUSES =
            "select count(*) from WAREHOUSE";
    static Integer maxWarehouseCount = null;

    // ---------------- Common parameter helpers ----------------

    protected int optInt(Map<?, ?> params, String key, int def) {
        Object v = params.get(key);
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) {
            try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    protected long optLong(Map<?, ?> params, String key, long def) {
        Object v = params.get(key);
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    protected double optDouble(Map<?, ?> params, String key, double def) {
        Object v = params.get(key);
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof String s) {
            try { return Double.parseDouble(s); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    protected String optString(Map<?, ?> params, String key, String def) {
        Object v = params.get(key);
        return (v instanceof String s) ? s : def;
    }

    protected int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    protected void fetchWarehouseCount(Connection connection) throws SwingBenchException {

//        if (maxWarehouseCount != null) return maxWarehouseCount;
        synchronized (NewOrderTransaction.class) {
            try (PreparedStatement ps = connection.prepareStatement(SQL_COUNT_WAREHOUSES);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    maxWarehouseCount = rs.getInt(1);
                    if (maxWarehouseCount < 1) maxWarehouseCount = 1;
                }
            } catch (SQLException e) {
                throw new SwingBenchException("Failed to get warehouse count, using default", UNRECOVERABLEERROR);
            }
        }
//        return maxWarehouseCount;
    }

    // ---------------- PreparedStatement helper ----------------

    @FunctionalInterface
    protected interface SqlSetter {
        void accept(PreparedStatement ps) throws SQLException;
    }

    protected ResultSet exec(PreparedStatement ps, SqlSetter setter) throws SQLException {
        setter.accept(ps);
        return ps.executeQuery();
    }

    // ---------------- Connection auto-commit helpers ----------------

    protected boolean disableAutoCommit(Connection c) throws SQLException {
        boolean original = c.getAutoCommit();
        if (original) c.setAutoCommit(false);
        return original;
    }

    protected void restoreAutoCommit(Connection c, boolean original) {
        try {
            c.setAutoCommit(original);
        } catch (Exception ignore) {
            // best-effort
        }
    }

    // ---------------- TPC-C helper(s) ----------------

    /**
     * Classic TPC-C last name generator is deterministic over a number (0..999).
     * Provide a canonical hook here so all transactions share the same implementation.
     * This stub simply delegates to a syllable-based implementation if present in subclasses or can be
     * replaced with a proper one later without touching all transactions.
     */
    protected String randomLastName(int num) {
        String[] syl = {"BAR", "OUT", "ABE", "PRI", "PRES", "ESE", "ANTI", "CALY", "ATN", "EING"};
        int n = num % 1000; // 0..999
        return syl[n / 100] + syl[(n / 10) % 10] + syl[n % 10];
    }
}
