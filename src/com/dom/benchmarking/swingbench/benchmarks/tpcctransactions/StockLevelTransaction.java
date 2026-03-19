// Java
package com.dom.benchmarking.swingbench.benchmarks.tpcctransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TPC-C Stock-Level Transaction (read-only).
 *
 * Steps:
 * 1) Read D_NEXT_O_ID for (W_ID, D_ID) from DISTRICT.
 * 2) Consider orders in [D_NEXT_O_ID - L, D_NEXT_O_ID - 1].
 * 3) Count distinct items (OL_I_ID) appearing in those orders where
 *    STOCK(S_W_ID = W_ID, S_I_ID = OL_I_ID).S_QUANTITY < THRESHOLD.
 *
 * Optional params:
 * - "W_ID" (int)
 * - "D_ID" (int)
 * - "THRESHOLD" (int; default 10, range 1..100)
 * - "L" (int; default 20, range >=1)
 */
public class StockLevelTransaction extends TPCTransaction {

    private static final Logger logger = Logger.getLogger(StockLevelTransaction.class.getName());

    // SQL statements
    private static final String SQL_GET_NEXT_O_ID =
            "select D_NEXT_O_ID from DISTRICT where D_W_ID = ? and D_ID = ?";

    // Count distinct items in the last L orders for district, where stock qty < threshold
    private static final String SQL_COUNT_LOW_STOCK_DISTINCT_ITEMS =
            "select count(*) " +
                    "from STOCK s " +
                    "where s.S_W_ID = ? " +
                    "  and s.S_QUANTITY < ? " +
                    "  and s.S_I_ID in (" +
                    "      select distinct ol.OL_I_ID " +
                    "      from ORDER_LINE ol " +
                    "      where ol.OL_W_ID = ? " +
                    "        and ol.OL_D_ID = ? " +
                    "        and ol.OL_O_ID >= ? " +   // startOId (inclusive)
                    "        and ol.OL_O_ID <  ? " +   // endOId   (exclusive)
                    "  )";

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long start = System.nanoTime();

        int wId = optInt(params, "W_ID", RandomGenerator.randomInteger(1, maxWarehouseCount));
        int dId = optInt(params, "D_ID", RandomGenerator.randomInteger(1, 10));
        int threshold = clampInt(optInt(params, "THRESHOLD", 10), 1, 100);
        int lastL = Math.max(1, optInt(params, "L", 20));

        try {
            // 1) Get next order id from DISTRICT
            int nextOId;
            try (PreparedStatement ps = connection.prepareStatement(SQL_GET_NEXT_O_ID)) {
                ps.setInt(1, wId);
                ps.setInt(2, dId);
                try (ResultSet rs = ps.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) {
                        throw new SQLException("District not found: W=" + wId + " D=" + dId);
                    }
                    nextOId = rs.getInt(1);
                }
            }

            // Compute order id range [start, end)
            int startOId = Math.max(1, nextOId - lastL);
            int endOIdExclusive = nextOId;

            // 2) Count low-stock items among distinct items in last L orders
            int lowStockCount = 0;
            if (startOId < endOIdExclusive) {
                try (PreparedStatement ps = connection.prepareStatement(SQL_COUNT_LOW_STOCK_DISTINCT_ITEMS)) {
                    ps.setInt(1, wId);                // s.S_W_ID
                    ps.setInt(2, threshold);          // s.S_QUANTITY < threshold
                    ps.setInt(3, wId);                // ol.OL_W_ID
                    ps.setInt(4, dId);                // ol.OL_D_ID
                    ps.setInt(5, startOId);           // ol.OL_O_ID >= start
                    ps.setInt(6, endOIdExclusive);    // ol.OL_O_ID < end
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (rs.next()) {
                            lowStockCount = rs.getInt(1);
                        }
                    }
                }
            } else {
                // If nextOId == 0 or 1, there are no prior orders to check
                lowStockCount = 0;
            }

            thinkSleep();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
        } catch (SQLException e) {
            logger.log(Level.FINE, "Stock-Level failed: " + e.getMessage(), e);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(e);
        }
    }

    // Helpers

    // Local replica to avoid external dependencies; mirrors the helper you use elsewhere

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        fetchWarehouseCount(connection);
    }

    @Override
    public void close(Map<String, Object> param) {

    }
}