package com.dom.benchmarking.swingbench.benchmarks.tpcctransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TPC-C Delivery transaction.
 *
 * Processes all 10 districts in the specified warehouse in a single transaction:
 * - For each district, deliver the oldest (lowest O_ID) new order, if any.
 * - Update ORDERS.O_CARRIER_ID, ORDER_LINE.OL_DELIVERY_D, and CUSTOMER's balance and delivery count.
 *
 * Optional params (otherwise randomized):
 * - "W_ID" (int)           - target warehouse
 * - "O_CARRIER_ID" (int)   - carrier id (1..10)
 */
public class DeliveryTransaction extends TPCTransaction {

    private static final Logger logger = Logger.getLogger(DeliveryTransaction.class.getName());

    private static final int MAX_W_ID_DEFAULT = 10;

    // SQL statements
    private static final String SQL_FIND_OLDEST_NEW_ORDER =
            "select min(NO_O_ID) from NEW_ORDER where NO_W_ID = ? and NO_D_ID = ?";

    private static final String SQL_DELETE_NEW_ORDER =
            "delete from NEW_ORDER where NO_W_ID = ? and NO_D_ID = ? and NO_O_ID = ?";

    private static final String SQL_GET_ORDER_CUST_FOR_UPDATE =
            "select O_C_ID from ORDERS where O_W_ID = ? and O_D_ID = ? and O_ID = ? for update";

    private static final String SQL_SET_ORDER_CARRIER =
            "update ORDERS set O_CARRIER_ID = ? where O_W_ID = ? and O_D_ID = ? and O_ID = ?";

    private static final String SQL_SET_ORDERLINES_DELIVERY_TS =
            "update ORDER_LINE set OL_DELIVERY_D = ? where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?";

    private static final String SQL_SUM_ORDERLINE_AMOUNT =
            "select sum(OL_AMOUNT) from ORDER_LINE where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?";

    private static final String SQL_UPDATE_CUSTOMER_AFTER_DELIVERY =
            "update CUSTOMER set C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
                    "where C_W_ID = ? and C_D_ID = ? and C_ID = ?";

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long start = System.nanoTime();

        int wId = optInt(params, "W_ID", RandomGenerator.randomInteger(1, MAX_W_ID_DEFAULT));
        int carrierId = optInt(params, "O_CARRIER_ID", RandomGenerator.randomInteger(1, 10));

        boolean committed = false;
        boolean originalAutoCommit = true;

        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            Timestamp now = Timestamp.from(Instant.now());
            int deliveredCount = 0;

            for (int dId = 1; dId <= 10; dId++) {
                // 1) Find the oldest new order in this district
                Integer oId = null;
                try (PreparedStatement ps = connection.prepareStatement(SQL_FIND_OLDEST_NEW_ORDER)) {
                    ps.setInt(1, wId);
                    ps.setInt(2, dId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (rs.next()) {
                            int v = rs.getInt(1);
                            if (!rs.wasNull()) oId = v;
                        }
                    }
                }

                if (oId == null) {
                    // No new order for this district; skip
                    continue;
                }

                // 2) Remove from NEW_ORDER
                try (PreparedStatement ps = connection.prepareStatement(SQL_DELETE_NEW_ORDER)) {
                    ps.setInt(1, wId);
                    ps.setInt(2, dId);
                    ps.setInt(3, oId);
                    int u = ps.executeUpdate();
                    addDeleteStatements(u);
                    if (u != 1) {
                        // Another concurrent delivery might have taken it; skip gracefully
                        // (but do not count as delivered since nothing to finalize)
                        continue;
                    }
                }

                // 3) Get customer id from ORDERS (lock row)
                int cId;
                try (PreparedStatement ps = connection.prepareStatement(SQL_GET_ORDER_CUST_FOR_UPDATE)) {
                    ps.setInt(1, wId);
                    ps.setInt(2, dId);
                    ps.setInt(3, oId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (!rs.next()) {
                            throw new SQLException("Order not found after NEW_ORDER deletion: W=" + wId + " D=" + dId + " O_ID=" + oId);
                        }
                        cId = rs.getInt(1);
                    }
                }

                // 4) Set carrier on ORDERS
                try (PreparedStatement ps = connection.prepareStatement(SQL_SET_ORDER_CARRIER)) {
                    ps.setInt(1, carrierId);
                    ps.setInt(2, wId);
                    ps.setInt(3, dId);
                    ps.setInt(4, oId);
                    int u = ps.executeUpdate();
                    addUpdateStatements(u);
                    if (u != 1) {
                        throw new SQLException("Failed to update ORDERS.O_CARRIER_ID for O_ID=" + oId);
                    }
                }

                // 5) Mark delivered on ORDER_LINE
                try (PreparedStatement ps = connection.prepareStatement(SQL_SET_ORDERLINES_DELIVERY_TS)) {
                    ps.setTimestamp(1, now);
                    ps.setInt(2, wId);
                    ps.setInt(3, dId);
                    ps.setInt(4, oId);
                    int u = ps.executeUpdate();
                    addUpdateStatements(u);
                    if (u < 1) {
                        throw new SQLException("No ORDER_LINE rows updated for delivery: O_ID=" + oId);
                    }
                }

                // 6) Sum order amount
                double sumAmount = 0.0;
                try (PreparedStatement ps = connection.prepareStatement(SQL_SUM_ORDERLINE_AMOUNT)) {
                    ps.setInt(1, wId);
                    ps.setInt(2, dId);
                    ps.setInt(3, oId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (rs.next()) {
                            sumAmount = rs.getDouble(1);
                            if (rs.wasNull()) sumAmount = 0.0;
                        }
                    }
                }

                // 7) Update customer balance and delivery count
                try (PreparedStatement ps = connection.prepareStatement(SQL_UPDATE_CUSTOMER_AFTER_DELIVERY)) {
                    ps.setDouble(1, sumAmount);
                    ps.setInt(2, wId);
                    ps.setInt(3, dId);
                    ps.setInt(4, cId);
                    int u = ps.executeUpdate();
                    addUpdateStatements(u);
                    if (u != 1) {
                        throw new SQLException("Failed to update CUSTOMER after delivery: C_ID=" + cId);
                    }
                }

                deliveredCount++;
            }

            connection.commit();
            addCommitStatements(1);

            thinkSleep();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
        } catch (SQLException e) {
            logger.log(Level.FINE, "Delivery failed: " + e.getMessage(), e);
            try {
                addRollbackStatements(1);
                connection.rollback();
            } catch (SQLException ignore) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(e);
        } finally {
            try {
                connection.setAutoCommit(originalAutoCommit);
            } catch (Exception ignore) {
                // best-effort; session may recreate/close the connection later
            }
        }
    }

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {

    }

    @Override
    public void close(Map<String, Object> param) {

    }
}