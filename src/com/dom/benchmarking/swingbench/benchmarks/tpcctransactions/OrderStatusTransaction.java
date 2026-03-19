package com.dom.benchmarking.swingbench.benchmarks.tpcctransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TPC-C Order-Status transaction (read-only).
 *
 * Behavior:
 * - Identify a customer either by id or, 60% of the time, by last name (pick the middle of rows ordered by first name).
 * - Retrieve the most recent order (highest O_ID) for the customer.
 * - Retrieve all ORDER_LINE rows for that order.
 *
 * Optional params (otherwise randomized):
 * - "W_ID" (int)
 * - "D_ID" (int)
 * - "C_W_ID" (int) - customer's warehouse (defaults to W_ID for local)
 * - "C_D_ID" (int) - customer's district (defaults to D_ID for local)
 * - "C_ID" (int)   - if provided, search by id; otherwise 60% by last name
 * - "C_LAST" (String) - used if searching by last name
 */
public class OrderStatusTransaction extends TPCTransaction {

    private static final Logger logger = Logger.getLogger(OrderStatusTransaction.class.getName());

    // SQL statements
    private static final String SQL_CUST_BY_ID =
            "select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE " +
                    "from CUSTOMER where C_W_ID = ? and C_D_ID = ? and C_ID = ?";

    private static final String SQL_CUST_BY_LASTNAME_IDS =
            "select C_ID, C_FIRST, C_MIDDLE, C_LAST " +
                    "from CUSTOMER where C_W_ID = ? and C_D_ID = ? and C_LAST = ? " +
                    "order by C_FIRST asc";

    private static final String SQL_LAST_ORDER_FOR_CUSTOMER =
            "select O_ID, O_CARRIER_ID, O_ENTRY_D " +
                    "from ORDERS " +
                    "where O_W_ID = ? and O_D_ID = ? and O_C_ID = ? " +
                    "and O_ID = (select max(O_ID) from ORDERS where O_W_ID = ? and O_D_ID = ? and O_C_ID = ?)";

    private static final String SQL_ORDER_LINES_FOR_ORDER =
            "select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
                    "from ORDER_LINE " +
                    "where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ? " +
                    "order by OL_NUMBER asc";

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long start = System.nanoTime();

        // Randomize or use provided inputs (local by default)
        int wId = optInt(params, "W_ID", RandomGenerator.randomInteger(1, maxWarehouseCount));
        int dId = optInt(params, "D_ID", RandomGenerator.randomInteger(1, 10));

        boolean local = true; // TPC-C OS uses the customer's home W/D; keep local semantics
        int cWId = optInt(params, "C_W_ID", local ? wId : wId);
        int cDId = optInt(params, "C_D_ID", local ? dId : dId);

        boolean byLastName = !params.containsKey("C_ID") && RandomGenerator.randomInteger(1, 100) <= 60;
        Integer cIdParam = (params.get("C_ID") instanceof Number n) ? n.intValue() : null;
        String cLastParam = (params.get("C_LAST") instanceof String s) ? s : randomLastName(RandomGenerator.randomInteger(1, 3000));

        try {
            // 1) Identify customer (by ID or last name)
            int cId;
            String cFirst, cMiddle, cLast;
            double cBalance;

            if (!byLastName && cIdParam != null) {
                // Search by customer id
                cId = cIdParam;
                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_ID)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setInt(3, cId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (!rs.next()) {
                            throw new SQLException("Customer not found by id: W=" + cWId + " D=" + cDId + " C_ID=" + cId);
                        }
                        cFirst = rs.getString(1);
                        cMiddle = rs.getString(2);
                        cLast = rs.getString(3);
                        cBalance = rs.getDouble(4);
                    }
                }
            } else {
                // 60% by last name: choose the "middle" row of the first-name-sorted list
                String cLastSearch = cLastParam;
                List<Integer> ids = new ArrayList<>();
                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_LASTNAME_IDS)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setString(3, cLastSearch);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        while (rs.next()) {
                            ids.add(rs.getInt(1));
                        }
                    }
                }

                if (ids.isEmpty()) {
                    throw new SQLException("No customer found by last name: '" + cLastSearch + "' W=" + cWId + " D=" + cDId);
                }

                int midIndex = (ids.size() - 1) / 2;
                cId = ids.get(midIndex);

                // Fetch the chosen customer's header (including balance)
                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_ID)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setInt(3, cId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (!rs.next()) {
                            throw new SQLException("Customer vanished after lastname select: C_ID=" + cId);
                        }
                        cFirst = rs.getString(1);
                        cMiddle = rs.getString(2);
                        cLast = rs.getString(3);
                        cBalance = rs.getDouble(4);
                    }
                }
            }

            // 2) Retrieve the customer's most recent order
            int oId;
            Integer oCarrierId; // can be null if not delivered yet
            Instant oEntryD;

            try (PreparedStatement ps = connection.prepareStatement(SQL_LAST_ORDER_FOR_CUSTOMER)) {
                ps.setInt(1, cWId);
                ps.setInt(2, cDId);
                ps.setInt(3, cId);
                ps.setInt(4, cWId);
                ps.setInt(5, cDId);
                ps.setInt(6, cId);
                try (ResultSet rs = ps.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) {
                        // It is valid for a customer to have no orders; report gracefully
                        // but still surface the transaction as successful with no order lines.
                        oId = -1;
                        oCarrierId = null;
                        oEntryD = null;
                    } else {
                        oId = rs.getInt(1);
                        int carrier = rs.getInt(2);
                        oCarrierId = rs.wasNull() ? null : carrier;
                        Timestamp2Instant:
                        {
                            java.sql.Timestamp ts = rs.getTimestamp(3);
                            oEntryD = (ts == null) ? null : ts.toInstant();
                        }
                    }
                }
            }

            // 3) Retrieve order lines if an order exists
            List<OrderLineRow> orderLines = new ArrayList<>();
            if (oId != -1) {
                try (PreparedStatement ps = connection.prepareStatement(SQL_ORDER_LINES_FOR_ORDER)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setInt(3, oId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        while (rs.next()) {
                            OrderLineRow ol = new OrderLineRow();
                            ol.itemId = rs.getInt(1);
                            ol.supplyWId = rs.getInt(2);
                            ol.quantity = rs.getInt(3);
                            ol.amount = rs.getDouble(4);
                            java.sql.Timestamp del = rs.getTimestamp(5);
                            ol.deliveryD = (del == null) ? null : del.toInstant();
                            orderLines.add(ol);
                        }
                    }
                }
            }

            // At this point we have:
            // - Customer header (cId, cFirst, cMiddle, cLast, cBalance)
            // - Optional last order header (oId, oCarrierId, oEntryD)
            // - Optional order lines

            thinkSleep();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
        } catch (SQLException e) {
            logger.log(Level.FINE, "Order-Status failed: " + e.getMessage(), e);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(e);
        }
    }

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        fetchWarehouseCount(connection);
    }

    @Override
    public void close(Map<String, Object> param) {

    }


    // Lightweight holder for OL rows (handy if you later emit or validate details)
    private static final class OrderLineRow {
        int itemId;
        int supplyWId;
        int quantity;
        double amount;
        java.time.Instant deliveryD;
    }
}