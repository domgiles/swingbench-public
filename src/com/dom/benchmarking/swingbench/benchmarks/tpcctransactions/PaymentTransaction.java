// java
package com.dom.benchmarking.swingbench.benchmarks.tpcctransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TPC-C Payment transaction (v5.11).
 *
 * Tables (classic TPC-C names):
 * - WAREHOUSE(W_ID, W_NAME, W_YTD, W_TAX, ...)
 * - DISTRICT(D_W_ID, D_ID, D_NAME, D_YTD, D_TAX, ...)
 * - CUSTOMER(C_W_ID, C_D_ID, C_ID, C_LAST, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
 *            C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT,
 *            C_DATA, ...)
 * - HISTORY(H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)
 *
 * Behavior:
 * - 85% local payments, 15% remote (different warehouse).
 * - 60% by last name search (middle of list ordered by first name), 40% by customer id.
 * - H_AMOUNT between 1.00 and 5000.00
 * - Updates WAREHOUSE.W_YTD, DISTRICT.D_YTD
 * - Updates CUSTOMER balance and, if bad credit ("BC"), prefixes payment info into C_DATA (trimmed to 500 chars).
 * - Inserts HISTORY row with W_NAME || D_NAME into H_DATA.
 *
 * Optional params (otherwise randomized):
 * - "W_ID" (int)
 * - "D_ID" (int)
 * - "C_W_ID" (int) - customer’s warehouse
 * - "C_D_ID" (int) - customer’s district
 * - "C_ID" (int)   - if provided, search by id; otherwise 60% by last name
 * - "C_LAST" (String) - used if searching by last name
 * - "H_AMOUNT" (double) - payment amount
 */
public class PaymentTransaction extends TPCTransaction {

    private static final Logger logger = Logger.getLogger(PaymentTransaction.class.getName());

    // SQLs
    private static final String SQL_W_UPDATE_GET =
            "update WAREHOUSE set W_YTD = W_YTD + ? where W_ID = ?";

    private static final String SQL_W_GET_NAME =
            "select W_NAME from WAREHOUSE where W_ID = ?";

    private static final String SQL_D_UPDATE_GET =
            "update DISTRICT set D_YTD = D_YTD + ? where D_W_ID = ? and D_ID = ?";

    private static final String SQL_D_GET_NAME =
            "select D_NAME from DISTRICT where D_W_ID = ? and D_ID = ?";

    private static final String SQL_CUST_BY_ID_FOR_UPDATE =
            "select C_FIRST, C_MIDDLE, C_LAST, C_CREDIT, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA " +
                    "from CUSTOMER where C_W_ID = ? and C_D_ID = ? and C_ID = ? for update";

    private static final String SQL_CUST_BY_LASTNAME =
            "select C_ID, C_FIRST, C_MIDDLE, C_LAST " +
                    "from CUSTOMER where C_W_ID = ? and C_D_ID = ? and C_LAST = ? " +
                    "order by C_FIRST asc";

    private static final String SQL_CUST_UPDATE_BALANCE =
            "update CUSTOMER set C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? " +
                    "where C_W_ID = ? and C_D_ID = ? and C_ID = ?";

    private static final String SQL_HIST_INSERT =
            "insert into HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long start = System.nanoTime();

        // Randomize or use provided inputs
        int wId = optInt(params, "W_ID", RandomGenerator.randomInteger(1, maxWarehouseCount));
        int dId = optInt(params, "D_ID", RandomGenerator.randomInteger(1, 10));

        boolean localPayment = RandomGenerator.randomInteger(1, 100) <= 85;
        int cWId = optInt(params, "C_W_ID", localPayment ? wId : pickRemoteWarehouse(wId));
        int cDId = optInt(params, "C_D_ID", localPayment ? dId : RandomGenerator.randomInteger(1, 10));

        boolean byLastName = !params.containsKey("C_ID") && RandomGenerator.randomInteger(1, 100) <= 60;
        Integer cIdParam = (params.get("C_ID") instanceof Number n) ? n.intValue() : null;
        String cLastParam = (params.get("C_LAST") instanceof String s) ? s : randomLastName(RandomGenerator.randomInteger(1, 3000));

        double hAmount = params.get("H_AMOUNT") instanceof Number n
                ? clipAmount(n.doubleValue())
                : randomAmount();

        boolean committed = false;

        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            // 1) Update WAREHOUSE (YTD) and get W_NAME
            try (PreparedStatement psUpdW = connection.prepareStatement(SQL_W_UPDATE_GET)) {
                psUpdW.setDouble(1, hAmount);
                psUpdW.setInt(2, wId);
                int u = psUpdW.executeUpdate();
                addUpdateStatements(u);
                if (u != 1) throw new SQLException("Warehouse not found: " + wId);
            }

            String wName;
            try (PreparedStatement psW = connection.prepareStatement(SQL_W_GET_NAME)) {
                psW.setInt(1, wId);
                try (ResultSet rs = psW.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) throw new SQLException("Warehouse not found (name): " + wId);
                    wName = rs.getString(1);
                }
            }

            // 2) Update DISTRICT (YTD) and get D_NAME
            try (PreparedStatement psUpdD = connection.prepareStatement(SQL_D_UPDATE_GET)) {
                psUpdD.setDouble(1, hAmount);
                psUpdD.setInt(2, wId);
                psUpdD.setInt(3, dId);
                int u = psUpdD.executeUpdate();
                addUpdateStatements(u);
                if (u != 1) throw new SQLException("District not found: W=" + wId + " D=" + dId);
            }

            String dName;
            try (PreparedStatement psD = connection.prepareStatement(SQL_D_GET_NAME)) {
                psD.setInt(1, wId);
                psD.setInt(2, dId);
                try (ResultSet rs = psD.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) throw new SQLException("District not found (name): W=" + wId + " D=" + dId);
                    dName = rs.getString(1);
                }
            }

            // 3) Find customer (by ID or by last name), lock FOR UPDATE
            int cId;
            String cFirst, cMiddle, cLast, cCredit;
            double cDiscount, cBalance, cYtdPayment;
            int cPaymentCnt;
            String cData;

            if (!byLastName && cIdParam != null) {
                // by customer id
                cId = cIdParam;
                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_ID_FOR_UPDATE)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setInt(3, cId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (!rs.next()) throw new SQLException("Customer not found by id");
                        cFirst = rs.getString(1);
                        cMiddle = rs.getString(2);
                        cLast = rs.getString(3);
                        cCredit = rs.getString(4);
                        cDiscount = rs.getDouble(5);
                        cBalance = rs.getDouble(6);
                        cYtdPayment = rs.getDouble(7);
                        cPaymentCnt = rs.getInt(8);
                        cData = rs.getString(9);
                    }
                }
            } else {
                // by last name: middle of the ordered list (by first name asc)
                String cLastSearch = cLastParam;
                List<Integer> ids = new ArrayList<>();
                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_LASTNAME)) {
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
                if (ids.isEmpty()) throw new SQLException("No customer found by last name");
                int midIndex = (ids.size() - 1) / 2;
                cId = ids.get(midIndex);

                try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_BY_ID_FOR_UPDATE)) {
                    ps.setInt(1, cWId);
                    ps.setInt(2, cDId);
                    ps.setInt(3, cId);
                    try (ResultSet rs = ps.executeQuery()) {
                        addSelectStatements(1);
                        if (!rs.next()) throw new SQLException("Customer vanished after lastname select");
                        cFirst = rs.getString(1);
                        cMiddle = rs.getString(2);
                        cLast = rs.getString(3);
                        cCredit = rs.getString(4);
                        cDiscount = rs.getDouble(5);
                        cBalance = rs.getDouble(6);
                        cYtdPayment = rs.getDouble(7);
                        cPaymentCnt = rs.getInt(8);
                        cData = rs.getString(9);
                    }
                }
            }

            // 4) Update CUSTOMER financials
            double newBalance = cBalance - hAmount;
            double newYtdPayment = cYtdPayment + hAmount;
            int newPaymentCnt = cPaymentCnt + 1;

            String newCData = cData;
            if (cCredit != null && cCredit.startsWith("BC")) {
                // Per spec, prepend payment info: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT, and pipe separators
                String paymentInfo = String.format("| %d %d %d %d %d %.2f |",
                        cId, cDId, cWId, dId, wId, hAmount);

                if (newCData == null) newCData = "";
                newCData = (paymentInfo + newCData);
                if (newCData.length() > 500) {
                    newCData = newCData.substring(0, 500);
                }
            } else {
                // For "GC", c_data is not changed by spec (some impls set to null explicitly).
            }

            try (PreparedStatement ps = connection.prepareStatement(SQL_CUST_UPDATE_BALANCE)) {
                ps.setDouble(1, newBalance);
                ps.setDouble(2, newYtdPayment);
                ps.setInt(3, newPaymentCnt);
                if (newCData == null) {
                    ps.setNull(4, Types.VARCHAR);
                } else {
                    ps.setString(4, newCData);
                }
                ps.setInt(5, cWId);
                ps.setInt(6, cDId);
                ps.setInt(7, cId);
                int u = ps.executeUpdate();
                addUpdateStatements(u);
                if (u != 1) throw new SQLException("Failed to update CUSTOMER");
            }

            // 5) Insert HISTORY with W_NAME || D_NAME
            String hData = wName + " " + dName;
            Timestamp hDate = Timestamp.from(Instant.now());
            try (PreparedStatement ps = connection.prepareStatement(SQL_HIST_INSERT)) {
                ps.setInt(1, cId);
                ps.setInt(2, cDId);
                ps.setInt(3, cWId);
                ps.setInt(4, dId);
                ps.setInt(5, wId);
                ps.setTimestamp(6, hDate);
                ps.setDouble(7, hAmount);
                ps.setString(8, hData);
                ps.executeUpdate();
                addInsertStatements(1);
            }

            connection.commit();
            addCommitStatements(1);
            thinkSleep();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
        } catch (SQLException e) {

            logger.log(Level.FINE, "Payment failed: " + e.getMessage(), e);
            try {
                addRollbackStatements(1);
                connection.rollback();
            } catch (SQLException ignore) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(e);
        }
    }

    // -------- Helpers --------

    private static double randomAmount() {
        // Random 1.00 .. 5000.00
        int cents = RandomGenerator.randomInteger(100, 500000);
        return cents / 100.0;
    }

    private static double clipAmount(double v) {
        if (v < 1.0) return 1.0;
        if (v > 5000.0) return 5000.0;
        return v;
    }

    private static int pickRemoteWarehouse(int homeW) {
        if (maxWarehouseCount <= 1) return homeW;
        int w;
        do {
            w = RandomGenerator.randomInteger(1, maxWarehouseCount);
        } while (w == homeW);
        return w;
    }

    // Simple last-name generator placeholder (TPC-C uses a deterministic syllable method)
//    private static String randomLastName() {
//        // For realism you'd implement the syllable-based scheme. Here, a simple placeholder.
//        String[] pool = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
//        return pool[RandomGenerator.randomInteger(0, pool.length)];
//    }


    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        fetchWarehouseCount(connection);
    }

    @Override
    public void close(Map<String, Object> param) {

    }
}