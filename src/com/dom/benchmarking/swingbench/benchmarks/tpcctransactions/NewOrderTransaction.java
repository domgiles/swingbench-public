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
 * TPC-C New-Order transaction (v5.11).
 *
 * Required tables (classic TPC-C names):
 * WAREHOUSE(W_ID, W_TAX, ...)
 * DISTRICT(D_W_ID, D_ID, D_TAX, D_NEXT_O_ID, ...)
 * CUSTOMER(C_W_ID, C_D_ID, C_ID, C_DISCOUNT, C_LAST, C_CREDIT, ...)
 * NEW_ORDER(NO_W_ID, NO_D_ID, NO_O_ID)
 * ORDERS(O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
 * ORDER_LINE(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D,
 *            OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
 * ITEM(I_ID, I_PRICE, I_NAME, I_DATA)
 * STOCK(S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA,
 *       S_DIST_01, ..., S_DIST_10)
 *
 * Params this task understands (optional; if missing, it will randomize):
 * - "W_ID" (int)
 * - "D_ID" (int)
 * - "C_ID" (int)
 * - "ITEM_IDS" (int[])  order-line item ids
 * - "SUPPLY_W_IDS" (int[]) per-line supply warehouse ids
 * - "QUANTITIES" (int[]) per-line quantities
 */
public class NewOrderTransaction extends TPCTransaction {

    private static final Logger logger = Logger.getLogger(NewOrderTransaction.class.getName());

    // Simple bounds for randomization when params are not supplied

    private static final int MAX_ITEMS_CATALOG = 100000; // Typical TPC-C I_ID range
    private static final int MIN_OL_CNT = 5;  // per spec: 5..15
    private static final int MAX_OL_CNT = 15;
    private static final int MIN_QTY = 1;     // per spec: 1..10
    private static final int MAX_QTY = 10;

    // SQL statements
    private static final String SQL_GET_WAREHOUSE =
            "select W_TAX from WAREHOUSE where W_ID = ?";
    private static final String SQL_GET_DISTRICT_FOR_UPDATE =
            "select D_TAX, D_NEXT_O_ID from DISTRICT where D_W_ID = ? and D_ID = ? for update";
    private static final String SQL_UPDATE_DISTRICT_NEXT_O_ID =
            "update DISTRICT set D_NEXT_O_ID = ? where D_W_ID = ? and D_ID = ?";
    private static final String SQL_GET_CUSTOMER =
            "select C_DISCOUNT, C_LAST, C_CREDIT from CUSTOMER where C_W_ID = ? and C_D_ID = ? and C_ID = ?";
    private static final String SQL_INSERT_ORDERS =
            "insert into ORDERS (O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) " +
                    "values (?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_INSERT_NEW_ORDER =
            "insert into NEW_ORDER (NO_W_ID, NO_D_ID, NO_O_ID) values (?, ?, ?)";
    private static final String SQL_GET_ITEM =
            "select I_PRICE, I_NAME, I_DATA from ITEM where I_ID = ?";
    private static final String SQL_GET_STOCK_FOR_UPDATE =
            "select S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA, " +
                    "       S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 " +
                    "from STOCK where S_W_ID = ? and S_I_ID = ? for update";
    private static final String SQL_UPDATE_STOCK =
            "update STOCK set S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? " +
                    "where S_W_ID = ? and S_I_ID = ?";
    private static final String SQL_INSERT_ORDER_LINE =
            "insert into ORDER_LINE (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, " +
                    "                        OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    "values (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)";

    // Helper to pick the correct S_DIST_XX column value (01..10) into a string
    private static String pickDistInfo(int dId,
                                       String d01, String d02, String d03, String d04, String d05,
                                       String d06, String d07, String d08, String d09, String d10) {
        return switch (dId) {
            case 1 -> d01;
            case 2 -> d02;
            case 3 -> d03;
            case 4 -> d04;
            case 5 -> d05;
            case 6 -> d06;
            case 7 -> d07;
            case 8 -> d08;
            case 9 -> d09;
            case 10 -> d10;
            default -> d01; // Should not happen in spec (districts are 1..10)
        };
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long start = System.nanoTime();
        boolean success = false;

        // Supply defaults or take provided params
        int wId = optInt(params, "W_ID", RandomGenerator.randomInteger(1, maxWarehouseCount));
        int dId = optInt(params, "D_ID", RandomGenerator.randomInteger(1, 10));
        int cId = optInt(params, "C_ID", RandomGenerator.randomInteger(1, 3000)); // typical 3k customers per district

        OrderLines orderLines = readOrRandomizeOrderLines(params, wId);

        try {
            // Begin transaction
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            // 1) Read W_TAX
            double wTax;
            try (PreparedStatement ps = connection.prepareStatement(SQL_GET_WAREHOUSE)) {
                ps.setInt(1, wId);
                try (ResultSet rs = ps.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) throw new SQLException("Warehouse not found: " + wId);
                    wTax = rs.getDouble(1);
                }
            }

            // 2) Read D_TAX and D_NEXT_O_ID FOR UPDATE, then increment it
            double dTax;
            int nextOId;
            try (PreparedStatement ps = connection.prepareStatement(SQL_GET_DISTRICT_FOR_UPDATE)) {
                ps.setInt(1, wId);
                ps.setInt(2, dId);
                try (ResultSet rs = ps.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) throw new SQLException("District not found: W=" + wId + " D=" + dId);
                    dTax = rs.getDouble(1);
                    nextOId = rs.getInt(2);
                }
            }
            int newOId = nextOId;
            try (PreparedStatement ps = connection.prepareStatement(SQL_UPDATE_DISTRICT_NEXT_O_ID)) {
                ps.setInt(1, nextOId + 1);
                ps.setInt(2, wId);
                ps.setInt(3, dId);
                int upd = ps.executeUpdate();
                addUpdateStatements(upd);
                if (upd != 1) throw new SQLException("Failed to update D_NEXT_O_ID");
            }

            // 3) Read customer C_DISCOUNT (and other fields, not strictly needed for calc)
            double cDiscount;
            try (PreparedStatement ps = connection.prepareStatement(SQL_GET_CUSTOMER)) {
                ps.setInt(1, wId);
                ps.setInt(2, dId);
                ps.setInt(3, cId);
                try (ResultSet rs = ps.executeQuery()) {
                    addSelectStatements(1);
                    if (!rs.next()) throw new SQLException("Customer not found: W=" + wId + " D=" + dId + " C=" + cId);
                    cDiscount = rs.getDouble(1);
                    // rs.getString(2) -> C_LAST
                    // rs.getString(3) -> C_CREDIT
                }
            }

            // 4) Insert into ORDERS and NEW_ORDER
            int allLocal = orderLines.isAllLocal ? 1 : 0;
            Timestamp entryTs = Timestamp.from(Instant.now());
            try (PreparedStatement ps = connection.prepareStatement(SQL_INSERT_ORDERS)) {
                ps.setInt(1, wId);
                ps.setInt(2, dId);
                ps.setInt(3, newOId);
                ps.setInt(4, cId);
                ps.setTimestamp(5, entryTs);
                ps.setInt(6, orderLines.count());
                ps.setInt(7, allLocal);
                ps.executeUpdate();
                addInsertStatements(1);
            }
            try (PreparedStatement ps = connection.prepareStatement(SQL_INSERT_NEW_ORDER)) {
                ps.setInt(1, wId);
                ps.setInt(2, dId);
                ps.setInt(3, newOId);
                ps.executeUpdate();
                addInsertStatements(1);
            }

            // 5) For each order-line: verify ITEM, update STOCK (wrap rules), insert ORDER_LINE
            double totalAmount = 0.0;

            try (PreparedStatement psItem = connection.prepareStatement(SQL_GET_ITEM);
                 PreparedStatement psStock = connection.prepareStatement(SQL_GET_STOCK_FOR_UPDATE);
                 PreparedStatement psUpdStock = connection.prepareStatement(SQL_UPDATE_STOCK);
                 PreparedStatement psOl = connection.prepareStatement(SQL_INSERT_ORDER_LINE)) {

                for (int line = 0; line < orderLines.count(); line++) {
                    int olNumber = line + 1;
                    int iId = orderLines.itemIds.get(line);
                    int supplyWId = orderLines.supplyWIds.get(line);
                    int qty = orderLines.quantities.get(line);

                    // ITEM lookup (if not found -> rollback entire txn)
                    double iPrice;
                    String iData;
                    try (ResultSet rs = exec(psItem, ps -> ps.setInt(1, iId))) {
                        addSelectStatements(1);
                        if (!rs.next()) {
                            // Invalid item -> per spec: rollback
                            throw new SQLException("Invalid ITEM " + iId + " (rollback new-order)");
                        }
                        iPrice = rs.getDouble(1);
                        // String iName = rs.getString(2);
                        iData = rs.getString(3);
                    }

                    // STOCK FOR UPDATE
                    int sQty, sYtd, sOrderCnt, sRemoteCnt;
                    String sData, sDist01, sDist02, sDist03, sDist04, sDist05,
                            sDist06, sDist07, sDist08, sDist09, sDist10;
                    try (ResultSet rs = exec(psStock, ps -> {
                        ps.setInt(1, supplyWId);
                        ps.setInt(2, iId);
                    })) {
                        addSelectStatements(1);
                        if (!rs.next()) {
                            throw new SQLException("Missing STOCK for I_ID=" + iId + " W_ID=" + supplyWId);
                        }
                        sQty = rs.getInt(1);
                        sYtd = rs.getInt(2);
                        sOrderCnt = rs.getInt(3);
                        sRemoteCnt = rs.getInt(4);
                        sData = rs.getString(5);
                        sDist01 = rs.getString(6);
                        sDist02 = rs.getString(7);
                        sDist03 = rs.getString(8);
                        sDist04 = rs.getString(9);
                        sDist05 = rs.getString(10);
                        sDist06 = rs.getString(11);
                        sDist07 = rs.getString(12);
                        sDist08 = rs.getString(13);
                        sDist09 = rs.getString(14);
                        sDist10 = rs.getString(15);
                    }

                    // Quantity decrement with wrap-around (if S_QUANTITY - qty >= 10 then subtract,
                    // else add 91)
                    int newSQty = (sQty - qty >= 10) ? (sQty - qty) : (sQty - qty + 91);
                    int newSYtd = sYtd + qty;
                    int newSOrderCnt = sOrderCnt + 1;
                    int newSRemoteCnt = sRemoteCnt + ((supplyWId == wId) ? 0 : 1);

                    // Update STOCK
                    try {
                        psUpdStock.setInt(1, newSQty);
                        psUpdStock.setInt(2, newSYtd);
                        psUpdStock.setInt(3, newSOrderCnt);
                        psUpdStock.setInt(4, newSRemoteCnt);
                        psUpdStock.setInt(5, supplyWId);
                        psUpdStock.setInt(6, iId);
                        int upd = psUpdStock.executeUpdate();
                        addUpdateStatements(upd);
                        if (upd != 1) throw new SQLException("Failed to update STOCK for I_ID=" + iId);
                    } catch (SQLException e) {
                        throw e;
                    }

                    // Order-line amount = qty * i_price
                    double olAmount = qty * iPrice;
                    totalAmount += olAmount;

                    // District-specific dist info
                    String distInfo = pickDistInfo(dId, sDist01, sDist02, sDist03, sDist04, sDist05,
                            sDist06, sDist07, sDist08, sDist09, sDist10);

                    // INSERT ORDER_LINE
                    psOl.setInt(1, wId);
                    psOl.setInt(2, dId);
                    psOl.setInt(3, newOId);
                    psOl.setInt(4, olNumber);
                    psOl.setInt(5, iId);
                    psOl.setInt(6, supplyWId);
                    psOl.setInt(7, qty);
                    psOl.setDouble(8, olAmount);
                    psOl.setString(9, distInfo);
                    psOl.executeUpdate();
                    addInsertStatements(1);
                }
            }

            // 6) Compute total with tax and discount (per spec)
            // total = totalAmount * (1 + W_TAX + D_TAX) * (1 - C_DISCOUNT)
            double total = totalAmount * (1.0 + wTax + dTax) * (1.0 - cDiscount);

            // Commit
            connection.commit();
            addCommitStatements(1);
            // Optional: think time
            thinkSleep();

            // Report event (if your environment requires it)
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
            // You might log or store the computed total if needed
            logger.log(Level.FINER, "New-Order committed: W={0}, D={1}, O={2}, total={3}", new Object[]{wId, dId, newOId, total});

        } catch (SQLException e) {
            logger.log(Level.FINE, "New-Order failed: " + e.getMessage(), e);
            try {
                addRollbackStatements(1);
                connection.rollback();
            } catch (SQLException ignore) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(e);
        }
    }

    // ---------- Helpers ----------------------------------------------------


    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        fetchWarehouseCount(connection);
    }

    @Override
    public void close(Map<String, Object> param) {

    }

    @FunctionalInterface
    private interface SqlSetter {
        void accept(PreparedStatement ps) throws SQLException;
    }

    private static final class OrderLines {
        final List<Integer> itemIds;
        final List<Integer> supplyWIds;
        final List<Integer> quantities;
        final boolean isAllLocal;

        OrderLines(List<Integer> itemIds, List<Integer> supplyWIds, List<Integer> quantities, boolean isAllLocal) {
            this.itemIds = itemIds;
            this.supplyWIds = supplyWIds;
            this.quantities = quantities;
            this.isAllLocal = isAllLocal;
        }

        int count() { return itemIds.size(); }
    }

    private static OrderLines readOrRandomizeOrderLines(Map<?, ?> params, int homeWId) {
        Object iObj = params.get("ITEM_IDS");
        Object sObj = params.get("SUPPLY_W_IDS");
        Object qObj = params.get("QUANTITIES");
        if (iObj instanceof int[] ii && sObj instanceof int[] ss && qObj instanceof int[] qq
                && ii.length == ss.length && ss.length == qq.length && ii.length > 0) {
            List<Integer> items = new ArrayList<>(ii.length);
            List<Integer> swids = new ArrayList<>(ii.length);
            List<Integer> qtys = new ArrayList<>(ii.length);
            boolean allLocal = true;
            for (int i = 0; i < ii.length; i++) {
                items.add(ii[i]);
                swids.add(ss[i]);
                qtys.add(qq[i]);
                if (ss[i] != homeWId) allLocal = false;
            }
            return new OrderLines(items, swids, qtys, allLocal);
        }

        // Randomization path
        int olCnt = RandomGenerator.randomInteger(MIN_OL_CNT, MAX_OL_CNT);
        List<Integer> items = new ArrayList<>(olCnt);
        List<Integer> swids = new ArrayList<>(olCnt);
        List<Integer> qtys = new ArrayList<>(olCnt);
        boolean allLocal = true;

        // 1% chance of a remote line (classic TPC-C); adjust as desired
        boolean includeRemote = RandomGenerator.randomInteger(1, 100) == 1;

        for (int i = 0; i < olCnt; i++) {
            items.add(RandomGenerator.randomInteger(1, MAX_ITEMS_CATALOG));
            int supplyW = homeWId;
            if (includeRemote) {
                // pick a different warehouse in [1..MAX_W_ID_DEFAULT], not equal to homeWId
                int w;
                if (maxWarehouseCount > 1) {
                    do {
                        w = RandomGenerator.randomInteger(1, maxWarehouseCount);
                    } while (w == homeWId);
                    supplyW = w;
                    allLocal = false;
                }
            }
            swids.add(supplyW);
            qtys.add(RandomGenerator.randomInteger(MIN_QTY, MAX_QTY));
        }
        return new OrderLines(items, swids, qtys, allLocal);
    }
}