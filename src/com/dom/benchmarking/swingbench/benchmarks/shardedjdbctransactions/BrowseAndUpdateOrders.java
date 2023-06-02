package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseAndUpdateOrders extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(BrowseAndUpdateOrders.class.getName());

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            sampleCustomerIds(params);
        } catch (Exception e) {
            logger.log(Level.FINE, "Transaction BrowseAndUpdateOrders() failed in init() : ", e);
            throw new SwingBenchException(e.getMessage(), SwingBenchException.UNRECOVERABLEERROR);
        }
    }

    public void execute(Map<String, Object> params) throws SwingBenchException {

        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String customerUUID = sampledCustomerIds.get(RandomGenerator.randomInteger(0, sampledCustomerIds.size()));
        initJdbcTask();

        long start = System.nanoTime();
        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(customerUUID, JDBCType.VARCHAR).build();
            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build()) {
                try (PreparedStatement liPs = connection.prepareStatement(
                        "select   ORDER_ID,           \n" +
                                "        LINE_ITEM_ID,           \n" +
                                "        PRODUCT_ID,           \n" +
                                "        UNIT_PRICE,           \n" +
                                "        QUANTITY,DISPATCH_DATE,     \n" +
                                "        RETURN_DATE,     \n" +
                                "        GIFT_WRAP,     \n" +
                                "        CONDITION,     \n" +
                                "        SUPPLIER_ID,     \n" +
                                "        ESTIMATED_DELIVERY           \n" +
                                "      from order_items           \n" +
                                "      where order_id = ?           \n" +
                                "      and customer_id = ? \n" +
                                "      and rownum < 5");
                     PreparedStatement upPs = connection.prepareStatement(
                             "update order_items           \n" +
                                     "        set quantity = quantity + 1           \n" +
                                     "        where order_items.ORDER_Id = ?           \n" +
                                     "        and order_items.LINE_ITEM_ID = ?       \n" +
                                     "        and order_items.CUSTOMER_ID = ?"
                             );
                     PreparedStatement upPs2 = connection.prepareStatement(
                             "update orders           \n" +
                                     "        set order_total = order_total + ?           \n" +
                                     "        where order_Id = ?" +
                                     "        and customer_id = ?")) {
                    logon(connection, customerUUID);
                    addInsertStatements(1);
                    addCommitStatements(1);
                    getCustomerDetails(connection, customerUUID);
                    getAddressDetails(connection, customerUUID);
                    List<Long> orders = getOrdersByCustomer(connection, customerUUID);
                    addSelectStatements(3);
                    if (orders.size() > 0) {
                        Long selectedOrder = orders.get(RandomGenerator.randomInteger(0, orders.size()));
                        liPs.setLong(1, selectedOrder);
                        liPs.setString(2, customerUUID);
                        addSelectStatements(1);
                        try (ResultSet rs = liPs.executeQuery()) {
                            addSelectStatements(1);
                            if (rs.next()) {
                                Long lit = rs.getLong(2);
                                Float up = rs.getFloat(4);
                                upPs.setLong(1, selectedOrder);
                                upPs.setLong(2, lit);
                                upPs.setString(3, customerUUID);
                                upPs.executeUpdate();
                                upPs2.setFloat(1, up);
                                upPs2.setLong(2, selectedOrder);
                                upPs2.setString(3, customerUUID);
                                addUpdateStatements(2);
                                connection.commit();
                                addCommitStatements(1);
                            }
                        }
                    }

                } catch (SQLRecoverableException sre) {
                    logger.log(Level.FINE, "SQLRecoverableException in BrowseAndUpdateOrders() probably because of end of benchmark : " + sre.getMessage());
                } catch (SQLException se) {
                    logger.log(Level.FINE, "Unexpected Exception in BrowseAndUpdateOrders() : ", se);
                    try {
                        addRollbackStatements(1);
                        connection.rollback();
                    } catch (
                            SQLException e) { // Nothing I can do. Typically as I hard close a connection at the end of run.
                    }
                    throw new SwingBenchException(se);
                }
                processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
            }
        } catch (SwingBenchException | SQLException sbe) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    public void close() {
    }
}
