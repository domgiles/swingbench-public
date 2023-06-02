package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NewOrderProcess extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(NewOrderProcess.class.getName());

    public NewOrderProcess() {
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            sampleCustomerIds(params);
        } catch (SQLException e) {
            logger.log(Level.FINE, "Transaction BrowseProducts() failed in init() : ", e);
            throw new SwingBenchException(e.getMessage(), SwingBenchException.UNRECOVERABLEERROR);
        }
    }


    public void execute(Map<String, Object> params) throws SwingBenchException {
        List<ProductDetails> productOrders = new ArrayList<>();
        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String customerUUID = sampledCustomerIds.get(RandomGenerator.randomInteger(0, sampledCustomerIds.size()));

        initJdbcTask();

        long start = System.nanoTime();
        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(customerUUID, JDBCType.VARCHAR).build();
            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build()) {
                try (PreparedStatement seqPs = connection.prepareStatement("select orders_seq.nextval from dual");
                     PreparedStatement insOPs = connection.prepareStatement("insert into orders(ORDER_ID, ORDER_DATE, CUSTOMER_ID, WAREHOUSE_ID, DELIVERY_TYPE, COST_OF_DELIVERY, WAIT_TILL_ALL_AVAILABLE) " +
                             "values (?, ?, ?, ?, ?, ?, ?)");
                     PreparedStatement insIPs = connection.prepareStatement("insert into order_items(ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, CUSTOMER_ID, UNIT_PRICE, QUANTITY, GIFT_WRAP, CONDITION, ESTIMATED_DELIVERY) " +
                             "values (?, ?, ?, ?, ?, ?, ?, ?, (SYSDATE+ 3))");
                     PreparedStatement updOPs = connection.prepareStatement("update orders " + "set order_mode = ?, order_status = ?, order_total = ? where order_id = ? and customer_id = ?")
                ) {
                    logon(connection, customerUUID);
                    addInsertStatements(1);
                    addCommitStatements(1);
                    getCustomerDetails(connection, customerUUID);
                    getAddressDetails(connection, customerUUID);
                    getCardDetails(connection, customerUUID);
                    addSelectStatements(3);
                    thinkSleep();

                    int numOfBrowseCategorys = RandomGenerator.randomInteger(1, MAX_BROWSE_CATEGORY);

                    for (int i = 0; i < numOfBrowseCategorys; i++) { // Look at a randomn number of products
                        productOrders = getProductDetailsByCategory(connection, RandomGenerator.randomInteger(MIN_CATEGORY, MAX_CATEGORY));
                        addSelectStatements(1);
                        thinkSleep();
                    }
                    if (productOrders.size() > 0) {
                        long orderID;
                        try (ResultSet rs = seqPs.executeQuery()) {
                            rs.next();
                            orderID = rs.getLong(1);
                        } catch (Exception se) {
                            logger.log(Level.SEVERE, "Getting Sequence : orders_seq.nextval", se);
                            throw new SwingBenchException(se);
                        }
                        addSelectStatements(1);
                        thinkSleep();
                        int wareHouseId = RandomGenerator.randomInteger(MIN_WAREHOUSE_ID, MAX_WAREHOUSE_ID);
                        Date orderDate = new Date(System.currentTimeMillis());
                        // TODO: Add insert columns to provide equililence to PL/SQL version
                        insOPs.setLong(1, orderID);
                        insOPs.setDate(2, orderDate);
                        insOPs.setString(3, customerUUID);
                        insOPs.setInt(4, wareHouseId);
                        insOPs.setString(5, "Standard");
                        insOPs.setInt(6, RandomGenerator.randomInteger(MIN_COST_DELIVERY, MAX_COST_DELIVERY));
                        insOPs.setString(7, "ship_asap");
                        insOPs.execute();

                        addInsertStatements(1);
                        thinkSleep();
                        int numOfProductsToBuy = RandomGenerator.randomInteger(MIN_PRODS_TO_BUY, productOrders.size());
                        double totalOrderCost = 0;
                        List<ProductDetails> itemsOrdered = new ArrayList<>();
                        for (int lineItemID = 0; lineItemID < numOfProductsToBuy; lineItemID++) {
                            try {
                                int prodID = productOrders.get(lineItemID).getProductID();
                                int quantity;
                                double price = productOrders.get(lineItemID).getProductID();
                                quantity = productOrders.get(lineItemID).getQuantityAvailable(); // check to see if its in stock
                                if (quantity > 0) {
                                    insIPs.setLong(1, orderID);
                                    insIPs.setInt(2, lineItemID);
                                    insIPs.setInt(3, prodID);
                                    insIPs.setString(4, customerUUID);
                                    insIPs.setDouble(5, price);
                                    insIPs.setInt(6, 1);
                                    insIPs.setString(7, "None");
                                    insIPs.setString(8, "New");
                                    insIPs.execute();
                                    addInsertStatements(1);
                                }
                                thinkSleep();
                                ProductDetails inventoryUpdate = new ProductDetails(prodID, wareHouseId, 1);
                                itemsOrdered.add(inventoryUpdate);
                                totalOrderCost = totalOrderCost + price;
                            } catch (java.lang.IndexOutOfBoundsException ignore) {
                                // TODO : Discover data issue
                            }
                        }

                        updOPs.setString(1, "online");
                        updOPs.setInt(2, RandomGenerator.randomInteger(0, AWAITING_PROCESSING));
                        updOPs.setDouble(3, totalOrderCost);
                        updOPs.setLong(4, orderID);
                        updOPs.setString(5, customerUUID);
                        
                        updOPs.execute();

                        addUpdateStatements(1);
                        thinkSleep();
                        connection.commit();
                        addCommitStatements(1);
                    }
                } catch (SQLRecoverableException sre) {
                    logger.log(Level.FINE, "SQLRecoverableException in NewOrderProcess() probably because of end of benchmark : " + sre.getMessage());
                } catch (SQLException se) {
                    logger.log(Level.FINE, "Unexpected Exception in NewOrderProcess() : ", se);
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
