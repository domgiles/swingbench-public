package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
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
        this.initialiseBenchmark(params);
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        String threadCountryAlignment = (String) params.get("THREAD_COUNTRY_ALIGNMENT");
        String shardedConnection = (String) params.get("USE_SHARDED_CONNECTION");
        List<ProductDetails> productOrders = new ArrayList<>();
        PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String myCountry = ThreadToCountryCode.getCountryCode(Thread.currentThread().getName(), countryCodes);
        Connection connection = null;
        initJdbcTask();

        long executeStart = System.nanoTime();
        try {

            if (shardedConnection.equals("true")) {
                OracleShardingKey daffKey = pds.createShardingKeyBuilder().subkey(myCountry, OracleType.VARCHAR2).build();
                connection = pds.createConnectionBuilder().shardingKey(daffKey).build();
            } else {
                connection = pds.getConnection();
            }
            long custID;
            if (threadCountryAlignment.equals("true")) {
                custID = CustomerIdManager.getRandomCustomerId(myCountry);
            } else {
                custID = CustomerIdManager.getRandomCustomerId();
                myCountry = CustomerIdManager.getCountryCode(custID);
            }
            logon(connection, myCountry, custID);
            addInsertStatements(1);
            addCommitStatements(1);
            getCustomerDetails(connection, custID, myCountry);
            long addrID = getAddressDetails(connection, custID, myCountry);
            long cardID = getCardDetails(connection, custID, myCountry);
            addSelectStatements(3);
            thinkSleep();

            int numOfBrowseCategorys = RandomGenerator.randomInteger(1, MAX_BROWSE_CATEGORY);

            for (int i = 0; i < numOfBrowseCategorys; i++) { // Look at a randomn number of products
                productOrders = getProductDetailsByCategory(connection, RandomGenerator.randomInteger(MIN_CATEGORY, MAX_CATEGORY), myCountry);
                addSelectStatements(1);
                thinkSleep();
            }
            if (!productOrders.isEmpty()) {
                long orderID;
                // TODO: Determine if this works for a sharded tableset
                try (PreparedStatement seqPs = connection.prepareStatement("select orders_seq.nextval from dual");
                     ResultSet rs = seqPs.executeQuery()) {
                    rs.next();
                    orderID = rs.getLong(1);
                }
                addSelectStatements(1);
                thinkSleep();
                int wareHouseId = getRandomWarehouseId(myCountry);
                Date orderDate = new Date(System.currentTimeMillis());
                // TODO: Add insert columns to provide equivalence to PL/SQL version
                try (PreparedStatement insOPs =
                             connection.prepareStatement("insert into orders(ORDER_ID, COUNTRY_CODE, ORDER_DATE, CUSTOMER_ID, WAREHOUSE_ID, DELIVERY_TYPE, COST_OF_DELIVERY, WAIT_TILL_ALL_AVAILABLE) " +
                                     "values (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    insOPs.setLong(1, orderID);
                    insOPs.setString(2, myCountry);
                    insOPs.setDate(3, orderDate);
                    insOPs.setLong(4, custID);
                    insOPs.setInt(5, wareHouseId);
                    insOPs.setString(6, "Standard");
                    insOPs.setInt(7, RandomGenerator.randomInteger(MIN_COST_DELIVERY, MAX_COST_DELIVERY));
                    insOPs.setString(8, "ship_asap");
                    insOPs.execute();
                }
                addInsertStatements(1);
                thinkSleep();


                int numOfProductsToBuy = Math.min(productOrders.size(), RandomGenerator.randomInteger(MIN_PRODS_TO_BUY, productOrders.size()));
                double totalOrderCost = 0;
                List<ProductDetails> itemsOrdered = new ArrayList<>();

                for (int lineItemID = 0; lineItemID < numOfProductsToBuy; lineItemID++) {
                    int prodID = productOrders.get(lineItemID).getProductID();
                    int quantity;
                    double price =
                            productOrders.get(lineItemID).getProductID();
                    quantity = productOrders.get(lineItemID).getQuantityAvailable(); // check to see if its in stock

                    if (quantity > 0) {
                        try (PreparedStatement insIPs =
                                     connection.prepareStatement("insert into order_items(ORDER_ID, COUNTRY_CODE, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY, GIFT_WRAP, CONDITION, ESTIMATED_DELIVERY) " +
                                             "values (?, ?, ?, ?, ?, ?, ?, ?, (SYSDATE+ 3))")) {
                            insIPs.setLong(1, orderID);
                            insIPs.setString(2, myCountry);
                            insIPs.setInt(3, lineItemID);
                            insIPs.setInt(4, prodID);
                            insIPs.setDouble(5, price);
                            insIPs.setInt(6, 1);
                            insIPs.setString(7, "None");
                            insIPs.setString(8, "New");
                            insIPs.execute();
                        }
                        addInsertStatements(1);
                    }

                    thinkSleep();

                    ProductDetails inventoryUpdate = new ProductDetails(prodID, wareHouseId, 1);
                    itemsOrdered.add(inventoryUpdate);
                    totalOrderCost = totalOrderCost + price;
                }

                try (PreparedStatement updOPs = connection.prepareStatement("update orders " +
                        "set order_mode = ?, " +
                        "order_status = ?, " +
                        "order_total = ?, " +
                        "delivery_address_id = ?, " +
                        "card_id = ? " +
                        "where order_id = ? and country_code = ?")) {
//                    logger.log(Level.FINE,String.format("CustomerID = %d, OrderID = %d",custID, orderID));
                    updOPs.setString(1, "online");
                    updOPs.setInt(2, RandomGenerator.randomInteger(0, AWAITING_PROCESSING));
                    updOPs.setDouble(3, totalOrderCost);
                    updOPs.setLong(4, addrID);
                    updOPs.setLong(5, cardID);
                    updOPs.setLong(6, orderID);
                    updOPs.setString(7, myCountry);
                    updOPs.execute();
                }

                addUpdateStatements(1);
                thinkSleep();
                //getOrderDetailsByOrderID(connection, orderID);
                //addSelectStatements(1);
                thinkSleep();

                for (ProductDetails inventoryUpdate : itemsOrdered) {
                    try (PreparedStatement updIns = connection.prepareStatement("update inventories " + "set quantity_on_hand = quantity_on_hand - ? " + "where product_id = ? " + "and warehouse_id = ? and country_code = ?")) {
                        updIns.setInt(1, inventoryUpdate.quantityAvailable);
                        updIns.setInt(2, inventoryUpdate.productID);
                        updIns.setInt(3, inventoryUpdate.warehouseID);
                        updIns.setString(4, myCountry);
                        updIns.execute();
                        updIns.close();
                        addUpdateStatements(1);
                    }
                }
                connection.commit();
                addCommitStatements(1);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException | SwingBenchException sbe) {
            handleException(connection, sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }

    }

    @Override
    public void close(Map<String, Object> param) {
    }


}
