package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseAndUpdateOrders extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(BrowseAndUpdateOrders.class.getName());

    public BrowseAndUpdateOrders() {
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        this.initialiseBenchmark(params);
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        String threadCountryAlignment = (String) params.get("THREAD_COUNTRY_ALIGNMENT");
        String shardedConnection = (String) params.get("USE_SHARDED_CONNECTION");
        PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String myCountry = ThreadToCountryCode.getCountryCode(Thread.currentThread().getName(), countryCodes);
        Connection connection = null;

        long executeStart = System.nanoTime();
        try {
            initJdbcTask();
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
            try (PreparedStatement liPs = connection.prepareStatement(
                    "select   order_id,           \n" +
                            "        line_item_id,           \n" +
                            "        product_id,           \n" +
                            "        unit_price,           \n" +
                            "        quantity,dispatch_date,     \n" +
                            "        return_date,     \n" +
                            "        gift_wrap,     \n" +
                            "        condition,     \n" +
                            "        supplier_id,     \n" +
                            "        estimated_delivery           \n" +
                            "      from order_items           \n" +
                            "      where order_id = ?           \n" +
                            "      and country_code = ?         \n" +
                            "      and rownum < 5");
                 PreparedStatement upPs = connection.prepareStatement(
                         "update order_items           \n" +
                                 "set quantity = quantity + 1           \n" +
                                 "where order_items.order_id = ?           \n" +
                                 "and country_code = ? \n" +
                                 "and order_items.line_item_id = ?");
                 PreparedStatement upPs2 = connection.prepareStatement(
                         "update orders           \n" +
                                 "set order_total = order_total + ?           \n" +
                                 "where order_Id = ? \n" +
                                 "and country_code = ?")) {
                getCustomerDetails(connection, custID, myCountry);
                getAddressDetails(connection, custID, myCountry);
                List<Long> orders = getOrdersByCustomer(connection, custID, myCountry);
                addSelectStatements(3);
                if (orders.size() > 0) {
                    Long selectedOrder = orders.get(RandomGenerator.randomInteger(0, orders.size()));
                    liPs.setLong(1, selectedOrder);
                    liPs.setString(2, myCountry);
                    try (ResultSet rs = liPs.executeQuery()) {
                        addSelectStatements(1);
                        if (rs.next()) {
                            long lit = rs.getLong(2);
                            float up = rs.getFloat(4);
                            upPs.setLong(1, selectedOrder);
                            upPs.setString(2, myCountry);
                            upPs.setLong(3, lit);
                            upPs.executeUpdate();
                            upPs2.setFloat(1, up);
                            upPs2.setLong(2, selectedOrder);
                            upPs2.setString(3, myCountry);
                            addUpdateStatements(2);
                        }
                    }
                }
                addSelectStatements(1);
            }
            connection.commit();
            addCommitStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));

        } catch (SQLException | SwingBenchException se) {
            handleException(connection, se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }

    }

    public void close(Map<String, Object> param) {
    }
}
