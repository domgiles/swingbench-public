package com.dom.benchmarking.swingbench.benchmarks.orderentrytruecache;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

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

    public void init(Map params) {
        Connection connection = (Connection) params.get(JDBC_CONNECTION);
        try {
            this.getMaxandMinCustID(connection, params);
        } catch (SQLException se) {
            logger.log(Level.SEVERE, "Unable to get max and min customer id", se);
        }
    }

    public void execute(Map params) throws SwingBenchException {
        Connection connection = (Connection) params.get(JDBC_CONNECTION);
        long custID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
        initJdbcTask();

        long executeStart = System.nanoTime();
        try {
            logon(connection, custID);
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
                            "      and rownum < 5");
                 PreparedStatement upPs = connection.prepareStatement(
                         "update order_items           \n" +
                                 "        set quantity = quantity + 1           \n" +
                                 "        where order_items.order_id = ?           \n" +
                                 "        and order_items.line_item_id = ?");
                 PreparedStatement upPs2 = connection.prepareStatement(
                         "update orders           \n" +
                                 "        set order_total = order_total + ?           \n" +
                                 "        where order_Id = ?")) {
                getCustomerDetails(connection, custID);
                getAddressDetails(connection, custID);
                List<Long> orders = getOrdersByCustomer(connection, custID);
                addSelectStatements(3);
                if (orders.size() > 0) {
                    Long selectedOrder = orders.get(RandomGenerator.randomInteger(0, orders.size()));
                    liPs.setLong(1, selectedOrder);
                    try (ResultSet rs = liPs.executeQuery()) {
                        addSelectStatements(1);
                        if (rs.next()) {
                            long lit = rs.getLong(2);
                            float up = rs.getFloat(4);
                            upPs.setLong(1, selectedOrder);
                            upPs.setLong(2, lit);
                            upPs.executeUpdate();
                            upPs2.setFloat(1, up);
                            upPs2.setLong(2, selectedOrder);
                            addUpdateStatements(2);

                        }
                    }
                }
                addSelectStatements(1);
            }
            connection.commit();
            addCommitStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));

        } catch (SQLException se) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se);
        }
    }

    public void close() {
    }
}
