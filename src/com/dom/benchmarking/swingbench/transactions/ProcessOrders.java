package com.dom.benchmarking.swingbench.transactions;


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


public class ProcessOrders extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(ProcessOrders.class.getName());
    private int orderID;

    public ProcessOrders() {
    }

    public void close() {
    }

    public void init(Map parameters) {
    }

    public void execute(Map params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long executeStart = System.nanoTime();

        try (PreparedStatement
                     orderPs3 = connection.prepareStatement(
                "WITH need_to_process AS            \n" +
                        "                          (SELECT order_id,            \n" +
                        "                            /* we're only looking for unprocessed orders */            \n" +
                        "                            customer_id            \n" +
                        "                          FROM orders            \n" +
                        "                          WHERE order_status <= 4            \n" +
                        "                          AND rownum         <  10            \n" +
                        "                          )            \n" +
                        "                        SELECT o.order_id,               \n" +
                        "                          oi.line_item_id,               \n" +
                        "                          oi.product_id,               \n" +
                        "                          oi.unit_price,               \n" +
                        "                          oi.quantity,               \n" +
                        "                          o.order_mode,               \n" +
                        "                          o.order_status,               \n" +
                        "                          o.order_total,               \n" +
                        "                          o.sales_rep_id,               \n" +
                        "                          o.promotion_id,               \n" +
                        "                          c.customer_id,               \n" +
                        "                          c.cust_first_name,               \n" +
                        "                          c.cust_last_name,               \n" +
                        "                          c.credit_limit,               \n" +
                        "                          c.cust_email,               \n" +
                        "                          o.order_date            \n" +
                        "                        FROM orders o,            \n" +
                        "                          need_to_process ntp,            \n" +
                        "                          customers c,            \n" +
                        "                          order_items oi            \n" +
                        "                        WHERE ntp.order_id = o.order_id            \n" +
                        "                        AND c.customer_id  = o.customer_id            \n" +
                        "                        and oi.order_id (+) = o.order_id");
             PreparedStatement updoPs = connection.prepareStatement("update /*+ index(orders, order_pk) */ " + "orders " + "set order_status = ? " + "where order_id = ?");
             ResultSet rs = orderPs3.executeQuery()
        ) {
            rs.next();
            orderID = rs.getInt(1);
            addSelectStatements(1);
            thinkSleep(); //update the order
            updoPs.setInt(1, RandomGenerator.randomInteger(AWAITING_PROCESSING + 1, ORDER_PROCESSED));
            updoPs.setInt(2, orderID);
            updoPs.execute();
            addUpdateStatements(1);
            connection.commit();
            addCommitStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            logger.log(Level.FINE, String.format("Exception : ", sbe.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", sbe);
            try {
                addRollbackStatements(1);
                connection.rollback();
            } catch (SQLException er) {
                logger.log(Level.FINE, "Unable to rollback transaction");
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe.getMessage());
        }
    }

}
