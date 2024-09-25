package com.dom.benchmarking.swingbench.benchmarks.orderentryjdbcac;


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


public class SalesRepsOrdersQuery extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(SalesRepsOrdersQuery.class.getName());

    public SalesRepsOrdersQuery() {
        super();
    }

    public void init(Map params) {
    }

    public void execute(Map params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();
        int salesRepId = RandomGenerator.randomInteger(1, 1000);
        long executeStart = System.nanoTime();

        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT tt.ORDER_TOTAL,\n" +
                        "  tt.SALES_REP_ID,\n" +
                        "  tt.ORDER_DATE,\n" +
                        "  customers.CUST_FIRST_NAME,\n" +
                        "  customers.CUST_LAST_NAME\n" +
                        "FROM\n" +
                        "  (SELECT orders.ORDER_TOTAL,\n" +
                        "    orders.SALES_REP_ID,\n" +
                        "    orders.ORDER_DATE,\n" +
                        "    orders.customer_id,\n" +
                        "    rank() Over (Order By orders.ORDER_TOTAL DESC) sal_rank\n" +
                        "  FROM orders\n" +
                        "  WHERE orders.SALES_REP_ID = ?\n" +
                        "  ) tt,\n" +
                        "  customers\n" +
                        "WHERE tt.sal_rank <= 10\n" +
                        "and customers.customer_id = tt.customer_id")) {
            ps.setInt(1, salesRepId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            logger.log(Level.FINE, String.format("Exception : ", sbe.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    public void close() {
    }
}
