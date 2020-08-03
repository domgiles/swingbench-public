package com.dom.benchmarking.swingbench.plsqltransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;


public class OrdersByCountyQuery extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(OrdersByCountyQuery.class.getName());

    public OrdersByCountyQuery() {
        super();
    }

    public void init(Map params) {
    }

    public void execute(Map params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long executeStart = System.nanoTime();

        try {
            try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(1),\n" +
                    "  county\n" +
                    "FROM orders,\n" +
                    "  addresses\n" +
                    "WHERE orders.DELIVERY_ADDRESS_ID = addresses.ADDRESS_ID\n" +
                    "GROUP BY county")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                addSelectStatements(1);

            } catch (Exception se) {
                logger.fine(String.format("Exception when calling store procedure orderentry.WarehouseActivityQuery in database : %s", se));
                throw new SwingBenchException(se);
            }

            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException ex) {
            addRollbackStatements(1);
            try {
                connection.rollback();
            } catch (SQLException ignored) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw ex;
        }
    }

    public void close() {
    }
}
