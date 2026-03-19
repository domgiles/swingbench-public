package com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseProducts extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(BrowseProducts.class.getName());

    public void init(Map<String, Object> params) {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

//        logger.log(Level.FINE, String.format("Prefetch size is %d", ((OracleConnection)connection).getDefaultRowPrefetch()));
        try {
            this.getMaxandMinCustID(connection, params);
        } catch (SQLException se) {
            logger.log(Level.SEVERE, "Unable to get max and min customer id", se);
        }
    }

    public void execute(Map<String, Object> params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();
        long executeStart = System.nanoTime();

        try {
            long custID = 0;
            getCustomerDetails(connection, custID);
            addSelectStatements(1);
            thinkSleep();

            int numOfBrowseCategorys = RandomGenerator.randomInteger(1, MAX_BROWSE_CATEGORY);

            for (int i = 0; i < numOfBrowseCategorys; i++) {
                double price = getProductDetailsByID(connection, RandomGenerator.randomInteger(MIN_PROD_ID, MAX_PROD_ID));
                addSelectStatements(1);
                thinkSleep();
            }

            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException se) {
            logger.log(Level.FINE, String.format("Exception : ", se.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se);
        }
    }

    public void close(Map<String, Object> param) {
    }
}
