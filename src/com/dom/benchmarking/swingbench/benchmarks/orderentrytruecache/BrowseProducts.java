package com.dom.benchmarking.swingbench.benchmarks.orderentrytruecache;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.sql.TRANSDUMP;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseProducts extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(com.dom.benchmarking.swingbench.benchmarks.orderentryplsql.BrowseProducts.class.getName());

    public void init(Map params) {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

//        logger.log(Level.FINE, String.format("Prefetch size is %d", ((OracleConnection)connection).getDefaultRowPrefetch()));
        try {
            this.getMaxandMinCustID(connection, params);
        } catch (SQLException se) {
            logger.log(Level.SEVERE, "Unable to get max and min customer id", se);
        }
    }

    public void execute(Map params) throws SwingBenchException {
        boolean USE_TRUECACHE_CON = Boolean.parseBoolean((String) params.get("USE_TRUECACHE_CON"));
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();
        long executeStart = System.nanoTime();

        try {

            if (USE_TRUECACHE_CON) {
                connection.setReadOnly(true);
//                logger.log(Level.FINE,"Using ReadOnly Connection");
            }

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
            if(connection.isReadOnly())
                connection.setReadOnly(false);

        } catch (SQLException se) {
            logger.log(Level.FINE, String.format("Exception : ", se.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se);
        }
    }

    public void close() {
    }
}
