package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;


public class BrowseProducts extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(BrowseProducts.class.getName());

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
            getCustomerDetails(connection, custID, myCountry);
            addSelectStatements(1);
            thinkSleep();

            int numOfBrowseCategorys = RandomGenerator.randomInteger(1, MAX_BROWSE_CATEGORY);

            for (int i = 0; i < numOfBrowseCategorys; i++) {
                double price = getProductDetailsByID(connection, RandomGenerator.randomInteger(MIN_PROD_ID, MAX_PROD_ID), myCountry);
                addSelectStatements(1);
                thinkSleep();
            }
            connection.close();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));

        } catch (SQLException se) {
            handleException(connection, se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se);
        }

    }

    public void close(Map<String, Object> param) {
    }
}
