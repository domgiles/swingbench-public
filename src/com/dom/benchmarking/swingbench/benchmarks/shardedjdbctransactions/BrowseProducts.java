package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseProducts extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(com.dom.benchmarking.swingbench.benchmarks.orderentryplsql.BrowseProducts.class.getName());

    public BrowseProducts() {
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

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String uuid = sampledCustomerIds.get(RandomGenerator.randomInteger(0, sampledCustomerIds.size()));

        initJdbcTask();

        long start = System.nanoTime();
        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();
            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build()) {

                logon(connection, uuid);
                addInsertStatements(1);
                addCommitStatements(1);
                getCustomerDetails(connection, uuid);
                addSelectStatements(1);
                thinkSleep();

                int numOfBrowseCategorys = RandomGenerator.randomInteger(1, MAX_BROWSE_CATEGORY);

                for (int i = 0; i < numOfBrowseCategorys; i++) {
                    getProductDetailsByID(connection, RandomGenerator.randomInteger(MIN_PROD_ID, MAX_PROD_ID));
                    addSelectStatements(1);
                    thinkSleep();
                }
                connection.commit();
                processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
            }

        } catch (SQLRecoverableException sre) {
            logger.log(Level.FINE, "SQLRecoverableException in BrowseProducts() probably because of end of benchmark : " + sre.getMessage());
        } catch (SQLException se) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(se);
        }
    }

    public void close() {
    }
}
