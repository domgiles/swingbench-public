package com.dom.benchmarking.swingbench.benchmarks.shardedplsqltransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleTypes;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BrowseProducts extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(BrowseProducts.class.getName());

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

        int queryTimeOut = checkForNull((Integer) params.get(QUERY_TIMEOUT), 60);
        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String uuid = sampledCustomerIds.get(RandomGenerator.randomInteger(0, sampledCustomerIds.size()));


        long start = System.nanoTime();
        int[] dmlArray = {0, 0, 0, 0, 0, 0, 0};
        boolean isSuccessful = true;
        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();
            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build();
                 CallableStatement cs = connection.prepareCall("{? = call orderentry.browseproducts(?,?,?)}")) {
                cs.registerOutParameter(1, OracleTypes.VARCHAR);
                cs.setString(2, uuid);
                cs.setInt(3, (int) this.getMinSleepTime());
                cs.setInt(4, (int) this.getMaxSleepTime());
                cs.setQueryTimeout(queryTimeOut);
                cs.executeUpdate();
                dmlArray = parseInfoArray(cs.getString(1));
                if (dmlArray[ROLLBACK_STATEMENTS] != 0)
                    isSuccessful = false;
                connection.commit();
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), isSuccessful, dmlArray));
        } catch (Exception se) {
            logger.log(Level.FINE, "Transaction BrowseProducts() failed in execute() : ", se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, dmlArray));
            throw new SwingBenchException(se);
        }
    }

    public void close() {
    }
}
