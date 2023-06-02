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
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NewCustomerProcess extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(NewCustomerProcess.class.getName());

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            loadSampleData(params);
        } catch (java.io.FileNotFoundException fne) {
            logger.log(Level.SEVERE, "Unable to open data seed files : ", fne);
            throw new SwingBenchException(fne);
        } catch (java.io.IOException ioe) {
            logger.log(Level.SEVERE, "IO problem opening seed files : ", ioe);
            throw new SwingBenchException(ioe);
        }

    }

    @Override
    public void execute(Map params) throws SwingBenchException {

        String uuid;
        OracleShardingKey key;
        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);

        int queryTimeOut = checkForNull((Integer) params.get(QUERY_TIMEOUT), 60);

        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        String town = towns.get(RandomGenerator.randomInteger(0, towns.size()));
        String county = counties.get(RandomGenerator.randomInteger(0, counties.size()));
        String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));
        NLSSupport nls = nlsInfo.get(RandomGenerator.randomInteger(0, nlsInfo.size()));
        long executeStart = System.nanoTime();
        int[] infoArray = null;
        boolean isSuccessful = true;

        uuid = UUID.randomUUID().toString();
        try {
            key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();

            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build();
                 CallableStatement cs = connection.prepareCall("{? = call orderentry.newcustomer(?,?,?,?,?,?,?,?,?,?)}");) {
                connection.setAutoCommit(false);
                cs.registerOutParameter(1, OracleTypes.VARCHAR);
                cs.setString(2, uuid);
                cs.setString(3, firstName);
                cs.setString(4, lastName);
                cs.setString(5, nls.language);
                cs.setString(6, nls.territory);
                cs.setString(7, town);
                cs.setString(8, county);
                cs.setString(9, country);
                cs.setInt(10, 0);
                cs.setInt(11, 0);
                cs.setQueryTimeout(queryTimeOut);
                cs.executeUpdate();
                infoArray = parseInfoArray(cs.getString(1));
                if (infoArray[ROLLBACK_STATEMENTS] != 0)
                    isSuccessful = false;
                connection.commit();
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), isSuccessful, infoArray));
        } catch (Exception se) {
            logger.log(Level.FINE, "Transaction NewCustomerProcess() failed in execute() : ", se);
            isSuccessful = false;
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), isSuccessful, infoArray));
            throw new SwingBenchException(se);
        }
    }

    public void close() {
    }

}
