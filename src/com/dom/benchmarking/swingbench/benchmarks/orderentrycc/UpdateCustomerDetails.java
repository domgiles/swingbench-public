package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class UpdateCustomerDetails extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(UpdateCustomerDetails.class.getName());

    public UpdateCustomerDetails() {
        super();
    }

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

        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        String town = towns.get(RandomGenerator.randomInteger(0, towns.size()));
        String county = counties.get(RandomGenerator.randomInteger(0, counties.size()));
        String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));

        initJdbcTask();
        long executeStart = System.nanoTime();
        try {
            if (shardedConnection.equals("true")) {
                OracleShardingKey daffKey = pds.createShardingKeyBuilder().subkey(myCountry, OracleType.VARCHAR2).build();
                connection = pds.createConnectionBuilder().shardingKey(daffKey).build();
            } else {
                connection = pds.getConnection();
            }
            if (!threadCountryAlignment.equals("true")) {
                myCountry = countryCodes.get(RandomGenerator.randomInteger(0, countryCodes.size()));
            }
            try (
                    PreparedStatement insAddPs = connection.prepareStatement(
                            "INSERT INTO ADDRESSES    \n" +
                                    "        ( address_id,    \n" +
                                    "          customer_id,    \n" +
                                    "          country_code, \n" +
                                    "          date_created,    \n" +
                                    "          house_no_or_name,    \n" +
                                    "          street_name,    \n" +
                                    "          town,    \n" +
                                    "          county,    \n" +
                                    "          country,    \n" +
                                    "          post_code,    \n" +
                                    "          zip_code    \n" +
                                    "        )    \n" +
                                    "        VALUES    \n" +
                                    "        ( ?, ?, ?, TRUNC(SYSDATE,'MI'), ?, 'Street Name', ?, ?, ?, 'Postcode', NULL)");
                    PreparedStatement updAddPs = connection.prepareStatement(" UPDATE CUSTOMERS SET PREFERRED_ADDRESS = ? WHERE customer_id = ? AND country_code = ?");
            ) {

                List<Long> custIDLists = getCustomerDetailsByName(connection, firstName, lastName, myCountry);
                addSelectStatements(1);
                thinkSleep();
                if (!custIDLists.isEmpty()) {
                    long custID = custIDLists.get(RandomGenerator.randomInteger(0, custIDLists.size()));
                    long addId = AddressIdManager.getNextAddressId(myCountry);
                    insAddPs.setLong(1, addId);
                    insAddPs.setLong(2, custID);
                    insAddPs.setString(3, myCountry);
                    insAddPs.setInt(4, RandomGenerator.randomInteger(1, HOUSE_NO_RANGE));
                    insAddPs.setString(5, town);
                    insAddPs.setString(6, county);
                    insAddPs.setString(7, country);
                    insAddPs.execute();
                    addInsertStatements(1);
                    updAddPs.setLong(1, addId);
                    updAddPs.setLong(2, custID);
                    updAddPs.setString(3, myCountry);
                    updAddPs.execute();
                    addUpdateStatements(1);
                }
            }
            connection.commit();
            addCommitStatements(1);
            connection.close();
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            handleException(connection, sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }

    }

    @Override
    public void close(Map<String, Object> param) {
    }

}
