package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NewCustomerProcess extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(NewCustomerProcess.class.getName());
    private static final Object lock = new Object();

    public NewCustomerProcess() {
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        initialiseBenchmark(params);
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        String threadCountryAlignment = (String) params.get("THREAD_COUNTRY_ALIGNMENT");
        String shardedConnection = (String) params.get("USE_SHARDED_CONNECTION");
        PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String myCountry = ThreadToCountryCode.getCountryCode(Thread.currentThread().getName(), countryCodes);
        Connection connection = null;

        long addressID;
        long cardID;
        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        String town = towns.get(RandomGenerator.randomInteger(0, towns.size()));
        String county = counties.get(RandomGenerator.randomInteger(0, counties.size()));
        String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));
        NLSSupport nls = nlsInfo.get(RandomGenerator.randomInteger(0, nlsInfo.size()));
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
                custID = CustomerIdManager.getNextCustomerId(myCountry);
            } else {
                myCountry = countryCodes.get(RandomGenerator.randomInteger(0, countryCodes.size()));
                custID = CustomerIdManager.getNextCustomerId(myCountry);
            }
            try (
                    PreparedStatement insPs1 = connection.prepareStatement("insert into customers (customer_id,\n" +
                            "                       country_code,\n" +
                            "                       cust_first_name,\n" +
                            "                       cust_last_name,\n" +
                            "                       nls_language,\n" +
                            "                       nls_territory,\n" +
                            "                       credit_limit,\n" +
                            "                       cust_email,\n" +
                            "                       account_mgr_id,\n" +
                            "                       customer_since,\n" +
                            "                       customer_class,\n" +
                            "                       suggestions,\n" +
                            "                       dob,\n" +
                            "                       mailshot,\n" +
                            "                       partner_mailshot,\n" +
                            "                       preferred_address,\n" +
                            "                       preferred_card)\n" +
                            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    PreparedStatement insPs2 = connection.prepareStatement("INSERT INTO ADDRESSES (address_id,\n" +
                            "                       country_code,\n" +
                            "                       customer_id,\n" +
                            "                       date_created,\n" +
                            "                       house_no_or_name,\n" +
                            "                       street_name,\n" +
                            "                       town,\n" +
                            "                       county,\n" +
                            "                       country,\n" +
                            "                       post_code,\n" +
                            "                       zip_code)\n" +
                            "VALUES (?, ?, ?, TRUNC(SYSDATE, 'MI'), ?, ?, ?, ?, ?, ?, NULL)");
                    PreparedStatement insPs3 = connection.prepareStatement("INSERT INTO CARD_DETAILS (card_id,\n" +
                            "                          country_code,\n" +
                            "                          customer_id,\n" +
                            "                          card_type,\n" +
                            "                          card_number,\n" +
                            "                          expiry_date,\n" +
                            "                          is_valid,\n" +
                            "                          security_code)\n" +
                            "VALUES (?, ?, ?, ?, ?, trunc(SYSDATE + ?), ?, ?)")) {

                addressID = AddressIdManager.getNextAddressId(myCountry);
                cardID = CardIdManager.getNextCardId(myCountry);

                addSelectStatements(1);
                thinkSleep();

                Date dob = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(18, 65) * 31556952000L));
                Date custSince = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(1, 4) * 31556952000L));

                insPs1.setLong(1, custID);
                insPs1.setString(2, myCountry);
                insPs1.setString(3, firstName);
                insPs1.setString(4, lastName);
                insPs1.setString(5, nls.language);
                insPs1.setString(6, nls.territory);
                insPs1.setInt(7, RandomGenerator.randomInteger(MIN_CREDITLIMIT, MAX_CREDITLIMIT));
                insPs1.setString(8, firstName + "." + lastName + "@" + "oracle.com");
                insPs1.setInt(9, RandomGenerator.randomInteger(MIN_SALESID, MAX_SALESID));
                insPs1.setDate(10, custSince);
                insPs1.setString(11, "Ocasional");
                insPs1.setString(12, "Music");
                insPs1.setDate(13, dob);
                insPs1.setString(14, "Y");
                insPs1.setString(15, "N");
                insPs1.setLong(16, addressID);
                insPs1.setLong(17, custID);

                insPs1.execute();
                addInsertStatements(1);

                insPs2.setLong(1, addressID);
                insPs2.setString(2, myCountry);
                insPs2.setLong(3, custID);
                insPs2.setInt(4, RandomGenerator.randomInteger(1, HOUSE_NO_RANGE));
                insPs2.setString(5, "Street Name");
                insPs2.setString(6, town);
                insPs2.setString(7, county);
                insPs2.setString(8, country);
                insPs2.setString(9, "Postcode");
                insPs2.execute();
//                insPs2.close();
                addInsertStatements(1);


                insPs3.setLong(1, cardID);
                insPs3.setString(2, myCountry);
                insPs3.setLong(3, custID);
                insPs3.setString(4, "Visa (Debit)");
                insPs3.setLong(5, RandomGenerator.randomLong(1111111111L, 9999999999L));
                insPs3.setInt(6, RandomGenerator.randomInteger(365, 1460));
                insPs3.setString(7, "Y");
                insPs3.setInt(8, RandomGenerator.randomInteger(1111, 9999));
                insPs3.execute();
//                insPs3.close();
                addInsertStatements(1);
            }
            connection.commit();
            addCommitStatements(1);
            thinkSleep();
            logon(connection, myCountry, custID);
            addInsertStatements(1);
            addCommitStatements(1);
            getCustomerDetails(connection, custID, myCountry);
            addSelectStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            handleException(new SwingBenchException(sbe));
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }


    @Override
    public void close(Map<String, Object> param) {
    }



}
