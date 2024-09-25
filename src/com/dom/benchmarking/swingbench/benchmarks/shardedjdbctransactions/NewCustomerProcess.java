package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NewCustomerProcess extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(NewCustomerProcess.class.getName());
    private static final String COUNTIES_FILE = "data/counties.txt";
    private static final String COUNTRIES_FILE = "data/countries.txt";
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static final String NLS_FILE = "data/nls.txt";
    private static final String TOWNS_FILE = "data/towns.txt";
    private static final int HOUSE_NO_RANGE = 200;
    private static List<NLSSupport> nlsInfo = new ArrayList<NLSSupport>();
    private static List<String> counties = null;
    private static List<String> countries = null;
    private static List<String> firstNames = null;
    private static List<String> lastNames = null;
    private static List<String> nlsInfoRaw = null;
    private static List<String> towns = null;
    private static final Object lock = new Object();

    public NewCustomerProcess() {
    }

    public void init(Map<String, Object> params) throws SwingBenchException {
        boolean initCompleted = false;

        // if one of the files has never been loaded
        if ((firstNames == null) || !initCompleted) { // load any data you might need (in this case only once)
            // try and obtain lock
            synchronized (lock) {
                if (firstNames == null) {
                    // Init data files
                    String value = (String) params.get("SOE_FIRST_NAMES_LOC");
                    File firstNamesFile = new File((value == null) ? FIRST_NAMES_FILE : value);
                    value = (String) params.get("SOE_LAST_NAMES_LOC");
                    File lastNamesFile = new File((value == null) ? LAST_NAMES_FILE : value);
                    value = (String) params.get("SOE_NLSDATA_LOC");
                    File nlsFile = new File((value == null) ? NLS_FILE : value);
                    value = (String) params.get("SOE_TOWNS_LOC");
                    File townsFile = new File((value == null) ? TOWNS_FILE : value);
                    value = (String) params.get("SOE_COUNTIES_LOC");
                    File countiesFile = new File((value == null) ? COUNTIES_FILE : value);
                    value = (String) params.get("SOE_COUNTRIES_LOC");
                    File countriesFile = new File((value == null) ? COUNTRIES_FILE : value);

                    try {
                        // Cache the data files in memory
                        firstNames = Utilities.cacheFile(firstNamesFile);
                        lastNames = Utilities.cacheFile(lastNamesFile);
                        nlsInfoRaw = Utilities.cacheFile(nlsFile);
                        counties = Utilities.cacheFile(countiesFile);
                        towns = Utilities.cacheFile(townsFile);
                        countries = Utilities.cacheFile(countriesFile);

                        for (String rawData : nlsInfoRaw) {
                            NLSSupport nls = new NLSSupport();
                            StringTokenizer st = new StringTokenizer(rawData, ",");
                            nls.language = st.nextToken();
                            nls.territory = st.nextToken();
                            nlsInfo.add(nls);
                        }
                        logger.fine("Completed reading files needed for initialisation of NewCustomerProcess()");

                    } catch (java.io.FileNotFoundException fne) {
                        logger.log(Level.SEVERE, "Unable to open data seed files : ", fne);
                        throw new SwingBenchException(fne);
                    } catch (java.io.IOException ioe) {
                        logger.log(Level.SEVERE, "IO problem opening seed files : ", ioe);
                        throw new SwingBenchException(ioe);
                    }
                }

                initCompleted = true;
            }
        }
    }

    public void execute(Map<String, Object> params) throws SwingBenchException {
        BigDecimal addressID;
        BigDecimal cardID;
        // Create some random data
        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        String town = towns.get(RandomGenerator.randomInteger(0, towns.size()));
        String county = counties.get(RandomGenerator.randomInteger(0, counties.size()));
        String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));
        NLSSupport nls = nlsInfo.get(RandomGenerator.randomInteger(0, nlsInfo.size()));

        // Get the connection pool for shared parameters
        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);

        // Get new UUID for new customer
        String uuid = UUID.randomUUID().toString();

        // Setup Meta Data for Transaction
        initJdbcTask();
        long start = System.nanoTime();
        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();
            try (
                    // Get the connection from the connection pool. The connection to the correct shard will automatically be returned.
                    Connection connection = ods.createConnectionBuilder().shardingKey(key).build();
                    // Setup the prepared statements with the connection
                    PreparedStatement seqPs = connection.prepareStatement("select address_seq.nextval, card_details_seq.nextval from dual");
                    PreparedStatement insPs = connection.prepareStatement("insert into customers (customer_id ,cust_first_name ,cust_last_name, " + "nls_language ,nls_territory ,credit_limit ,cust_email ,account_mgr_id, " +
                            "customer_since, customer_class, suggestions, dob, mailshot, " + "partner_mailshot, preferred_address, preferred_card) " +
                            "values (? , ? , ? , ? , ? , ? ,? , ?, ?, ?, ?, ?, ?, ?, ?, ?) ");
                    PreparedStatement insPs2 = connection.prepareStatement(
                            "INSERT INTO ADDRESSES  (address_id, customer_id, date_created, house_no_or_name, street_name, town, county, country, post_code, zip_code) VALUES " +
                                    "(?, ? ,TRUNC(SYSDATE,'MI'), ?, ? , ?, ?, ? , ?, NULL)");
                    PreparedStatement insPs3 = connection.prepareStatement(
                            "INSERT INTO CARD_DETAILS(CARD_ID, CUSTOMER_ID, CARD_TYPE, CARD_NUMBER, EXPIRY_DATE, IS_VALID, SECURITY_CODE) VALUES " +
                                    "(?, ?, ?, ?, trunc(SYSDATE + ?), ?, ?)");
            ) {
//                long s2 = System.currentTimeMillis();
                try {
                    // Get the next sequence for address and Credit Card

//                    long seqtime = System.currentTimeMillis();

                    try (ResultSet rs = seqPs.executeQuery();) {
                        rs.next();
                        addressID = rs.getBigDecimal(1);
                        cardID = rs.getBigDecimal(2);
                    }

//                    logger.log(Level.FINE,"Sequences took : " + (System.currentTimeMillis() - seqtime) + " ms");

                    addSelectStatements(1);
                    thinkSleep();

//                    long ins1 = System.currentTimeMillis();
                    // Create some more random data
                    Date dob = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(18, 65) * 31556952000L));
                    Date custSince = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(1, 4) * 31556952000L));

                    // Insert Customer
                    insPs.setString(1, uuid);
                    insPs.setString(2, firstName);
                    insPs.setString(3, lastName);
                    insPs.setString(4, nls.language);
                    insPs.setString(5, nls.territory);
                    insPs.setInt(6, RandomGenerator.randomInteger(MIN_CREDITLIMIT, MAX_CREDITLIMIT));
                    insPs.setString(7, firstName + "." + lastName + "@" + "oracle.com");
                    insPs.setInt(8, RandomGenerator.randomInteger(MIN_SALESID, MAX_SALESID));
                    insPs.setDate(9, custSince);
                    insPs.setString(10, "Ocasional");
                    insPs.setString(11, "Music");
                    insPs.setDate(12, dob);
                    insPs.setString(13, "Y");
                    insPs.setString(14, "N");
                    insPs.setBigDecimal(15, addressID);
                    insPs.setBigDecimal(16, cardID);

                    insPs.execute();
                    addInsertStatements(1);

//                    logger.log(Level.FINE,"Insert1 took : " + (System.currentTimeMillis() - ins1) + " ms");

//                    long ins2 = System.currentTimeMillis();
                    // Insert address
                    insPs2.setBigDecimal(1, addressID);
                    insPs2.setString(2, uuid);
                    insPs2.setInt(3, RandomGenerator.randomInteger(1, HOUSE_NO_RANGE));
                    insPs2.setString(4, "Street Name");
                    insPs2.setString(5, town);
                    insPs2.setString(6, county);
                    insPs2.setString(7, country);
                    insPs2.setString(8, "Postcode");

                    insPs2.execute();
//                    logger.log(Level.FINE, "Insert2 took : " + (System.currentTimeMillis() - ins2) + " ms");
                    addInsertStatements(1);

//                    long ins3 = System.currentTimeMillis();
                    // Insert Credit Card
                    insPs3.setBigDecimal(1, cardID);
                    insPs3.setString(2, uuid);
                    insPs3.setString(3, "Visa (Debit)");
                    insPs3.setLong(4, RandomGenerator.randomLong(1111111111l, 9999999999l));
                    insPs3.setInt(5, RandomGenerator.randomInteger(365, 1460));
                    insPs3.setString(6, "Y");
                    insPs3.setInt(7, RandomGenerator.randomInteger(1111, 9999));

                    insPs3.execute();
//                    logger.log(Level.FINE, "Insert3 took : " + (System.currentTimeMillis() - ins3) + " ms");
                    addInsertStatements(1);

                    // Commit Transactions
                    connection.commit();
//                    logger.log(Level.FINE, "Statements took : " + (System.currentTimeMillis() - s2) + " ms");
                    addCommitStatements(1);
                    thinkSleep();
                    logon(connection, uuid);
                    addInsertStatements(1);
                    addCommitStatements(1);
                    getCustomerDetails(connection, uuid);
                    addSelectStatements(1);
                } catch (SQLRecoverableException sre) {
                    logger.log(Level.FINE, "SQLRecoverableException in NewCustomerProcess() probably because of end of benchmark : " + sre.getMessage());
                } catch (SQLException se) {
                    logger.log(Level.FINE, "Unexpected Exception in NewCustomerProcess() : ", se);
                    try {
                        addRollbackStatements(1);
                        connection.rollback();
                    } catch (
                            SQLException e) { // Nothing I can do. Typically as I hard close a connection at the end of run.
                    }
                    throw new SwingBenchException(se);
                }
                // Process successful transaction
                processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), true, getInfoArray()));
            }
        } catch (Exception sbe) {
            // Process failed transaction
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - start), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    public void close() {
    }

    private class NLSSupport {

        String language = null;
        String territory = null;

    }

}
