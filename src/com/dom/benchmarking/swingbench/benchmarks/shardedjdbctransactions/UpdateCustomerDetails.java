package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdateCustomerDetails extends OrderEntryProcess {

    private static final String COUNTIES_FILE = "data/counties.txt";
    private static final String COUNTRIES_FILE = "data/countries.txt";
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static final String TOWNS_FILE = "data/towns.txt";
    private static List<String> counties = null;
    private static List<String> countries = null;
    private static List<String> firstNames = null;
    private static List<String> lastNames = null;
    private static List<String> towns = null;
    private static final Object lock = new Object();
    private static final Logger logger = Logger.getLogger(UpdateCustomerDetails.class.getName());

    public UpdateCustomerDetails() {
        super();
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {

        if (firstNames == null) { // load any data you might need (in this case only once)
            synchronized (lock) {
                if (firstNames == null) {
                    String value = (String) params.get("SOE_FIRST_NAMES_LOC");
                    File firstNamesFile = new File((value == null) ? FIRST_NAMES_FILE : value);
                    value = (String) params.get("SOE_LAST_NAMES_LOC");
                    File lastNamesFile = new File((value == null) ? LAST_NAMES_FILE : value);

                    value = (String) params.get("SOE_TOWNS_LOC");
                    File townsFile = new File((value == null) ? TOWNS_FILE : value);
                    value = (String) params.get("SOE_COUNTIES_LOC");
                    File countiesFile = new File((value == null) ? COUNTIES_FILE : value);
                    value = (String) params.get("SOE_COUNTRIES_LOC");
                    File countriesFile = new File((value == null) ? COUNTRIES_FILE : value);

                    try {
                        firstNames = Utilities.cacheFile(firstNamesFile);
                        lastNames = Utilities.cacheFile(lastNamesFile);
                        counties = Utilities.cacheFile(countiesFile);
                        towns = Utilities.cacheFile(townsFile);
                        countries = Utilities.cacheFile(countriesFile);


                        logger.fine("Completed reading files needed for initialisation of NewCustomerProcess()");

                    } catch (java.io.FileNotFoundException fne) {
                        logger.log(Level.SEVERE, "Unable to open data seed files : ", fne);
                        throw new SwingBenchException(fne);
                    } catch (java.io.IOException ioe) {
                        logger.log(Level.SEVERE, "IO problem opening seed files : ", ioe);
                        throw new SwingBenchException(ioe);
                    }
                }
            }
        }

        try {
            sampleCustomerIds(params);
        } catch (Exception e) {
            logger.log(Level.FINE, "Transaction BrowseAndUpdateOrders() failed in init() : ", e);
            throw new SwingBenchException(e.getMessage(), SwingBenchException.UNRECOVERABLEERROR);
        }
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String uuid = sampledCustomerIds.get(RandomGenerator.randomInteger(0, sampledCustomerIds.size()));
        initJdbcTask();

        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        String town = towns.get(RandomGenerator.randomInteger(0, towns.size()));
        String county = counties.get(RandomGenerator.randomInteger(0, counties.size()));
        String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));
        long executeStart = System.nanoTime();

        try {
            OracleShardingKey key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();
            try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build();
                 PreparedStatement addSeqPs = connection.prepareStatement("select address_seq.nextval from dual");
                 PreparedStatement insAddPs = connection.prepareStatement("INSERT INTO ADDRESSES ( address_id, customer_id, date_created, house_no_or_name, street_name, town, county, country, post_code, zip_code ) VALUES " +
                         "( ?, ?, TRUNC(SYSDATE,'MI'), ?, 'Street Name', ?, ?, ?, 'Postcode', NULL)");
                 PreparedStatement updAddPs = connection.prepareStatement(" UPDATE CUSTOMERS SET PREFERRED_ADDRESS = ? WHERE customer_id = ?")
            ) {
                try {
                    List<String> custIDLists = getCustomerDetailsByID(connection, uuid);
                    addSelectStatements(1);
                    thinkSleep();
                    if (custIDLists.size() > 0) {
                        Long addId;
                        try (ResultSet rs = addSeqPs.executeQuery()) {
                            rs.next();
                            addId = rs.getLong(1);
                        }

                        insAddPs.setLong(1, addId);
                        insAddPs.setString(2, custIDLists.get(0));
                        insAddPs.setInt(3, RandomGenerator.randomInteger(1, HOUSE_NO_RANGE));
                        insAddPs.setString(4, town);
                        insAddPs.setString(5, county);
                        insAddPs.setString(6, country);
                        insAddPs.execute();
                        addInsertStatements(1);

                        updAddPs.setLong(1, addId);
                        updAddPs.setString(2, custIDLists.get(0));
                        updAddPs.execute();
                        addUpdateStatements(1);
                        connection.commit();
                        addCommitStatements(1);
                    }

                } catch (SQLRecoverableException sre) {
                    logger.log(Level.FINE, "SQLRecoverableException in UpdateCustomerDetails() probably because of end of benchmark : " + sre.getMessage());
                } catch (SQLException se) {
                    try {
                        addRollbackStatements(1);
                        connection.rollback();
                    } catch (
                            SQLException e) { // Nothing I can do. Typically as I hard close a connection at the end of run.
                    }
                    throw new SwingBenchException(se);
                }
                processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
            }
        } catch (SQLException | SwingBenchException sbe) {

            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    @Override
    public void close() {
        // TODO Implement this method
    }
}
