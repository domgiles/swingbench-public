package com.dom.benchmarking.swingbench.transactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
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
    private static List<NLSSupport> nlsInfo = new ArrayList<>();
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

        if ((firstNames == null) || !initCompleted) { // load any data you might need (in this case only once)

            synchronized (lock) {
                if (firstNames == null) {

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
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        long custID;
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
            try (PreparedStatement seqPs = connection.prepareStatement("select customer_seq.nextval, address_seq.nextval, card_details_seq.nextval from dual");
                 PreparedStatement insPs1 = connection.prepareStatement("insert into customers (customer_id,\n" +
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
                         "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                 PreparedStatement insPs2 = connection.prepareStatement("INSERT INTO ADDRESSES (address_id,\n" +
                         "                       customer_id,\n" +
                         "                       date_created,\n" +
                         "                       house_no_or_name,\n" +
                         "                       street_name,\n" +
                         "                       town,\n" +
                         "                       county,\n" +
                         "                       country,\n" +
                         "                       post_code,\n" +
                         "                       zip_code)\n" +
                         "VALUES (?, ?, TRUNC(SYSDATE, 'MI'), ?, ?, ?, ?, ?, ?, NULL)");
                 PreparedStatement insPs3 = connection.prepareStatement("INSERT INTO CARD_DETAILS (card_id,\n" +
                         "                          customer_id,\n" +
                         "                          card_type,\n" +
                         "                          card_number,\n" +
                         "                          expiry_date,\n" +
                         "                          is_valid,\n" +
                         "                          security_code)\n" +
                         "VALUES (?, ?, ?, ?, trunc(SYSDATE + ?), ?, ?)");
                 ResultSet rs = seqPs.executeQuery()) {

                rs.next();
                custID = rs.getLong(1);
                addressID = rs.getLong(2);
                cardID = rs.getLong(3);

                addSelectStatements(1);
                thinkSleep();

                Date dob = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(18, 65) * 31556952000L));
                Date custSince = new Date(System.currentTimeMillis() - (RandomGenerator.randomLong(1, 4) * 31556952000L));

                insPs1.setLong(1, custID);
                insPs1.setString(2, firstName);
                insPs1.setString(3, lastName);
                insPs1.setString(4, nls.language);
                insPs1.setString(5, nls.territory);
                insPs1.setInt(6, RandomGenerator.randomInteger(MIN_CREDITLIMIT, MAX_CREDITLIMIT));
                insPs1.setString(7, firstName + "." + lastName + "@" + "oracle.com");
                insPs1.setInt(8, RandomGenerator.randomInteger(MIN_SALESID, MAX_SALESID));
                insPs1.setDate(9, custSince);
                insPs1.setString(10, "Ocasional");
                insPs1.setString(11, "Music");
                insPs1.setDate(12, dob);
                insPs1.setString(13, "Y");
                insPs1.setString(14, "N");
                insPs1.setLong(15, addressID);
                insPs1.setLong(16, custID);

                insPs1.execute();
                addInsertStatements(1);

                insPs2.setLong(1, addressID);
                insPs2.setLong(2, custID);
                insPs2.setInt(3, RandomGenerator.randomInteger(1, HOUSE_NO_RANGE));
                insPs2.setString(4, "Street Name");
                insPs2.setString(5, town);
                insPs2.setString(6, county);
                insPs2.setString(7, country);
                insPs2.setString(8, "Postcode");
                insPs2.execute();
                insPs2.close();
                addInsertStatements(1);


                insPs3.setLong(1, cardID);
                insPs3.setLong(2, custID);
                insPs3.setString(3, "Visa (Debit)");
                insPs3.setLong(4, RandomGenerator.randomLong(1111111111L, 9999999999L));
                insPs3.setInt(5, RandomGenerator.randomInteger(365, 1460));
                insPs3.setString(6, "Y");
                insPs3.setInt(7, RandomGenerator.randomInteger(1111, 9999));
                insPs3.execute();
                insPs3.close();
                addInsertStatements(1);

                connection.commit();
                addCommitStatements(1);
                thinkSleep();
                logon(connection, custID);
                addInsertStatements(1);
                addCommitStatements(1);
                getCustomerDetails(connection, custID);
                addSelectStatements(1);

            } catch (SQLException se) {
                logger.log(Level.FINE, String.format("Exception : %s", se.getMessage()));
                logger.log(Level.FINEST, "SQLException thrown : %s", se);
                throw new SwingBenchException(se);
            }

            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException sbe) {
            try {
                addRollbackStatements(1);
                connection.rollback();
            } catch (SQLException er) {
                logger.log(Level.FINE, "Unable to rollback transaction");
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
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
