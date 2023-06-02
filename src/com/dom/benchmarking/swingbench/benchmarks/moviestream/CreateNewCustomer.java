package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;

import javax.jms.JMSException;
import javax.json.JsonObject;
import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CreateNewCustomer extends MovieStream {

    private static final Logger logger = Logger.getLogger(CreateNewCustomer.class.getName());
    private static final String COUNTIES_FILE = "data/counties.txt";
    private static final String COUNTRIES_FILE = "data/countries.txt";
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static final String TOWNS_FILE = "data/towns.txt";
    private static final String ROAD_NAMES_FILE = "data/1000mostpopularwords.txt";
    private static final String STATES_FILE = "data/us_states.csv";
    private static final String[] emailDomains = new String[]{"gmail.com", "msn.com", "yahoo.com", "hotmail.com", "msn.com", "aol.com", "gmail.com", "msn.com", "gmail.com", "gmail.com", "msn.com", "outlook.com"};
    private static final String[] continent = new String[]{"Asia", "Africa", "Oceania", "North America", "South America", "Europe"};
    private static final String[] roadDescriptions = new String[]{"Road", "Road", "Road", "Street", "Avenue", "Close"};
     private static final String[] cardProviders = new String[]{"visa", "amex", "visa", "visa", "mastercard", "visa"};
    private static final int HOUSE_NO_RANGE = 200;
    private static volatile List<String> counties = null;
    private static volatile List<String> countries = null;
    private static volatile List<String> firstNames = null;
    private static volatile List<String> lastNames = null;
    private static volatile List<String> roadNames = null;
    private static volatile List<String> towns = null;
    private static volatile List<String> states = null;
    private static final Object lock = new Object();


    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {

        if ((firstNames == null)) { // load any data you might need (in this case only once)
            synchronized (lock) {
                if (firstNames == null) {
                    try {
                        firstNames = Utilities.cacheFile(new File(FIRST_NAMES_FILE));
                        lastNames = Utilities.cacheFile(new File(LAST_NAMES_FILE));
                        counties = Utilities.cacheFile(new File(COUNTIES_FILE));
                        towns = Utilities.cacheFile(new File(TOWNS_FILE));
                        countries = Utilities.cacheFile(new File(COUNTRIES_FILE));
                        roadNames = Utilities.cacheFile(new File(ROAD_NAMES_FILE));
                        states = Utilities.cacheFile(new File(STATES_FILE));
                        logger.fine("Completed reading files needed for initialisation of CreateNewCustomer()");

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
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        initJdbcTask();
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        long executeStart = System.nanoTime();
        try {
            addInsertStatements(1);
            String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
            String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
            String emailDomain = emailDomains[RandomGenerator.randomInteger(0, emailDomains.length)];
            String email = String.format("%s.%s@%s", firstName, lastName, emailDomain);
            String country = countries.get(RandomGenerator.randomInteger(0, countries.size()));
            String state = counties.get(RandomGenerator.randomInteger(0, counties.size()));
            String postalCode = RandomGenerator.randomAlpha(6, 6).toUpperCase();
            String city = towns.get(RandomGenerator.randomInteger(0, towns.size()));
            String countryCode = RandomGenerator.randomAlpha(2, 2).toUpperCase();
            String roadName = String.format("%d %s %s", RandomGenerator.randomInteger(1, HOUSE_NO_RANGE), roadNames.get(RandomGenerator.randomInteger(0, roadNames.size())), roadDescriptions[RandomGenerator.randomInteger(0, roadDescriptions.length)]);

            if (!customerExists(connection, email)) {
                long newCustomerId = 0L;
                try (PreparedStatement ps_seq = connection.prepareStatement("select CUST_CONTACT_SEQ.nextval from dual")) {
                    ResultSet rsSeq = ps_seq.executeQuery();
                    rsSeq.next();
                    newCustomerId = rsSeq.getLong(1);
                }
                addSelectStatements(1);
                try (PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_CONTACT (CUST_ID, LAST_NAME, FIRST_NAME, EMAIL, STREET_ADDRESS, POSTAL_CODE, CITY, STATE_PROVINCE, COUNTRY, COUNTRY_CODE, CONTINENT, YRS_CUSTOMER, PROMOTION_RESPONSE, LOC_LAT, LOC_LONG) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                    ps.setLong(1, newCustomerId);
                    ps.setString(2, firstName);
                    ps.setString(3, lastName);
                    ps.setString(4, email);
                    ps.setString(5, roadName);
                    ps.setString(6, postalCode);
                    ps.setString(7, city);
                    ps.setString(8, state);
                    ps.setString(9, country);
                    ps.setString(10, countryCode);
                    ps.setString(11, continent[RandomGenerator.randomInteger(0, continent.length)]);
                    ps.setInt(12, RandomGenerator.randomInteger(0, 10));
                    ps.setInt(13, RandomGenerator.randomInteger(1, 5));
                    ps.setInt(14, RandomGenerator.randomInteger(-90, 90));
                    ps.setInt(15, RandomGenerator.randomInteger(-180, 180));
                    ps.execute();
                }
                addInsertStatements(1);

                long deviceId = addNewDevice(connection, newCustomerId);

                long paymentId = 0L;
                try (PreparedStatement ps_seq = connection.prepareStatement("select CUST_PAYMENT_INFO_SEQ.nextval from dual")) {
                    ResultSet rsSeq = ps_seq.executeQuery();
                    rsSeq.next();
                    paymentId = rsSeq.getLong(1);
                }
                try (PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_PAYMENT_INFORMATION (PAYMENT_INFORMATION_ID, CUST_ID, CARD_NUMBER, PROVIDER,\n" +
                        "                                          BILLING_STREET_ADDRESS, BILLING_POSTAL_CODE, BILLING_CITY,\n" +
                        "                                          BILLING_STATE_COUNTY, BILLING_COUNTRY, BILLING_COUNTRY_CODE, EXPIRY_DATE,\n" +
                        "                                          DEFAULT_PAYMENT_METHOD)\n" +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?)")) {
                    ps.setLong(1, paymentId);
                    ps.setLong(2, newCustomerId);
                    ps.setString(3, Long.toString(RandomGenerator.randomLong(11111111111l, 99999999999l)));
                    ps.setString(4, cardProviders[RandomGenerator.randomInteger(0, cardProviders.length)]);
                    ps.setString(5, roadName);
                    ps.setString(6, postalCode);
                    ps.setString(7, city);
                    ps.setString(8, state);
                    ps.setString(9, country);
                    ps.setString(10, countryCode);
                    // The following breaks my heart
                    ZoneId defaultZoneId = ZoneId.systemDefault();
                    LocalDate futureDate = LocalDate.now().plusMonths(36);
                    java.util.Date date = Date.from(futureDate.atStartOfDay(defaultZoneId).toInstant());
                    ps.setDate(11, new java.sql.Date(date.getTime()));
                    ps.setString(12, "Y");
                    ps.execute();
                }
                addInsertStatements(1);

                connection.commit();
                addCommitStatements(1);

                JsonObject customerJ = getCustomerDetailsAsJSON(connection, newCustomerId);
//                logger.log(Level.FINE,customerJ.toString());
                addSelectStatements(1);
                fireLogonEvent(connection, customerJ);
                addInsertStatements(1);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException | JMSException se) {
            logger.log(Level.FINE, String.format("Exception : ", se.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
