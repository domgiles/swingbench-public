package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import javax.jms.JMSException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdatePaymentDetails extends MovieStream {

    private static final Logger logger = Logger.getLogger(UpdatePaymentDetails.class.getName());

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            initialiseMovieStream(params);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        initJdbcTask();
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        long executeStart = System.nanoTime();
        long customerID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
        try {
            JsonObject customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
            fireLogonEvent(connection, customerJ);
            addInsertStatements(1);
            try (PreparedStatement ps = connection.prepareStatement("update CUSTOMER_PAYMENT_INFORMATION p\n" +
                    "set p.DEFAULT_PAYMENT_METHOD = 'N'\n" +
                    "where p.CUST_ID = ?")) {
                ps.setLong(1, customerID);
                int u = ps.executeUpdate();
                addUpdateStatements(1);
            }
            if (customerJ != null) {
                try (PreparedStatement psSeq = connection.prepareStatement("select CUST_PAYMENT_INFO_SEQ.nextval from dual");
                     PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_PAYMENT_INFORMATION (PAYMENT_INFORMATION_ID, CUST_ID, CARD_NUMBER, PROVIDER,\n" +
                             "                                          BILLING_STREET_ADDRESS, BILLING_POSTAL_CODE, BILLING_CITY,\n" +
                             "                                          BILLING_STATE_COUNTY, BILLING_COUNTRY, BILLING_COUNTRY_CODE, EXPIRY_DATE,\n" +
                             "                                          DEFAULT_PAYMENT_METHOD)\n" +
                             "values (?,?,?,?,?,?,?,?,?,?,?,?)")) {
                    ResultSet rsSeq = psSeq.executeQuery();
                    rsSeq.next();
                    long paymentID = rsSeq.getLong(1);
                    ps.setLong(1, paymentID);
                    ps.setLong(2, customerID);
                    ps.setString(3, Long.toString(RandomGenerator.randomLong(1111111111L, 9999999999L)));
                    ps.setString(4, cardProviders[RandomGenerator.randomInteger(0, cardProviders.length)]);
                    ps.setString(5, customerJ.getString("StreetAddress"));
                    ps.setString(6, customerJ.getString("PostalCode"));
                    ps.setString(7, customerJ.getString("City"));
                    ps.setString(8, customerJ.getString("StateProvince"));
                    ps.setString(9, customerJ.getString("Country"));
                    ps.setString(10, customerJ.getString("CountryCode"));
                    ZoneId defaultZoneId = ZoneId.systemDefault();
                    LocalDate futureDate = LocalDate.now().plusMonths(36);
                    java.util.Date date = Date.from(futureDate.atStartOfDay(defaultZoneId).toInstant());
                    ps.setDate(11, new java.sql.Date(date.getTime()));
                    ps.setString(12, "Y");
                    ps.execute();
                }
                addSelectStatements(1);
                addInsertStatements(1);
            }
            connection.commit();
            addCommitStatements(1);
            customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
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
