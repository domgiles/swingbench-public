package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.sql.NUMBER;
import oracle.ucp.proxy.annotation.Pre;

import javax.jms.JMSException;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamMovie extends MovieStream {

    private static final Logger logger = Logger.getLogger(StreamMovie.class.getName());

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
        Long customerID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
        try {
            JsonObject customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
            fireLogonEvent(connection, customerJ);
            addInsertStatements(1);
            Long deviceID = null;
            JsonArray di = customerJ.getJsonArray("DeviceInfo");
            if (di.size() == 0) {
                deviceID = addNewDevice(connection, customerID);
                addInsertStatements(1);
            } else  {
                JsonObject de = di.getJsonObject(0);
                deviceID = de.getJsonNumber("DeviceId").longValue();
            }
            ZoneId defaultZoneId = ZoneId.systemDefault();
            LocalDateTime dateTime = LocalDateTime.now();
            java.util.Date rightNow = java.sql.Timestamp.valueOf(dateTime);

            ArrayList<Integer> bm = browseMovies(connection);
            long selectedMovie = bm.get(RandomGenerator.randomInteger(0, bm.size()));
            JsonObject selectedMovieJ = getMovieAsJson(connection, selectedMovie);
            addSelectStatements(1);
            try (PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_SALES_TRANSACTION (SALES_TRANSACTION_ID, CUST_ID, MOVIE_ID, TRANSACTION_TIMESTAMP, DEVICE_ID,\n" +
                    "                                        PAYMENT_METHOD, CUSTOMER_PAYMENT_INFORMATION_ID, PROMOTION_DISCOUNT,\n" +
                    "                                        PAYMENT_AMOUNT)\n" +
                    "values (?,?,?,?,?,?,?,?,?)");
                 PreparedStatement seqPs = connection.prepareStatement("select CUST_SALES_TX_SEQ.nextval from dual")) {
                long sTXId = 0;
                try (ResultSet rs = seqPs.executeQuery()) {
                    rs.next();
                    sTXId = rs.getLong(1);
                }
                ps.setLong(1, sTXId);
                ps.setLong(2, customerID);
                ps.setLong(3, selectedMovie);
                ps.setDate(4, new Date(rightNow.getTime()));
                ps.setLong(5, deviceID);
                JsonArray pi = customerJ.getJsonArray("PaymentInfo");
                if (pi.size() != 0) {
                    JsonObject pe = pi.getJsonObject(0);
                    ps.setInt(6, 1);
                    ps.setLong(7, pe.getJsonNumber("PaymentInformationId").longValue());
                } else {
                    // This should be a rare occurance
                    ps.setInt(6, 1);
                    ps.setNull(7, Types.NUMERIC);
                }
                ps.setInt(8, RandomGenerator.randomInteger(5, 20));
                float f = (float) RandomGenerator.randomInteger(100, 900) / 100f;
                ps.setFloat(9, f);
                ps.execute();
                addInsertStatements(1);
            }
            try (PreparedStatement ps = connection.prepareStatement("update CUSTOMER_DEVICE\n" +
                    "set LAST_ACCESS = ?, ACCESS_COUNT = ACCESS_COUNT + 1\n" +
                    "where CUST_ID = ?")) {
                ps.setDate(1, new Date(rightNow.getTime()));
                ps.setLong(2, customerID);
                addUpdateStatements(1);
            }
            connection.commit();
            addCommitStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException | JMSException se) {
            logger.log(Level.FINE, String.format("Exception when running streaming movie transaction on customer : %d with message %s", customerID, se.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", se);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(se.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
