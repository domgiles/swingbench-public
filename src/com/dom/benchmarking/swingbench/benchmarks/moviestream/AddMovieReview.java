package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;

import javax.jms.JMSException;
import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AddMovieReview extends MovieStream {

    private static final Logger logger = Logger.getLogger(AddMovieReview.class.getName());

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            initialiseMovieStream(params);
        } catch (SQLException  | IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        initJdbcTask();
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        long executeStart = System.nanoTime();
        try {
            Long customerID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
            JsonObject customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
            fireLogonEvent(connection, customerJ);
            addInsertStatements(1);
            String title = movieList.get(RandomGenerator.randomInteger(0, movieList.size()));
            if (title.length() > 6) {
                title = title.substring(0, 7).trim() + '*';
            }
            Long movieID = findMovieByNAme(connection, title);
            addSelectStatements(1);
            if (movieID != null) {
                long reviewID = 0L;
                try (PreparedStatement ps_seq = connection.prepareStatement("select CUST_REVIEW_SEQ.nextval from dual")) {
                    ResultSet rsSeq = ps_seq.executeQuery();
                    rsSeq.next();
                    reviewID = rsSeq.getLong(1);
                }
                addSelectStatements(1);
                try (PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_REVIEW (CUST_ID, MOVIE_ID, STAR_RATING, REVIEW, REVIEW_ID, REVIEW_TIMESTAMP) values (?,?,?,?,?,?)")) {
                    ps.setLong(1, customerID);
                    ps.setLong(2, movieID);
                    ps.setInt(3, RandomGenerator.randomInteger(1, 6));
                    StringBuffer sb = new StringBuffer();
                    for (int i = 0; i < 150; i++) {
                        sb.append(wordList.get(RandomGenerator.randomInteger(1, wordList.size())) + " ");
                    }
                    ps.setString(4, sb.toString());
                    ps.setLong(5, reviewID);
                    LocalDateTime ld = LocalDateTime.now();
                    java.util.Date date = Date.from(ld.atZone(ZoneId.systemDefault()).toInstant());
                    ps.setDate(6, new Date(date.getTime()));
                    ps.execute();
                    connection.commit();
                }
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
