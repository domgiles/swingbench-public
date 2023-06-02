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
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrowseMovies extends MovieStream {

    private static final Logger logger = Logger.getLogger(BrowseMovies.class.getName());

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
        Long customerID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
        long executeStart = System.nanoTime();
        try {
            JsonObject customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
            fireLogonEvent(connection, customerJ);
            addInsertStatements(1);
            ArrayList<Integer> bm = browseMovies(connection);
            long movieID = RandomGenerator.randomInteger(0,bm.size());
            JsonObject movieJ = getMovieAsJson(connection, movieID);
            addSelectStatements(1);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
//        } catch (Exception se) {
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
