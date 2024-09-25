package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import javax.jms.JMSException;
import javax.json.JsonObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrowseMoviesByGenre extends MovieStream {

    private static final Logger logger = Logger.getLogger(BrowseMoviesByGenre.class.getName());


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
        // todo : Fire Logon on event
        long executeStart = System.nanoTime();
        try {
            Long customerID = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
            JsonObject customerJ = getCustomerDetailsAsJSON(connection, customerID);
            addSelectStatements(1);
            fireLogonEvent(connection, customerJ);
            addInsertStatements(1);
            String genere = genreList.get(RandomGenerator.randomInteger(0, NUMBEROFGENRE));
            if (RandomGenerator.randomInteger(1, 3) == 1) {
                try (PreparedStatement ps = connection.prepareStatement("select * from MOVIE where json_textcontains(GENRES, '$', ?) order by year desc FETCH FIRST 16 rows only")) {
                    ps.setString(1, genere);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            long movieID = rs.getLong(1);
                            JsonObject movieJ = getMovieAsJson(connection, movieID);
                            addSelectStatements(1);
//                            logger.finest(String.format("Gerere = %s, Movie ID = %d, Movie Name = %s\n", genere, movieID, movieName));
                        }
                    }
                }
            } else {
                String logicOperator = (RandomGenerator.randomInteger(1, 3) == 1) ? "&" : "|";
                String genere1 = genreList.get(RandomGenerator.randomInteger(0, NUMBEROFGENRE));
                String genere2 = genreList.get(RandomGenerator.randomInteger(0, NUMBEROFGENRE));
                String genereQuery = String.format("%s%s%s", genere1, logicOperator, genere2);
                try (PreparedStatement ps = connection.prepareStatement("select * from MOVIE where json_textcontains(GENRES, '$', ?) order by year desc FETCH FIRST 16 rows only")) {
                    ps.setString(1, genereQuery);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            long movieID = rs.getLong(1);
                            JsonObject movieJ = getMovieAsJson(connection, movieID);
                            addSelectStatements(1);
//                            logger.finest(String.format("genereQuery = %s, Movie ID = %d, Movie Name = %s\n", genereQuery, movieID, movieName));
                        }
                    }
                }
            }
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
