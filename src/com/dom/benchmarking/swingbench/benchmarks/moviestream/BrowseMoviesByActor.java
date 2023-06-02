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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrowseMoviesByActor extends MovieStream{

    private static final Logger logger = Logger.getLogger(BrowseMoviesByActor.class.getName());


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
            String actor = actorList.get(RandomGenerator.randomInteger(0, actorList.size()));
                try (PreparedStatement ps = connection.prepareStatement("select * from MOVIE where json_textcontains(CAST, '$', ?) order by year desc FETCH FIRST 16 rows only")) {
                    ps.setString(1, actor);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            long movieID = rs.getLong(1);
                            JsonObject movieJ = getMovieAsJson(connection, movieID);
                            addSelectStatements(1);
//                            logger.finest(String.format("Actor = %s, Movie ID = %d, Movie Name = %s\n", actor, movieID, movieName));
                        }
                    }
                    addSelectStatements(1);
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
