package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.datagen.constants.Constants;
import com.dom.datagen.kernel.utilities.Random;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteExistingComment extends DatabaseTransaction {
    private static final Logger logger = Logger.getLogger(DeleteExistingComment.class.getName());
    private static final Lock lock = new ReentrantLock();
    private static Long articlesRange = null;
    private static Boolean jdvMode = false;

    private Random random;

    public DeleteExistingComment() {
        super();
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        random = new Random(Constants.RandomType.MERSENNE);

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

        lock.lock();
        if (articlesRange == null) {
            logger.fine("Reference data loaded for DeleteExistingComment transaction");
            try (Statement s = connection.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT METADATA_VALUE\n" +
												   "FROM ARTICLES_METADATA\n" +
												   "WHERE METADATA_KEY = 'MAX_ARTICLES_COUNT'")) {
                    if (rs.next()) {
                        articlesRange = rs.getLong(1);
                    }
                }
                try (ResultSet rs = s.executeQuery("SELECT METADATA_VALUE\n" +
												   "FROM ARTICLES_METADATA\n" +
												   "WHERE METADATA_KEY = 'MODE'")) {
                    if (rs.next()) {
                        jdvMode = "JDV".equalsIgnoreCase("JDV");
                    }
                }
            }
            catch (SQLException se) {
                logger.log(Level.SEVERE, "Error initialising DeleteExistingComment", se);
                throw new SwingBenchException(se);
            }
        }
        lock.unlock();
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        long executeStart = 0;
        try {
            Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
            final long articleKey = RandomGenerator.randomLong(1, articlesRange);

            initJdbcTask();

            executeStart = System.nanoTime();
            try {
                if(jdvMode) {
                    try (PreparedStatement p = connection.prepareStatement("select a.data.comments.size() from articles_dv a where a.data.\"_id\"=?")) {
                        p.setLong(1, articleKey);
                        try (ResultSet r = p.executeQuery()) {
                            if (r.next()) {
                                addSelectStatements(1);
                                final int numberOfComments = r.getInt(1);

                                final int commentPositionToUpdate = random.randomInteger(0, numberOfComments - 1);

                                try (PreparedStatement p2 = connection.prepareStatement(String.format("update articles_dv d set data = json_transform( data, \n" +
                                        "REMOVE '$.comments[%d]') \n" +
                                        "where d.data.\"_id\"=?", commentPositionToUpdate))) {
                                    p2.setLong(1, articleKey);
                                    p2.executeUpdate();
                                    addUpdateStatements(1);
                                }
                            }
                        }
                    }
                } else {
                    try (PreparedStatement p = connection.prepareStatement("select a.data.comments.size() from articles a where id=?")) {
                        p.setLong(1, articleKey);
                        try (ResultSet r = p.executeQuery()) {
                            if (r.next()) {
                                addSelectStatements(1);
                                final int numberOfComments = r.getInt(1);

                                final int commentPositionToUpdate = random.randomInteger(0, numberOfComments - 1);

                                try (PreparedStatement p2 = connection.prepareStatement(String.format("update articles d set data = json_transform( data, \n" +
                                        "REMOVE '$.comments[%d]') \n" +
                                        "where id=?", commentPositionToUpdate))) {
                                    p2.setLong(1, articleKey);
                                    p2.executeUpdate();
                                    addUpdateStatements(1);
                                }
                            }
                        }
                    }
                }

                connection.commit();
                addCommitStatements(1);
            } catch (SQLException oe) {
                try {
                    connection.rollback();
                    addRollbackStatements(1);
                }
				catch (SQLException ignored) {
				}
				logger.log(Level.FINE,"Exception thrown in DeleteExistingComment",oe);
                throw new SwingBenchException(oe.getMessage());
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException sbe) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw sbe;
        }
    }

    @Override
    public void close() {
    }
}
