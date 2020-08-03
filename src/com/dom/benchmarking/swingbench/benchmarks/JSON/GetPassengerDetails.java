package com.dom.benchmarking.swingbench.benchmarks.JSON;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GetPassengerDetails extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(GetPassengerDetails.class.getName());
    private static final Lock lock = new ReentrantLock();
    private static Long passengerRange = null;
    private OracleRDBMSClient client = null;

    public GetPassengerDetails() {
        super();
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        Properties prop = new Properties();
        prop.put("oracle.soda.sharedMetadataCache", "true");
        client = new OracleRDBMSClient(prop);

        lock.lock();
        if (passengerRange == null) {
            logger.fine("Reference data loaded for GetPassengerDetails transaction");
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT MAX(us.last_number)\n" +
                            "FROM user_sequences us\n" +
                            "WHERE us.sequence_name = 'PASSENGER_SEQ'")) {
                ResultSet rs = ps.executeQuery();
                rs.next();
                passengerRange = rs.getLong(1);
            } catch (SQLException se) {
                logger.log(Level.SEVERE, "Error initialising GetPassengerDetails", se);
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

            Long passengerKey = RandomGenerator.randomLong(1, passengerRange);

            initJdbcTask();

            executeStart = System.nanoTime();
            try {
                OracleDatabase database = client.getDatabase(connection);
                OracleCollection collection = database.openCollection("PASSENGERCOLLECTION");
                OracleDocument doc = collection.find()
                        .key(passengerKey.toString())
                        .getOne();
                if (doc != null) {
                    String json = doc.getContentAsString();
                }
                addSelectStatements(1);
            } catch (Exception oe) {
                throw new SwingBenchException(oe);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException sbe) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw sbe;
        }
    }

    @Override
    public void close() {
        // TODO Implement this method
    }
}
