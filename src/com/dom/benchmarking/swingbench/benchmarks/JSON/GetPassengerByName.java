package com.dom.benchmarking.swingbench.benchmarks.JSON;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.sql.json.OracleJsonObject;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GetPassengerByName extends DatabaseTransaction {
    private static final Logger logger = Logger.getLogger(GetPassengerDetails.class.getName());
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static List<String> firstNames = null;
    private static List<String> lastNames = null;
    private static final Lock lock = new ReentrantLock();
    private static Long passengerRange = null;
    private OracleRDBMSClient client = null;

    public GetPassengerByName() {
        super();
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        Properties prop = new Properties();
        prop.put("oracle.soda.sharedMetadataCache", "true");
        client = new OracleRDBMSClient(prop);

        String value = (String) params.get("SOE_FIRST_NAMES_LOC");
        File firstNamesFile = new File((value == null) ? FIRST_NAMES_FILE : value);
        value = (String) params.get("SOE_LAST_NAMES_LOC");
        File lastNamesFile = new File((value == null) ? LAST_NAMES_FILE : value);

        lock.lock();
        try {
            if (firstNames == null) {
                firstNames = Utilities.cacheFile(firstNamesFile);
                lastNames = Utilities.cacheFile(lastNamesFile);
                logger.fine("Reference data loaded for GetPassengerByName transaction");
            }

        } catch (IOException ie) {
            logger.log(Level.SEVERE, "Unable to open data seed files : ", ie);
            throw new SwingBenchException(ie);
        }
        lock.unlock();
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        long executeStart = 0;
        try {
            Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
            initJdbcTask();

            String randomFirstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
            String randomLastName = firstNames.get(RandomGenerator.randomInteger(0, lastNames.size()));

            executeStart = System.nanoTime();
            try {
                OracleDatabase database = client.getDatabase(connection);
//                OracleDocument filterSpec = database.createDocumentFromString(String.format("{ \"FirstName\" : \"%s\"}", randomFirstName));
                OracleDocument filterSpec = database.createDocumentFromString(String.format("{ \"FirstName\" : \"%s\", \"LastName\" : \"%s\"}", randomFirstName, randomLastName));
                OracleCollection collection = database.openCollection("PASSENGERCOLLECTION");
                OracleCursor c = collection.find().filter(filterSpec).getCursor();
                if (c.hasNext()) {
                    OracleDocument resultDoc = c.next();
                    OracleJsonObject json = resultDoc.getContentAs(OracleJsonObject.class);
                }
                c.close();
                addSelectStatements(1);
            } catch (oracle.soda.OracleException | IOException oe) {
                logger.log(Level.FINE, "Exception thrown in GetPassengerDetails()", oe);
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
