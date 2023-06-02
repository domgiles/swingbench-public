package com.dom.benchmarking.swingbench.benchmarks.JSON;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.sql.json.*;

import javax.json.*;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdatePassengerDetails extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(UpdatePassengerDetails.class.getName());
    private static final Lock lock = new ReentrantLock();
    private static final String US_AIRPORTS_FILE = "data/USAirportCodes.txt";
    private static final String NON_US_AIRPORTS_FILE = "data/NonUSAirportCodes.txt";
    private static Long passengerRange = null;
    private static List<String> USAirports = null;
    private static List<String> NonUSAirports = null;
    private OracleRDBMSClient client = null;

    public UpdatePassengerDetails() {
        super();
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        File usAirportsFiles = new File(US_AIRPORTS_FILE);
        File nonUSAirportsFiles = new File(NON_US_AIRPORTS_FILE);
        Properties prop = new Properties();
        prop.put("oracle.soda.sharedMetadataCache", "true");
        client = new OracleRDBMSClient(prop);

        lock.lock();
        if (passengerRange == null) {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT METADATA_VALUE\n" +
                            "FROM PASSENGER_METADATA\n" +
                            "WHERE METADATA_KEY = 'MAX_PASSENGER_COUNT'"
            )) {
                ResultSet rs = ps.executeQuery();
                rs.next();
                passengerRange = rs.getLong(1);
            } catch (SQLException se) {
                logger.log(Level.FINE, "Error initialising UpdatePassengerDetails", se);
                throw new SwingBenchException(se);
            }

        }
        try {
            if (USAirports == null) {
                USAirports = Utilities.cacheFile(usAirportsFiles);
                NonUSAirports = Utilities.cacheFile(nonUSAirportsFiles);
                logger.fine("Reference data loaded for UpdatePassengerDetails transaction");
            }

        } catch (IOException ie) {
            logger.log(Level.FINE, "Error initialising UpdatePassengerDetails", ie);
            throw new SwingBenchException(ie);
        }
        lock.unlock();
    }

    static OracleJsonFactory FACTORY = new OracleJsonFactory();

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {


        long executeStart = 0;
        try {
            Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
            Long passengerKey = RandomGenerator.randomLong(1, passengerRange);
            String USAirport = USAirports.get(RandomGenerator.randomInteger(0, USAirports.size()));
            String nonUSAirport = NonUSAirports.get(RandomGenerator.randomInteger(0, NonUSAirports.size()));
            Long flightDate = RandomGenerator.randomLong(959337930, 1432583142);
            Long returnFlightDate = flightDate + (604800);
            initJdbcTask();

            executeStart = System.nanoTime();
            try {
                OracleDatabase database = client.getDatabase(connection);
                OracleCollection collection = database.openCollection("PASSENGERCOLLECTION");
                OracleDocument doc = collection.find()
                        .key(passengerKey.toString())
                        .getOne();

                addSelectStatements(1);
                if (doc != null) {
                    String version = doc.getVersion();
                    OracleJsonObject obj = doc.getContentAs(OracleJsonObject.class);
                    obj = FACTORY.createObject(obj); // modifiable copy

                    OracleJsonValue flightHistory = obj.get("FlightHistory");
                    OracleJsonArray content;
                    if (flightHistory == null) {
                        content = FACTORY.createArray();
                    } else if (flightHistory.getOracleJsonType() == OracleJsonValue.OracleJsonType.ARRAY) {
                        content = flightHistory.asJsonArray();
                    } else {
                        content = FACTORY.createArray();
                        content.add(flightHistory);
                    }

                    OracleJsonObject newFlight = FACTORY.createObject();
                    newFlight.put("FlightDate", flightDate);
                    newFlight.put("Airport", USAirport);
                    OracleJsonObject returnFlight = FACTORY.createObject();
                    returnFlight.put("FlightDate", returnFlightDate);
                    returnFlight.put("Airport", nonUSAirport);
                    content.add(newFlight);
                    content.add(returnFlight);

                    obj.put("FlightHistory", content);
                    OracleDocument document = database.createDocumentFrom(obj);

                    collection.find().key(passengerKey.toString()).version(version).replaceOne(document);

                    addUpdateStatements(1);
                    connection.commit();
                    addCommitStatements(1);

                }
                addSelectStatements(1);
            } catch (Exception oe) {
                logger.log(Level.FINE, "Exception thrown in GetPassengerDetails()", oe);
                throw new SwingBenchException(oe);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException sbe) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw sbe;
        }
    }

//    private JsonArrayBuilder cloneArray(JsonObject j, String arrayKey) {
//        JsonArrayBuilder ab = Json.createArrayBuilder();
//        for (Map.Entry<String, JsonValue> entry : j.entrySet()) {
//            if (entry.getKey()
//                    .equals(arrayKey)) {
//                if (entry.getValue() instanceof JsonArray) {
//                    JsonArray a = (JsonArray) entry.getValue();
//                    for (JsonValue jv : a) {
//                        ab.add(jv);
//                    }
//                }
//            }
//        }
//        return ab;
//    }
//
//    private JsonObjectBuilder cloneJson(JsonObject j, String excludeString) {
//        JsonObjectBuilder newj = Json.createObjectBuilder();
//        for (Map.Entry<String, JsonValue> entry : j.entrySet()) {
//            if (!entry.getKey()
//                    .equals(excludeString))
//                newj.add(entry.getKey(), entry.getValue());
//        }
//        return newj;
//    }

    @Override
    public void close() {
        // TODO Implement this method
    }
}
