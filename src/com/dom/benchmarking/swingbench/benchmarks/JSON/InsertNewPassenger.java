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
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InsertNewPassenger extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(InsertNewPassenger.class.getName());
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static final String COUNTRIES_FILE = "data/countries.txt";
    private static final String US_AIRPORTS_FILE = "data/USAirportCodes.txt";
    private static final String NON_US_AIRPORTS_FILE = "data/NonUSAirportCodes.txt";
    private static final int BIO_SIZE = 128;
    private static List<String> firstNames = null;
    private static List<String> lastNames = null;
    private static List<String> countries = null;
    private static List<String> USAirports = null;
    private static List<String> NonUSAirports = null;
    private static final Lock lock = new ReentrantLock();
    private OracleRDBMSClient client = null;

    public InsertNewPassenger() {
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
        File countriesFiles = new File(COUNTRIES_FILE);
        File usAirportsFiles = new File(US_AIRPORTS_FILE);
        File nonUSAirportsFiles = new File(NON_US_AIRPORTS_FILE);

        lock.lock();
        try {
            if (firstNames == null) {
                firstNames = Utilities.cacheFile(firstNamesFile);
                lastNames = Utilities.cacheFile(lastNamesFile);
                countries = Utilities.cacheFile(countriesFiles);
                USAirports = Utilities.cacheFile(usAirportsFiles);
                NonUSAirports = Utilities.cacheFile(nonUSAirportsFiles);
                logger.fine("Reference data loaded for InsertNewPassenger transaction");
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
            String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
            String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
            String nationality = countries.get(RandomGenerator.randomInteger(0, countries.size()));
            String USAirport = USAirports.get(RandomGenerator.randomInteger(0, USAirports.size()));
            String nonUSAirport = NonUSAirports.get(RandomGenerator.randomInteger(0, NonUSAirports.size()));
            Long flightDate = RandomGenerator.randomLong(959337930, 1432583142);
            Long returnFlightDate = flightDate + (604800);

            initJdbcTask();

            executeStart = System.nanoTime();
            try {
                OracleDatabase database = client.getDatabase(connection);
                OracleCollection collection = database.openCollection("PASSENGERCOLLECTION");
                JsonBuilderFactory jsonfactory = Json.createBuilderFactory(null);
                JsonObject passengerJSON = jsonfactory.createObjectBuilder()
                        .add("ID", UUID.randomUUID().toString())
                        .add("FirstName", lastName)
                        .add("LastName", firstName)
                        .add("Nationality", nationality)
                        .add("DOB", RandomGenerator.randomLong(-1262304000, 1432583142))
                        .add("PassportNo", RandomGenerator.randomInteger(100000000, 999999999))
                        .add("Flagged", "N")
                        .add("BioData", RandomGenerator.randomAlpha(BIO_SIZE, BIO_SIZE))
                        .add("FlightHistory", jsonfactory.createArrayBuilder()
                                .add(jsonfactory.createObjectBuilder()
                                        .add("FlightDate", flightDate).add("Airport", USAirport))
                                .add(jsonfactory.createObjectBuilder()
                                        .add("FlightDate", returnFlightDate).add("Airport", nonUSAirport)))
                        .build();

                OracleDocument document = database.createDocumentFromString(passengerJSON.toString());
                collection.insert(document);
                addInsertStatements(1);
                connection.commit();
                addCommitStatements(1);
            } catch (OracleException | SQLException oe) {
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
        // TODO Implement this method
    }
}
