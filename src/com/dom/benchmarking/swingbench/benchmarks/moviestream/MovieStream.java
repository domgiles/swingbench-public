package com.dom.benchmarking.swingbench.benchmarks.moviestream;

import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.OracleUtilities;
import com.dom.util.Utilities;
import oracle.jms.AQjmsQueueConnectionFactory;
import oracle.jms.AQjmsSession;

import javax.jms.*;
import javax.json.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MovieStream extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(MovieStream.class.getName());
    private static final String EVENT_QUEUE = "INTERACTION_EVENT";
    protected static long MIN_CUSTID = 0;
    protected static long MAX_CUSTID = 0;
    protected static volatile String connectedUser = null;
    private static final Object lock = new Object();

    protected static List<String> movieList = null;
    protected static List<String> wordList = null;
    protected static List<String> actorList = null;
    protected static final ArrayList<String> genreList = new ArrayList(List.of("Biography", "Musical", "Animation", "Mystery", "Film-Noir", "Lifestyle", "War", "Romance", "Family", "Western", "Drama", "Documentary", "Horror", "Action", "Sci-Fi", "Thriller", "Adventure", "Fantasy", "Crime", "Comedy"));
    protected static final String[] deviceTypes = new String[]{"mobile", "mobile", "mobile", "mobile", "tablet", "tablet", "tablet", "smarttv", "smarttv", "smarttv", "smarttv", "smarttv", "mobile", "computer", "computer", "smartdevice"};
    protected static final String[] manufacturers = new String[]{"Apple", "Apple", "Samsung", "Samsung", "Samsung", "LG", "Google", "Google"};
    protected static final String[] os = new String[]{"MacOS", "Windows", "Android", "iOS"};
    protected static final String[] cardProviders = new String[]{"visa", "amex", "visa", "visa", "mastercard", "visa"};

    protected static final int NUMBEROFGENRE = genreList.size();

    private void queueEvent(Connection connection, JsonObject event) throws JMSException {

        QueueConnection qc = AQjmsQueueConnectionFactory.createQueueConnection(connection);
        qc.start();
        try (QueueSession ses = qc.createQueueSession(true, Session.CLIENT_ACKNOWLEDGE)) {
            Topic t = ((AQjmsSession) ses).getTopic(connectedUser, EVENT_QUEUE);
            TopicPublisher tp = ((AQjmsSession) ses).createPublisher(t);
            TextMessage tMsg = ses.createTextMessage(event.toString());
            tp.publish(tMsg);
            ses.commit();
        }
    }

    protected void initialiseMovieStream(Map<String, Object> params) throws SQLException, IOException {
        if (connectedUser == null) {
            synchronized (lock) {
                if (connectedUser == null) {
                    logger.log(Level.FINE,"Caching data for benchmark");
                    Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
                    connectedUser = OracleUtilities.getConnectionUserName(connection);
                    getMaxandMinCustID(connection, params);
                    movieList = Utilities.cacheFile(new File("data/movies.txt"));
                    wordList = Utilities.cacheFile(new File("data/1000mostpopularwords.txt"));
                    actorList = Utilities.cacheFile(new File("data/actors.txt"));
                }
            }
        }
    }


    protected void fireLogonEvent(Connection connection, JsonObject customer) throws JMSException, SQLException {
        fireInteractionEvent(connection, customer, "Logon");
    }

    protected void fireInteractionEvent(Connection connection, JsonObject customer, String eventType) throws JMSException, SQLException {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        Long customerID = customer.getJsonNumber("CustomerID").longValue();
        Long deviceID;
        JsonArray di = customer.getJsonArray("DeviceInfo");
        if (di.size() == 0) {
            deviceID = addNewDevice(connection, customerID);
            addInsertStatements(1);
        } else {
            JsonObject de = di.getJsonObject(0);
            deviceID = de.getJsonNumber("DeviceId").longValue();
        }
        double locationLongtitude = RandomGenerator.randomInteger(-100, -18000) / 100d;
        double locationLatitude = RandomGenerator.randomInteger(-9000, -9000) / 100d;

        JsonBuilderFactory factory = Json.createBuilderFactory(null);
        JsonObject jsonLogonEvent = factory.createObjectBuilder().add("EventId", UUID.randomUUID().toString())
                .add("EventType", eventType)
                .add("TimeStamp", df.format(new Date()))
                .add("CustomerID", customer.get("CustomerID"))
                .add("DeviceID", deviceID)
                .add("LocationLatitude", locationLatitude)
                .add("LocationLongtitude", locationLongtitude)
                .build();

        queueEvent(connection, jsonLogonEvent);
    }

    protected Long addNewDevice(Connection connection, Long customerID) throws SQLException {

        Long deviceId = null;
        try (PreparedStatement ps_seq = connection.prepareStatement("select CUST_DEVICE_SEQ.nextval from dual")) {
            ResultSet rsSeq = ps_seq.executeQuery();
            rsSeq.next();
            deviceId = rsSeq.getLong(1);
        }
        addSelectStatements(1);
        try (PreparedStatement ps = connection.prepareStatement("insert into CUSTOMER_DEVICE (DEVICE_ID, CUST_ID, DEVICE_TYPE, DEVICE_MANUFACTURER, DEVICE_OS, DEVICE_VERSION, INITIAL_ACCESS, LAST_ACCESS, ACCESS_COUNT) values (?,?,?,?,?,?,?,?,?)")) {
            ps.setLong(1, deviceId);
            ps.setLong(2, customerID);
            ps.setString(3, deviceTypes[RandomGenerator.randomInteger(0, deviceTypes.length)]);
            ps.setString(4, manufacturers[RandomGenerator.randomInteger(0, manufacturers.length)]);
            ps.setString(5, os[RandomGenerator.randomInteger(0, os.length)]);
            ps.setString(6, String.format("%d.%d", RandomGenerator.randomInteger(1, 16), RandomGenerator.randomInteger(1, 16)));
            ZoneId defaultZoneId = ZoneId.systemDefault();
            LocalDateTime rightNow = LocalDateTime.now();
            java.util.Date date = java.sql.Timestamp.valueOf(rightNow);
            ps.setDate(7, new java.sql.Date(date.getTime()));
            ps.setDate(8, new java.sql.Date(date.getTime()));
            ps.setInt(9, 1);
            ps.execute();
        }
        return deviceId;
    }

    protected JsonObject getMovieAsJson(Connection connection, Long movieID) throws SQLException {
        JsonObject movieJ = null;
        try (PreparedStatement ps = connection.prepareStatement("select json_object(\n" +
                "               'MovieID' VALUE m.MOVIE_ID,\n" +
                "               'Title' VALUE m.TITLE,\n" +
                "               'Budget' VALUE m.BUDGET,\n" +
                "               'Gross' VALUE m.GROSS,\n" +
                "               'ListPrice' VALUE m.LIST_PRICE,\n" +
                "               'Genres' VALUE REPLACE(COALESCE(m.GENRES, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'SKU' VALUE m.SKU,\n" +
                "               'Year' VALUE m.YEAR,\n" +
                "               'OpeningDate' VALUE m.OPENING_DATE,\n" +
                "               'Views' VALUE m.VIEWS,\n" +
                "               'Cast' VALUE REPLACE(COALESCE(m.CAST, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'Crew' VALUE REPLACE(COALESCE(m.CREW, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'Studio' VALUE REPLACE(COALESCE(m.STUDIO, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'MainSubject' VALUE '\"'||COALESCE(m.MAIN_SUBJECT, 'Unknown')||'\"' FORMAT JSON,\n" +
                "               'Awards' VALUE REPLACE(COALESCE(m.AWARDS, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'Nominations' VALUE REPLACE(COALESCE(m.NOMINATIONS, '[]'),'''', '\"') FORMAT JSON,\n" +
                "               'Runtime' VALUE m.RUNTIME,\n" +
                "               'Summary' VALUE '\"'||m.SUMMARY||'\"' FORMAT JSON RETURNING CLOB\n" +
                "           )\n" +
                "from MOVIE m\n" +
                "where movie_id = ?")
        ) {
            ps.setLong(1,movieID);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                Clob clob = rs.getClob(1);
                JsonReader reader = Json.createReader(clob.getCharacterStream());
                movieJ = reader.readObject();
            } catch (Exception ignore) {
                // There's some wierd character encodings in some rows which result in errors. I'll look at the data
            }
        }
        return movieJ;
    }

    protected ArrayList<Integer> browseMovies(Connection connection) throws SQLException, JMSException {
        ArrayList<String> genereListClone = (ArrayList<String>) genreList.clone();
        ArrayList<String> genreBrowseList = new ArrayList<>();
        ArrayList<Integer> browsedMovies = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            int _sampleNumber = RandomGenerator.randomInteger(0, NUMBEROFGENRE - i);
            genreBrowseList.add(genereListClone.get(_sampleNumber));
            genereListClone.remove(_sampleNumber);
        }
        addInsertStatements(1);
        // Try to simulate getting 4 different random sets of movies for users to browse by genere
        try (PreparedStatement ps = connection.prepareStatement("with random_selection as (select *\n" +
                "                          from MOVIE\n" +
                "                          where json_textcontains(GENRES, '$', ?))\n" +
                "select *\n" +
                "from random_selection\n" +
                "SAMPLE (10)")) {
            for (String genere : genreBrowseList) {
                ps.setString(1, genere);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Integer movieID = rs.getInt(1);
                        String movieName = rs.getString(2);
                        browsedMovies.add(movieID);
//                            logger.finest(String.format("Gerere = %s, Movie ID = %d, Movie Name = %s\n", genere, movieID, movieName));
                    }
                }
                addSelectStatements(1);
            }
        }
        return browsedMovies;
    }

    protected JsonObject getCustomerDetailsAsJSON(Connection connection, Long customerID) throws SQLException, JMSException {
        JsonObject customerJ = null;

        try {
            try (PreparedStatement ps = connection.prepareStatement("SELECT json_object('CustomerID' VALUE c.CUST_ID,\n" +
                    "                   'Name' VALUE FIRST_NAME || ' ' || LAST_NAME,\n" +
                    "                   'Email' VALUE EMAIL,\n" +
                    "                   'StreetAddress' VALUE STREET_ADDRESS,\n" +
                    "                   'PostalCode' VALUE POSTAL_CODE,\n" +
                    "                   'City' VALUE CITY,\n" +
                    "                   'StateProvince' VALUE STATE_PROVINCE,\n" +
                    "                   'Country' VALUE COUNTRY,\n" +
                    "                   'CountryCode' VALUE COUNTRY_CODE,\n" +
                    "                   'Continent' VALUE CONTINENT,\n" +
                    "                   'YearsAsCustomer' VALUE YRS_CUSTOMER,\n" +
                    "                   'PromotionResponse' VALUE PROMOTION_RESPONSE,\n" +
                    "                   'LocationLatitude' VALUE LOC_LAT,\n" +
                    "                   'LocationLongtitude' VALUE LOC_LONG,\n" +
                    "                   'PaymentInfo' VALUE COALESCE(\n" +
                    "                           (select json_arrayagg(json_OBJECT('PaymentInformationId' VALUE p.PAYMENT_INFORMATION_ID,\n" +
                    "                                                             'CardNumber' VALUE p.CARD_NUMBER,\n" +
                    "                                                             'Provider' VALUE p.PROVIDER,\n" +
                    "                                                             'BillingStreetAddress' VALUE p.BILLING_STREET_ADDRESS,\n" +
                    "                                                             'BillingPostalCode' VALUE p.BILLING_POSTAL_CODE,\n" +
                    "                                                             'BillingCity' VALUE p.BILLING_CITY,\n" +
                    "                                                             'BillingStateCounty' VALUE p.BILLING_STATE_COUNTY,\n" +
                    "                                                             'BillingCountry' VALUE p.BILLING_COUNTRY,\n" +
                    "                                                             'BillingCountryCode' VALUE p.BILLING_COUNTRY_CODE,\n" +
                    "                                                             'ExpiryDate' VALUE p.EXPIRY_DATE,\n" +
                    "                                                             'DefaultPaymentMethod' VALUE p.DEFAULT_PAYMENT_METHOD)\n" +
                    "                                                 ORDER BY\n" +
                    "                                                 p.EXPIRY_DATE DESC RETURNING CLOB)\n" +
                    "                            from CUSTOMER_PAYMENT_INFORMATION p\n" +
                    "                            where p.CUST_ID = c.CUST_ID), TO_CLOB('[]')) FORMAT JSON,\n" +
                    "                   'DeviceInfo' VALUE COALESCE(\n" +
                    "                           (select json_arrayagg(json_object(\n" +
                    "                                                         'DeviceId' VALUE d.DEVICE_ID,\n" +
                    "                                                         'Type' VALUE d.DEVICE_TYPE,\n" +
                    "                                                         'Manufacturer' VALUE d.DEVICE_MANUFACTURER,\n" +
                    "                                                         'OS' VALUE d.DEVICE_OS,\n" +
                    "                                                         'Version' VALUE d.DEVICE_VERSION,\n" +
                    "                                                         'InitialAccess' VALUE INITIAL_ACCESS,\n" +
                    "                                                         'LastAccess' VALUE LAST_ACCESS,\n" +
                    "                                                         'AccessCount' VALUE ACCESS_COUNT\n" +
                    "                                                         ) order by d.LAST_ACCESS desc RETURNING CLOB)\n" +
                    "                            from CUSTOMER_DEVICE d\n" +
                    "                            where d.CUST_ID = c.CUST_ID), TO_CLOB('[]')) FORMAT JSON,\n" +
                    "                   'FriendsAndFamily' VALUE COALESCE(\n" +
                    "                           (select json_arrayagg(json_object(\n" +
                    "                                                         'FriendFamilyId' VALUE f.FRIEND_FAMILY_ID,\n" +
                    "                                                         'Relationship' VALUE f.RELATIONSHIP,\n" +
                    "                                                         'CustomerTarget' VALUE f.CUSTOMER_TARGET\n" +
                    "                                                         ) RETURNING CLOB)\n" +
                    "                            from CUSTOMER_FRIENDS_AND_FAMILY f\n" +
                    "                            where f.CUSTOMER_SOURCE = c.CUST_ID ), TO_CLOB('[]')) FORMAT JSON\n" +
                    "                   RETURNING CLOB) as CUSTOMER_JSON\n" +
                    "FROM CUSTOMER_CONTACT c\n" +
                    "where c.CUST_ID = ?")) {
                ps.setLong(1, customerID);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Clob clob = rs.getClob(1);
                        JsonReader reader = Json.createReader(clob.getCharacterStream());
                        customerJ = reader.readObject();
                    }
                }
            }
        } catch (SQLException se) {
            logger.log(Level.FINE, String.format("Got exception when attempting to retrieve customer details for customer %d", customerID), se);
            throw se;
        }
        return customerJ;
    }

    protected Long findMovieByNAme(Connection connection, String title) throws SQLException {

        try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM movie WHERE CATSEARCH(title, ?, null)> 0 order by year desc FETCH FIRST 16 rows only")) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Long movieID = rs.getLong(1);
                    String movieName = rs.getString(2);
                    logger.finest(String.format("Actor = %s, Movie ID = %d, Movie Name = %s\n", title, movieID, movieName));
                    return movieID;
                } else {
                    return null;
                }
            }
        }
    }

    protected boolean customerExists(Connection connection, String email) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("select 1 from CUSTOMER_CONTACT where email = ?")) {
            ps.setString(1, email);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    protected void getMaxandMinCustID(Connection connection, Map<String, Object> params) throws SQLException {
        if (MAX_CUSTID == 0) {
            try (PreparedStatement ps = connection.prepareStatement("select metadata_key, metadata_value from MOVIE_METADATA")) {
                logger.fine("Acquiring customer counts from metadata table");
                try (ResultSet vrs = ps.executeQuery()) {
                    while (vrs.next()) {
                        if (vrs.getString(1).equals("MIN_CUST_ID")) {
                            MIN_CUSTID = Long.parseLong(vrs.getString(2));
                        } else if (vrs.getString(1).equals("MAX_CUST_ID")) {
                            MAX_CUSTID = Long.parseLong(vrs.getString(2));
                        }
                    }
                }
                logger.fine("Min CustomerID = " + MIN_CUSTID + ", Max CustomerID = " + MAX_CUSTID);
            }
        }
    }

    private String clobToString(java.sql.Clob data) throws SQLException, IOException {
        final StringBuilder sb = new StringBuilder();
        final BufferedReader br = new BufferedReader(data.getCharacterStream());
        int b;
        while (-1 != (b = br.read())) {
            sb.append((char) b);
        }
        br.close();
        return sb.toString();
    }
}
