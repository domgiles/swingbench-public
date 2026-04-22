package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.util.RandomUtilities;
import com.dom.util.Utilities;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class OrderEntryProcess extends DatabaseTransaction {

    static final int MIN_CATEGORY = 1;
    static final int MAX_CATEGORY = 199;
    static final int MAX_BROWSE_CATEGORY = 24;
    static final int MAX_CREDITLIMIT = 5000;
    static final int MIN_CREDITLIMIT = 100;
    static final int MIN_SALESID = 145;
    static final int MAX_SALESID = 171;
    static final int MIN_PRODS_TO_BUY = 2;
    static final int MAX_PRODS_TO_BUY = 6;
    static final int MIN_PROD_ID = 1;
    static final int HOUSE_NO_RANGE = 200;
    static final int MAX_PROD_ID = 1000;
    static final int MIN_COST_DELIVERY = 1;
    static final int MAX_COST_DELIVERY = 5;
    static final int AWAITING_PROCESSING = 4;
    static final int ORDER_PROCESSED = 10;
    static final String COUNTIES_FILE = "data/counties.txt";
    static final String COUNTRIES_FILE = "data/countries.txt";
    static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    static final String NLS_FILE = "data/nls.txt";
    static final String TOWNS_FILE = "data/towns.txt";
    private static final String COUNTRY_CODES_FILE = "data/iso_country_codes.txt";
    private static final Logger logger = Logger.getLogger(OrderEntryProcess.class.getName());
    private static final Object lock = new Object();
    public static List<String> countryCodes = null;
    static List<NewCustomerProcess.NLSSupport> nlsInfo = new ArrayList<>();
    static List<String> counties = null;
    static List<String> countries = null;
    static List<String> firstNames = null;
    static List<String> lastNames = null;
    static List<String> nlsInfoRaw = null;
    static List<String> towns = null;
    static int MIN_WAREHOUSE_ID = 1;
    static int MAX_WAREHOUSE_ID = 1000;
    private static Map<String, int[]> warehouseMap = new ConcurrentHashMap<>();
    private static volatile boolean dataInitialised = false;

    public static List<String> getCountryCodes() throws Exception {
        File nlsFile = new File(COUNTRY_CODES_FILE);
        return Utilities.cacheFile(nlsFile);
    }


    public void logon(Connection connection, String countryCode, long custid) throws SQLException {
        Date currentTime = new Date(System.currentTimeMillis());
        try (PreparedStatement insLogon = connection.prepareStatement("insert into logon (logon_id, country_code, customer_id, logon_date) values(?, ?, ?, ?)")) {
            insLogon.setLong(1, LogonIdManager.getNextLogonId(countryCode));
            insLogon.setString(2, countryCode);
            insLogon.setLong(3, custid);
            insLogon.setDate(4, currentTime);
            insLogon.executeUpdate();
        }
        connection.commit();
    }

    public void initialiseBenchmark(Map<String, Object> params) throws SwingBenchException {
        if (!dataInitialised) {
            synchronized (lock) {
                if (!dataInitialised) {
                    logger.log(Level.FINE, "Initialising Benchmark Data");
                    PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
                    String shardedConnection = (String) params.get("USE_SHARDED_CONNECTION");

                    String value = (String) params.get("SOE_FIRST_NAMES_LOC");
                    File firstNamesFile = new File((value == null) ? FIRST_NAMES_FILE : value);
                    value = (String) params.get("SOE_LAST_NAMES_LOC");
                    File lastNamesFile = new File((value == null) ? LAST_NAMES_FILE : value);
                    value = (String) params.get("SOE_NLSDATA_LOC");
                    File nlsFile = new File((value == null) ? NLS_FILE : value);
                    value = (String) params.get("SOE_TOWNS_LOC");
                    File townsFile = new File((value == null) ? TOWNS_FILE : value);
                    value = (String) params.get("SOE_COUNTIES_LOC");
                    File countiesFile = new File((value == null) ? COUNTIES_FILE : value);
                    value = (String) params.get("SOE_COUNTRIES_LOC");
                    File countriesFile = new File((value == null) ? COUNTRIES_FILE : value);

                    try {
                        firstNames = Utilities.cacheFile(firstNamesFile);
                        lastNames = Utilities.cacheFile(lastNamesFile);
                        nlsInfoRaw = Utilities.cacheFile(nlsFile);
                        counties = Utilities.cacheFile(countiesFile);
                        towns = Utilities.cacheFile(townsFile);
                        countries = Utilities.cacheFile(countriesFile);

                        for (String rawData : nlsInfoRaw) {
                            NLSSupport nls = new NLSSupport();
                            StringTokenizer st = new StringTokenizer(rawData, ",");
                            nls.language = st.nextToken();
                            nls.territory = st.nextToken();
                            nlsInfo.add(nls);
                        }
                        Connection connection = pds.getConnection();
                        if (connection != null) {
                            CustomerIdManager.loadRanges(connection);
                            CustomerIdManager.checkCustomerRanges(connection);
                            AddressIdManager.loadRanges(connection);
                            AddressIdManager.checkAddressRanges(connection);
                            CardIdManager.loadRanges(connection);
                            CardIdManager.checkCardRanges(connection);
                            LogonIdManager.loadRanges(connection);
                            LogonIdManager.checkLogonRanges(connection);
                            populateWarehouseMap(connection);
                            this.countryCodes = CustomerIdManager.getInitializedCountryCodes();
                        }
                        connection.close();
                    } catch (java.io.IOException fne) {
                        logger.log(Level.SEVERE, "Unable to open data seed files : ", fne);
                        throw new SwingBenchException(fne);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Unexpected error during initialisation : ", e);
                        throw new SwingBenchException(e);
                    }

                }
            }
            dataInitialised = true;
        }
    }

    public int getRandomWarehouseId(String countryCode) {
        int[] range = warehouseMap.get(countryCode);
        if (range == null) {
            return RandomUtilities.randomInteger(MIN_WAREHOUSE_ID, MAX_WAREHOUSE_ID + 1);
        }
        return RandomUtilities.randomInteger(range[0], range[1] + 1);
    }

    public void populateWarehouseMap(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("select country_code, min(warehouse_id), max(warehouse_id) from warehouses group by country_code");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String countryCode = rs.getString(1);
                int min = rs.getInt(2);
                int max = rs.getInt(3);
                warehouseMap.put(countryCode, new int[]{min, max});
            }
        }
    }

    public List<Long> getCustomerDetailsByName(Connection connection, String firstname, String lastName, String country) throws
            SQLException {
        List<Long> result = new ArrayList<>();
        try (PreparedStatement
                     custPs = connection.prepareStatement(
                "SELECT   CUSTOMER_ID,      \n" +
                        "          CUST_FIRST_NAME,      \n" +
                        "          CUST_LAST_NAME,      \n" +
                        "          NLS_LANGUAGE,      \n" +
                        "          NLS_TERRITORY,      \n" +
                        "          CREDIT_LIMIT,      \n" +
                        "          CUST_EMAIL,      \n" +
                        "          ACCOUNT_MGR_ID,      \n" +
                        "          CUSTOMER_SINCE,      \n" +
                        "          CUSTOMER_CLASS,      \n" +
                        "          SUGGESTIONS,      \n" +
                        "          DOB,      \n" +
                        "          MAILSHOT,      \n" +
                        "          PARTNER_MAILSHOT,      \n" +
                        "          PREFERRED_ADDRESS,\n" +
                        "          PREFERRED_CARD\n" +
                        "        FROM CUSTOMERS      \n" +
                        "        WHERE lower(cust_last_name) = lower(?) \n" +
                        "        AND lower(cust_first_name) = lower(?)      \n" +
                        "        AND country_code = ?\n" +
                        "        AND rownum        < 5")) {
            custPs.setString(2, firstname);
            custPs.setString(1, lastName);
            custPs.setString(3, country);
            try (ResultSet rs = custPs.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
            }
        }
        return result;
    }

    public void getCustomerDetails(Connection connection, long custId, String countryCode) throws SQLException, SwingBenchException {
        try (PreparedStatement custPs = connection.prepareStatement(
                "SELECT CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, NLS_LANGUAGE, \n" +
                        "  NLS_TERRITORY, CREDIT_LIMIT, CUST_EMAIL, ACCOUNT_MGR_ID, CUSTOMER_SINCE, \n" +
                        "  CUSTOMER_CLASS, SUGGESTIONS, DOB, MAILSHOT, PARTNER_MAILSHOT, \n" +
                        "  PREFERRED_ADDRESS, PREFERRED_CARD \n" +
                        "FROM\n" +
                        " CUSTOMERS WHERE CUSTOMER_ID = ? AND country_code = ? AND ROWNUM < 5 ")) {

            custPs.setLong(1, custId);
            custPs.setString(2, countryCode);

            try (ResultSet rs = custPs.executeQuery()) {
                if (!rs.next()) {
                    throw new SwingBenchException(String.format(
                            "No customer found for CUSTOMER_ID=%d, COUNTRY_CODE=%s",
                            custId, countryCode), SwingBenchException.MINORERROR);
                }
            }
        }
    }

    void handleException(Connection connection, Exception se) {
        logger.log(Level.FINE, String.format("Exception : %s", se.getMessage()));
        logger.log(Level.FINEST, "SQLException thrown : %s", se);
        try {
            addRollbackStatements(1);
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    void handleException(SwingBenchException se) {
        if (se.getSeverity() == SwingBenchException.MINORERROR) {
            logger.log(Level.FINEST, "SQLException thrown : %s", se);
        } else {
            logger.log(Level.FINE, String.format("Exception : %s", se.getMessage()));
        }
    }

    public long getAddressDetails(Connection connection, long custId, String country_code) throws SQLException, SwingBenchException {
        try (PreparedStatement
                     addPs = connection.prepareStatement(
                "SELECT   ADDRESS_ID,      \n" +
                        "          CUSTOMER_ID,      \n" +
                        "          DATE_CREATED,      \n" +
                        "          HOUSE_NO_OR_NAME,      \n" +
                        "          STREET_NAME,      \n" +
                        "          TOWN,      \n" +
                        "          COUNTY,      \n" +
                        "          COUNTRY,      \n" +
                        "          POST_CODE,      \n" +
                        "          ZIP_CODE      \n" +
                        "        FROM ADDRESSES       \n" +
                        "        WHERE customer_id = ? \n" +
                        "        AND country_code = ? \n" +
                        "        AND rownum        < 5")) {
            addPs.setLong(1, custId);
            addPs.setString(2, country_code);
            try (ResultSet rs = addPs.executeQuery()) {
                if (!rs.next()) {
                    throw new SwingBenchException(String.format(
                            "No address found for CUSTOMER_ID=%d, COUNTRY_CODE=%s",
                            custId, country_code), SwingBenchException.MINORERROR);
                }
                return rs.getLong(1);
            }
        }
    }

    public long getCardDetails(Connection connection, long custId, String country_code) throws SQLException, SwingBenchException {
        try (PreparedStatement cardPs = connection.prepareStatement(
                "SELECT CARD_ID,\n" +
                        "          CUSTOMER_ID,\n" +
                        "          CARD_TYPE,\n" +
                        "          CARD_NUMBER,\n" +
                        "          EXPIRY_DATE,\n" +
                        "          IS_VALID,\n" +
                        "          SECURITY_CODE\n" +
                        "        FROM card_details       \n" +
                        "        WHERE CUSTOMER_ID = ?      \n" +
                        "        AND COUNTRY_CODE = ?      \n" +
                        "        AND rownum        < 5")) {
            cardPs.setLong(1, custId);
            cardPs.setString(2, country_code);
            try (ResultSet rs = cardPs.executeQuery()) {
                if (!rs.next()) {
                    throw new SwingBenchException(String.format(
                            "No card found for CUSTOMER_ID=%d, COUNTRY_CODE=%s",
                            custId, country_code), SwingBenchException.MINORERROR);
                }
                return rs.getLong(1);
            }
        }
    }

    public List<Long> getOrdersByCustomer(Connection connection, long custId, String country_code) throws SQLException {
        List<Long> orders = new ArrayList<>();
        try (PreparedStatement orderPs3 = connection.prepareStatement(
                "SELECT ORDER_ID,     \n" +
                        "        ORDER_DATE,     \n" +
                        "        ORDER_MODE,     \n" +
                        "        CUSTOMER_ID,     \n" +
                        "        ORDER_STATUS,     \n" +
                        "        ORDER_TOTAL,     \n" +
                        "        SALES_REP_ID,     \n" +
                        "        PROMOTION_ID,     \n" +
                        "        WAREHOUSE_ID,     \n" +
                        "        DELIVERY_TYPE,     \n" +
                        "        COST_OF_DELIVERY,     \n" +
                        "        WAIT_TILL_ALL_AVAILABLE,     \n" +
                        "        DELIVERY_ADDRESS_ID,     \n" +
                        "        CUSTOMER_CLASS,     \n" +
                        "        CARD_ID,     \n" +
                        "        INVOICE_ADDRESS_ID           \n" +
                        "      from orders           \n" +
                        "      where customer_id = ?           \n" +
                        "      and country_code = ? \n" +
                        "      and rownum < 5")) {
            orderPs3.setLong(1, custId);
            orderPs3.setString(2, country_code);
            try (ResultSet rs = orderPs3.executeQuery()) {
                while (rs.next()) { // Will only ever be a maximum of 5
                    orders.add(rs.getLong(1));
                }
            }
            return orders;
        }
    }

    public double getProductDetailsByID(Connection connection, int prodID, String myCountry) throws SQLException {
        double price = 0;

        try (PreparedStatement prodPs =
                     connection.prepareStatement("select products.product_id," +
                             " product_name," +
                             " product_description," +
                             " category_id," +
                             " weight_class," +
                             " warranty_period," +
                             " supplier_id," +
                             " product_status," +
                             " list_price," +
                             " min_price," +
                             " catalog_url," +
                             " quantity_on_hand" +
                             " from  products, inventories" +
                             " where inventories.product_id = products.product_id" +
                             " and products.product_id = ?" +
                             " and inventories.country_code = ?" +
                             " and rownum < 15");
        ) {
            prodPs.setInt(1, prodID);
            prodPs.setString(2, myCountry);
            try (ResultSet rs = prodPs.executeQuery()) {
                while (rs.next()) {
                    price = rs.getDouble(9);
                }
            }
        }
        return price;
    }

    public List<ProductDetails> getProductDetailsByCategory(Connection connection, int catID, String countryCode) throws
            SQLException {
        List<ProductDetails> result = new ArrayList<>();
        int warehouseId = getRandomWarehouseId(countryCode);
        try (
                PreparedStatement catPs = connection.prepareStatement(
                        "select /*+ LEADING(p i) USE_NL(i) NLJ_BATCHING(i) INDEX(p PROD_CATEGORY_IX) INDEX(i INVENTORY_PK) */ " +
                                "              p.PRODUCT_ID,           \n" +
                                "              PRODUCT_NAME,           \n" +
                                "              PRODUCT_DESCRIPTION,           \n" +
                                "              CATEGORY_ID,           \n" +
                                "              WEIGHT_CLASS,           \n" +
                                "              WARRANTY_PERIOD,           \n" +
                                "              SUPPLIER_ID,           \n" +
                                "              PRODUCT_STATUS,           \n" +
                                "              LIST_PRICE,           \n" +
                                "              MIN_PRICE,           \n" +
                                "              CATALOG_URL,           \n" +
                                "              QUANTITY_ON_HAND \n" +
                                "      from products p,           \n" +
                                "      inventories i           \n" +
                                "      where p.category_id = ?           \n" +
                                "      and i.product_id = p.product_id           \n" +
                                "      and i.warehouse_id = ?           \n" +
                                "      and i.country_code = ?           \n" +
                                "      and rownum < 4")) {
            catPs.setInt(1, catID);
            catPs.setInt(2, warehouseId);
            catPs.setString(3, countryCode);

            try (ResultSet rs = catPs.executeQuery()) {
                while (rs.next()) {
                    result.add(new ProductDetails(rs.getInt(1), warehouseId, rs.getInt(12)));
                }
            }
            return result;
        }
    }

    class NLSSupport {

        String language = null;
        String territory = null;

    }


}
