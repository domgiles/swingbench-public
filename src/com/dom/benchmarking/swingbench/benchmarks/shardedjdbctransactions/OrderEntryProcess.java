package com.dom.benchmarking.swingbench.benchmarks.shardedjdbctransactions;


import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.util.OracleUtilities;
import oracle.ucp.jdbc.PoolDataSource;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Logger;


public abstract class OrderEntryProcess extends DatabaseTransaction {

    static final String NEW_CUSTOMER = "New Customer";
    static final String CUTOMER_ORDER = "Customer Order";
    static final String CUSTOMER_BROWSE = "Customer Browse";
    static final String GET_CUSTOMER_SEQ_TX = "Get Customer Sequence";
    static final String GET_ORDER_SEQ_TX = "Get Order Sequence";
    static final String INSERT_CUSTOMER_TX = "Insert New Customer";
    static final String INSERT_ITEM_TX = "Insert Order Item";
    static final String INSERT_ORDER_TX = "Insert Order";
    static final String UPDATE_ORDER_TX = "Update Order";
    static final String UPDATE_WAREHOUSE_TX = "Update Warehouse";
    static final String GET_CUSTOMER_DETAILS_TX = "Get Customer Details";
    static final String BROWSE_PENDING_ORDERS = "Browse Pending Orders";
    static final String BROWSE_BY_PROD_ID = "Browse Product by ID";
    static final String BROWSE_BY_CATEGORY_TX = "Browse Products by Category";
    static final String BROWSE_BY_CAT_QUAN_TX = "Browse Products by Quantity";
    static final String BROWSE_BY_ORDER_ID = "Browse Orders by ID";
    static final String BROWSE_ORDER_DETAILS = "Browse Order Details";
    static final String UPDATE_PENDING_ORDERS = "Update Pending Orders";
    static final String GET_ORDER_BY_CUSTOMER_TX = "Browse Order by Customer";
    static final int MIN_CATEGORY = 1;
    static final int MAX_CATEGORY = 199;
    static final int MAX_BROWSE_CATEGORY = 6;
    static final int MAX_CREDITLIMIT = 5000;
    static final int MIN_CREDITLIMIT = 100;
    static final int MIN_SALESID = 145;
    static final int MAX_SALESID = 171;
    static final int MIN_PRODS_TO_BUY = 2;
    static final int MIN_PROD_ID = 1;
    static final int HOUSE_NO_RANGE = 200;
    static final int MAX_PROD_ID = 1000;
    static final int MIN_COST_DELIVERY = 1;
    static final int MAX_COST_DELIVERY = 5;
    static int MIN_WAREHOUSE_ID = 1;
    static int MAX_WAREHOUSE_ID = 1000;
    static final int AWAITING_PROCESSING = 4;
    private static final Logger logger = Logger.getLogger(OrderEntryProcess.class.getName());
    private static final Object orderEntryLock = new Object();
    protected static volatile List<String> sampledCustomerIds = null;
    protected static final String DEFAULT_SAMPLE_SIZE = "1000";
    protected static Integer sampleSize;

    public void sampleCustomerIds(Map<String, Object> params) throws SQLException {
        if (sampledCustomerIds == null) { // load any data you might need (in this case only once)
            synchronized (orderEntryLock) {
                if (sampledCustomerIds == null) {
                    logger.fine("Staring sampling for Customer IDs");
                    sampleSize = Integer.parseInt(checkForNull((String) params.get("CustIDSampleSize"), DEFAULT_SAMPLE_SIZE));
                    sampledCustomerIds = new ArrayList<>();
                    PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
                    String uuid = UUID.randomUUID().toString();
                    String url = "jdbc:oracle:thin:@" + params.get("CATALOGUE_URL");
                    String username = (String) params.get("CATALOGUE_USERNAME");
                    String password = (String) params.get("CATALOGUE_PASSWORD");
                    try (Connection connection = OracleUtilities.getConnection(username, password, url);
//                         PreparedStatement ps = connection.prepareStatement("select customer_Id from customers sample(20) where rownum <= " + sampleSize)) {
                         PreparedStatement ps = connection.prepareStatement("select o.customer_id,c.rw_dbnum from (SELECT   i.customer_id\n" +
                                 "                                 ,ORA_HASH(i.customer_id) AS customer_id_hash from customers i order by dbms_random.value() fetch first "+ sampleSize +" rows only) o,gsmadmin_internal.chunks c\n" +
                                 "WHERE o.customer_id_hash BETWEEN c.low_key AND c.high_key")) {
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String ci = rs.getString(1);
                                sampledCustomerIds.add(ci);
                                String shardNo = String.valueOf(rs.getInt(2));
                                logger.fine("Sample customer Id added is: "+ci+ " Shard No:"+ String.valueOf(rs.getInt(2)));
                                //CustomerIdsmap.put(ci,shardNo);
                            }
                        }
                    }
                    logger.fine("Completed reading sample Customer IDs. Size = " + sampledCustomerIds.size());
                }
            }
        }
    }


    public void logon(Connection connection, String custid) throws SQLException {
        Date currentTime = new Date(System.currentTimeMillis());
        try (PreparedStatement insLogon = connection.prepareStatement("insert into logon (logon_id, customer_id, logon_date) values(logon_seq.nextval,?,?)");) {
            insLogon.setString(1, custid);
            insLogon.setDate(2, currentTime);
            insLogon.executeUpdate();
            connection.commit();
        }
    }


    public List<String> getCustomerDetailsByName(Connection connection, String firstname, String lastName, String uuid) throws SQLException {
        List<String> result = new ArrayList<>();
        try (PreparedStatement custPs = connection.prepareStatement("SELECT   CUSTOMER_ID,      \n" +
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
                "        AND rownum        < 5");) {

            custPs.setString(2, firstname);
            custPs.setString(1, lastName);
            try (ResultSet rs = custPs.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            }
        }
        return result;
    }

    public void getCustomerDetails(Connection connection, String custId) throws SQLException {
        try (PreparedStatement custPs = connection.prepareStatement("SELECT CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, NLS_LANGUAGE, \n" +
                "  NLS_TERRITORY, CREDIT_LIMIT, CUST_EMAIL, ACCOUNT_MGR_ID, CUSTOMER_SINCE, \n" +
                "  CUSTOMER_CLASS, SUGGESTIONS, DOB, MAILSHOT, PARTNER_MAILSHOT, \n" +
                "  PREFERRED_ADDRESS, PREFERRED_CARD \n" +
                "FROM\n" +
                " CUSTOMERS WHERE CUSTOMER_ID = ? AND ROWNUM < 5")) {

            custPs.setString(1, custId);

            try (ResultSet rs = custPs.executeQuery()) {
                rs.next();
            }
        }

    }

    public List<String> getCustomerDetailsByID(Connection connection, String custId) throws SQLException {
        List<String> result = new ArrayList<>();
        try (PreparedStatement custPs = connection.prepareStatement("SELECT CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, NLS_LANGUAGE, \n" +
                "  NLS_TERRITORY, CREDIT_LIMIT, CUST_EMAIL, ACCOUNT_MGR_ID, CUSTOMER_SINCE, \n" +
                "  CUSTOMER_CLASS, SUGGESTIONS, DOB, MAILSHOT, PARTNER_MAILSHOT, \n" +
                "  PREFERRED_ADDRESS, PREFERRED_CARD \n" +
                "FROM\n" +
                " CUSTOMERS WHERE CUSTOMER_ID = ? AND ROWNUM < 5")) {

            custPs.setString(1, custId);

            try (ResultSet rs = custPs.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            }
            return result;
        }
    }

    public void getAddressDetails(Connection connection, String custId) throws SQLException {
        try (PreparedStatement addPs = connection.prepareStatement("SELECT   ADDRESS_ID,      \n" + "          CUSTOMER_ID,      \n" + "          DATE_CREATED,      \n" + "          HOUSE_NO_OR_NAME,      \n" + "          STREET_NAME,      \n" + "          TOWN,      \n" + "          COUNTY,      \n" + "          COUNTRY,      \n" + "          POST_CODE,      \n" + "          ZIP_CODE      \n" + "        FROM ADDRESSES       \n" + "        WHERE customer_id = ? \n" + "        AND rownum        < 5")) {
            addPs.setString(1, custId);
            try (ResultSet rs = addPs.executeQuery()) {
                rs.next();
            }
        }
    }

    public void getCardDetails(Connection connection, String custId) throws SQLException {
        try (PreparedStatement cardPs = connection.prepareStatement("SELECT CARD_ID,\n" +
                "          CUSTOMER_ID,\n" +
                "          CARD_TYPE,\n" +
                "          CARD_NUMBER,\n" +
                "          EXPIRY_DATE,\n" +
                "          IS_VALID,\n" +
                "          SECURITY_CODE\n" +
                "        FROM card_details       \n" +
                "        WHERE CUSTOMER_ID = ?      \n" +
                "        AND rownum        < 5")) {
            cardPs.setString(1, custId);
            try (ResultSet rs = cardPs.executeQuery()) {
                rs.next();
            }
        }
    }

    public List<BigDecimal> getOrdersByCustomer(Connection connection, String custId) throws SQLException {
        List<BigDecimal> orders = new ArrayList<>();

        try (
                PreparedStatement orderPs3 = connection.prepareStatement("SELECT o.ORDER_ID,     \n" +
                        "        o.ORDER_DATE,     \n" +
                        "        o.ORDER_MODE,     \n" +
                        "        o.CUSTOMER_ID,     \n" +
                        "        o.ORDER_STATUS,     \n" +
                        "        o.ORDER_TOTAL,     \n" +
                        "        o.SALES_REP_ID,     \n" +
                        "        o.PROMOTION_ID,     \n" +
                        "        o.WAREHOUSE_ID,     \n" +
                        "        o.DELIVERY_TYPE,     \n" +
                        "        o.COST_OF_DELIVERY,     \n" +
                        "        o.WAIT_TILL_ALL_AVAILABLE,     \n" +
                        "        o.DELIVERY_ADDRESS_ID,     \n" +
                        "        o.CUSTOMER_CLASS,     \n" +
                        "        o.CARD_ID,     \n" +
                        "        o.INVOICE_ADDRESS_ID           \n" +
                        "      from orders o          \n" +
                        "      where o.customer_id = ?           \n" +
                        "      and rownum < 5");
        ) {
            orderPs3.setString(1, custId);
            try (ResultSet rs = orderPs3.executeQuery()) {
                while (rs.next()) { // Will only ever be a maximum of 5
                    orders.add(rs.getBigDecimal(1));
                }
            }
        }
        return orders;
    }


    public double getProductDetailsByID(Connection connection, int prodID) throws SQLException {
        double price = 0;
        try (PreparedStatement prodPs = connection.prepareStatement("select product_id, product_name, product_description, category_id, weight_class, supplier_id, product_status, list_price, min_price, catalog_url from  product_information where product_id = ?")) {
            prodPs.setInt(1, prodID);
            try (ResultSet rs = prodPs.executeQuery()) {
                while (rs.next()) {
                    price = rs.getDouble(8);
                }
            }
        }
        return price;
    }

    public List<ProductDetails> getProductDetailsByCategory(Connection connection, int catID) throws SQLException {
        List<ProductDetails> result = new ArrayList<>();
        try (PreparedStatement catPs = connection.prepareStatement("select   products.PRODUCT_ID,           \n" +
                "              PRODUCT_NAME,           \n" +
                "              PRODUCT_DESCRIPTION,           \n" +
                "              CATEGORY_ID,           \n" +
                "              WEIGHT_CLASS,           \n" +
                "              WARRANTY_PERIOD,           \n" +
                "              SUPPLIER_ID,           \n" +
                "              PRODUCT_STATUS,           \n" +
                "              LIST_PRICE,           \n" +
                "              MIN_PRICE,           \n" +
                "              CATALOG_URL           \n" +
                "      from products           \n" +
                "      where products.category_id = ?           \n" +
                "      and rownum < 5")) {

            catPs.setInt(1, catID);
            try (ResultSet rs = catPs.executeQuery();) {
                while (rs.next()) {
                    result.add(new ProductDetails(rs.getInt(1)));
                }
            }
        }
        return result;
    }

    public static <T> T checkForNull(T value, T mydefault) {
        return (value == null) ? mydefault : value;
    }


}
