package com.dom.benchmarking.swingbench.benchmarks.orderentryjdbcac;


import com.dom.benchmarking.swingbench.constants.Constants;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.util.RandomUtilities;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    static int MIN_WAREHOUSE_ID = 1;
    static int MAX_WAREHOUSE_ID = 1000;
    static long MIN_ORDERID = 1;
    static long MAX_ORDERID = 146610;
    static final int AWAITING_PROCESSING = 4;
    static final int ORDER_PROCESSED = 10;
    static long MIN_CUSTID = 0;
    static long MAX_CUSTID = 0;
    private static final Logger logger = Logger.getLogger(OrderEntryProcess.class.getName());
    private static final Object lock = new Object();


    public void logon(Connection connection, long custid) throws SQLException {
        // This is run this way because we want to commit a logon but have a seperate context from the main transaction for AC.
        try (CallableStatement insLogon = connection.prepareCall("{call autonomousLogon(?,?,?)}");
             PreparedStatement ps = connection.prepareStatement("select logon_seq.nextval from dual")) {
            long seqVal = 0;
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                seqVal = rs.getLong(1);
            };
            insLogon.setLong(1, custid);
            insLogon.setLong(2, seqVal);
            insLogon.setDate(3, new Date(System.currentTimeMillis()));
            insLogon.executeUpdate();
        }
    }

    public void getMaxandMinCustID(Connection connection, Map<String, Object> params) throws SQLException {
        if (MAX_CUSTID == 0) { // load any data you might need (in this case only once)
            synchronized (lock) {
                if (MAX_CUSTID == 0) {
                    String minCI = (String) params.get(Constants.SOEMINCUSTOMERID);
                    String maxCI = (String) params.get(Constants.SOEMAXCUSTOMERID);
                    if ((minCI != null) && (maxCI != null)) {
                        logger.fine("Acquiring customer counts from environment variables");
                        MIN_CUSTID = Long.parseLong(minCI);
                        MAX_CUSTID = Long.parseLong(maxCI);
                    } else {
                        try (PreparedStatement ps = connection.prepareStatement("select 1 from user_tables where table_name =  'ORDERENTRY_METADATA'");
                             PreparedStatement vps = connection.prepareStatement("select metadata_key, metadata_value from ORDERENTRY_METADATA");
                        ) {
                            try (ResultSet trs = ps.executeQuery()) {
                                if (trs.next()) {
                                    logger.fine("Acquiring customer counts from metadata table");
                                    try (ResultSet vrs = vps.executeQuery()) {
                                        while (vrs.next()) {
                                            switch (vrs.getString(1)) {
                                                case "SOE_MIN_ORDER_ID":
                                                    MIN_ORDERID = Long.parseLong(vrs.getString(2));
                                                    break;
                                                case "SOE_MAX_ORDER_ID":
                                                    MAX_ORDERID = Long.parseLong(vrs.getString(2));
                                                    break;
                                                case "SOE_MIN_CUSTOMER_ID":
                                                    MIN_CUSTID = Long.parseLong(vrs.getString(2));
                                                    break;
                                                case "SOE_MAX_CUSTOMER_ID":
                                                    MAX_CUSTID = Long.parseLong(vrs.getString(2));
                                                    break;
                                            }
                                        }
                                    }
                                    logger.fine("Min CustomerID = " + MIN_CUSTID + ", Max CustomerID = " + MAX_CUSTID);
                                } else {
                                    try (PreparedStatement mmPs = connection.prepareStatement("select min(customer_id), max(customer_id) from customers");
                                         PreparedStatement moPs = connection.prepareStatement("select min(order_id), max(order_id) from orders");
                                         ResultSet crs = mmPs.executeQuery()) {
                                        logger.fine("Acquiring customer counts from database");
                                        if (crs.next()) {
                                            MIN_CUSTID = crs.getLong(1);
                                            MAX_CUSTID = crs.getLong(2);
                                        }
                                        logger.fine("Min CustomerID = " + MIN_CUSTID + ", Max CustomerID = " + MAX_CUSTID);
                                        try (ResultSet rs = moPs.executeQuery()) {
                                            if (rs.next()) {
                                                MIN_ORDERID = rs.getLong(1);
                                                MAX_ORDERID = rs.getLong(2);
                                            }
                                            logger.fine("Min CustomerID = " + MIN_CUSTID + ", Max CustomerID = " + MAX_CUSTID);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void getMaxandMinWarehouseID(Connection connection) throws SQLException {
        try (PreparedStatement mmPs = connection.prepareStatement("select min(warehouse_id), max(warehouse_id) from warehouses");
             ResultSet rs = mmPs.executeQuery()) {
            if (rs.next()) {
                MIN_WAREHOUSE_ID = rs.getInt(1);
                MAX_WAREHOUSE_ID = rs.getInt(2);
            }
        }
    }


    public List<Long> getCustomerDetailsByName(Connection connection, String firstname, String lastName) throws
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
                        "        AND rownum        < 5")) {
            custPs.setString(2, firstname);
            custPs.setString(1, lastName);
            try (ResultSet rs = custPs.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
            }
        }
        return result;
    }

    public void getCustomerDetails(Connection connection, long custId) throws SQLException {
        try (PreparedStatement custPs = connection.prepareStatement(
                "SELECT CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, NLS_LANGUAGE, \n" +
                        "  NLS_TERRITORY, CREDIT_LIMIT, CUST_EMAIL, ACCOUNT_MGR_ID, CUSTOMER_SINCE, \n" +
                        "  CUSTOMER_CLASS, SUGGESTIONS, DOB, MAILSHOT, PARTNER_MAILSHOT, \n" +
                        "  PREFERRED_ADDRESS, PREFERRED_CARD \n" +
                        "FROM\n" +
                        " CUSTOMERS WHERE CUSTOMER_ID = ? AND ROWNUM < 5");) {

            custPs.setLong(1, custId);

            try (ResultSet rs = custPs.executeQuery()) {
                rs.next();
            }
        }
    }

    public void getAddressDetails(Connection connection, long custId) throws SQLException {
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
                        "        AND rownum        < 5")) {
            addPs.setLong(1, custId);
            try (ResultSet rs = addPs.executeQuery()) {
                rs.next();
            }
        }
    }

    public void getCardDetails(Connection connection, long custId) throws SQLException {
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
                        "        AND rownum        < 5")) {
            cardPs.setLong(1, custId);
            try (ResultSet rs = cardPs.executeQuery()) {
                rs.next();
            }
        }
    }

    public List<Long> getOrdersByCustomer(Connection connection, long custId) throws SQLException {
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
                        "      order by ORDER_DATE" +
                        "      fetch first 5 rows only");) {
            orderPs3.setLong(1, custId);
            try (ResultSet rs = orderPs3.executeQuery()) {
                while (rs.next()) { // Will only ever be a maximum of 5
                    orders.add(rs.getLong(1));
                }
            }
            return orders;
        }
    }


    public double getProductDetailsByID(Connection connection, int prodID) throws SQLException {
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
                             " order BY products.product_id\n" +
                             "    fetch first 15 rows only");
        ) {
            prodPs.setInt(1, prodID);
            try (ResultSet rs = prodPs.executeQuery()) {
                while (rs.next()) {
                    price = rs.getDouble(9);
                }
            }
        }
        return price;
    }

    public List<ProductDetails> getProductDetailsByCategory(Connection connection, int catID) throws
            SQLException {
        List<ProductDetails> result = new ArrayList<>();
        int warehouseId = RandomUtilities.randomInteger(MIN_WAREHOUSE_ID, MAX_WAREHOUSE_ID);
        try (
                PreparedStatement catPs = connection.prepareStatement(
                        "select   products.PRODUCT_ID,           \n" +
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
                                "      from products,           \n" +
                                "      inventories           \n" +
                                "      where products.category_id = ?           \n" +
                                "      and inventories.product_id = products.product_id           \n" +
                                "      and inventories.warehouse_id = ?           \n" +
                                "      order by products.product_id" +
                                "      fetch first 5 rows only")) {
//                                "      and rownum < 4")) {
            catPs.setInt(1, catID);
            catPs.setInt(2, warehouseId);

            try (ResultSet rs = catPs.executeQuery()) {
                while (rs.next()) {
                    result.add(new ProductDetails(rs.getInt(1), warehouseId, rs.getInt(12)));
                }
            }
            return result;
        }
    }

//    public int getProductQuantityByID(Connection connection, int ID) throws SQLException {
//        int quantity = 0;
//        try (PreparedStatement prodqPs =
//                     connection.prepareStatement(" select  p.product_id, product_name, product_description, category_id, weight_class, supplier_id,  product_status,  list_price, min_price, catalog_url, quantity_on_hand, warehouse_id from  product_information p, inventories i where   i.product_id = ?  and   i.product_id = p.product_id")) {
//            prodqPs.setInt(1, ID);
//            try (ResultSet rs = prodqPs.executeQuery()) {
//                if (rs.next()) {
//                    quantity = rs.getInt(11);
//                }
//            }
//        }
//        return quantity;
//    }

//    public void getProductQuantityByCategory(Connection connection, int catID) throws SQLException {
//        try (PreparedStatement catqPs =
//                     connection.prepareStatement("select p.product_id, product_name, product_description, category_id, weight_class, supplier_id, product_status, list_price, min_price, catalog_url,  quantity_on_hand, warehouse_id from   product_information p,  inventories i where  category_id = ? and i.product_id = p.product_id")) {
//            catqPs.setInt(1, catID);
//            try (ResultSet rs = catqPs.executeQuery()) {
//                rs.next();
//            }
//        }
//    }

//    public void getOrderByID(Connection connection, int orderID) throws SQLException {
//        try (PreparedStatement orderPs =
//                     connection.prepareStatement(" select  order_id,   order_date,   order_mode,   customer_id,   order_status,   order_total,   sales_rep_id,   promotion_id from    orders where    order_id = ?")) {
//            orderPs.setInt(1, orderID);
//            try (ResultSet rs = orderPs.executeQuery()) {
//                rs.next();
//            }
//        }
//    }

//    public void getOrderDetailsByOrderID(Connection connection, long orderID) throws SQLException {
//        try (PreparedStatement orderPs2 =
//                     connection.prepareStatement(" SELECT  o.order_id,   line_item_id,   product_id,   unit_price,   quantity,   order_mode,   order_status,   order_total,   sales_rep_id,   promotion_id,   c.customer_id,   cust_first_name,   cust_last_name,   credit_limit,   cust_email  FROM    orders o ,   order_items oi,   customers c WHERE    o.order_id = oi.order_id  and   o.customer_id = c.customer_id  and   o.order_id = ?")) {
//            orderPs2.setLong(1, orderID);
//            try (ResultSet rs = orderPs2.executeQuery()) {
//                rs.next();
//            }
//        }
//    }

}
