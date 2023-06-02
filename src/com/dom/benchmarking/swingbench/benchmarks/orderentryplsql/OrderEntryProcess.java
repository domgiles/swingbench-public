package com.dom.benchmarking.swingbench.benchmarks.orderentryplsql;


import com.dom.benchmarking.swingbench.constants.Constants;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;

import java.sql.*;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class OrderEntryProcess extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(OrderEntryProcess.class.getName());
    static final int MIN_WAREHOUSE_ID = 1;
    static final int MAX_WAREHOUSE_ID = 1000;
    static long MIN_CUSTID = 0;
    static volatile long MAX_CUSTID = 0;
    static long MIN_ORDERID = 0;
    static long MAX_ORDERID = 0;

    private static boolean isInitCompleted = false;
    static boolean commitClientSide = false;
    private final static Object lock = new Object();

    public void commit(Connection connection) throws SQLException {
        if (commitClientSide) {
            connection.commit();
            addCommitStatements(1);
        }
    }

    void setCommitClientSide(Connection connection, boolean commitClientSide) throws SQLException {
        CallableStatement cs = connection.prepareCall("{call orderentry.setPLSQLCOMMIT(?)}");
        cs.setString(1, Boolean.toString(!commitClientSide));
        cs.executeUpdate();
        cs.close();
    }

    void parseCommitClientSide(Map params) {
        this.commitClientSide = Boolean.parseBoolean((String) params.get(SwingBenchTask.COMMIT_CLIENT_SIDE));
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
                                            if (vrs.getString(1).equals("SOE_MIN_ORDER_ID")) {
                                                MIN_ORDERID = Long.parseLong(vrs.getString(2));
                                            } else if (vrs.getString(1).equals("SOE_MAX_ORDER_ID")) {
                                                MAX_ORDERID = Long.parseLong(vrs.getString(2));
                                            } else if (vrs.getString(1).equals("SOE_MIN_CUSTOMER_ID")) {
                                                MIN_CUSTID = Long.parseLong(vrs.getString(2));
                                            } else if (vrs.getString(1).equals("SOE_MAX_CUSTOMER_ID")) {
                                                MAX_CUSTID = Long.parseLong(vrs.getString(2));
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


    public int[] parseInfoArray(String data) throws Exception {
        int[] result = new int[7];
        try {
            StringTokenizer st = new StringTokenizer(data, ",");
            result[SELECT_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[INSERT_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[UPDATE_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[DELETE_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[COMMIT_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[ROLLBACK_STATEMENTS] = Integer.parseInt(st.nextToken());
            result[SLEEP_TIME_LOC] = Integer.parseInt(st.nextToken());
            setDMLArray(result);
            return result;
        } catch (Exception e) {
            throw new Exception("Unable to parse string returned from OrderEntry Package. String = " + data, e);
        }
    }

    void setIsStatic(boolean isStatic, Connection connection) throws SQLException {
        if (isStatic) {
            synchronized (lock) {
                if (!isInitCompleted) {
                    logger.fine("Runing OrderEntry benchmark in constant mode" + isInitCompleted);
                    CallableStatement cs = connection.prepareCall("{call orderentry.setIsStatic(?)}");
                    cs.setString(1, Boolean.toString(isStatic));
                    cs.execute();
                    cs.close();
                    isInitCompleted = true;
                }
            }
        }
    }

//    public void setDMLstatements(int[] dmlArray) {
//        addSelectStatements(dmlArray[SELECT_STATEMENTS]);
//        addInsertStatements(dmlArray[INSERT_STATEMENTS]);
//        addUpdateStatements(dmlArray[UPDATE_STATEMENTS]);
//        addDeleteStatements(dmlArray[DELETE_STATEMENTS]);
//        addCommitStatements(dmlArray[COMMIT_STATEMENTS]);
//        addRollbackStatements(dmlArray[ROLLBACK_STATEMENTS]);
//    }

}
