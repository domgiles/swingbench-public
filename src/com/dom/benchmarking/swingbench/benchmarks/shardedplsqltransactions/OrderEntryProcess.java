package com.dom.benchmarking.swingbench.benchmarks.shardedplsqltransactions;


import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.util.Utilities;
import oracle.jdbc.OracleShardingKey;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;


public abstract class OrderEntryProcess extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(OrderEntryProcess.class.getName());
    static final int SELECT_STATEMENTS = 0;
    static final int INSERT_STATEMENTS = 1;
    static final int UPDATE_STATEMENTS = 2;
    static final int DELETE_STATEMENTS = 3;
    static final int COMMIT_STATEMENTS = 4;
    static final int ROLLBACK_STATEMENTS = 5;
    static final int SLEEP_TIME_LOC = 6;
    protected static final String COUNTIES_FILE = "data/counties.txt";
    protected static final String COUNTRIES_FILE = "data/countries.txt";
    protected static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    protected static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    protected static final String NLS_FILE = "data/nls.txt";
    protected static final String TOWNS_FILE = "data/towns.txt";
    protected static volatile List<NLSSupport> nlsInfo = new ArrayList<NLSSupport>();
    protected static volatile List<String> counties = null;
    protected static volatile List<String> countries = null;
    protected static volatile List<String> firstNames = null;
    protected static volatile List<String> lastNames = null;
    protected static volatile List<String> nlsInfoRaw = null;
    protected static volatile List<String> towns = null;
    private static boolean commitClientSide = true;
    protected static final Object orderEntryLock = new Object();
    protected static List<String> sampledCustomerIds = null;
    protected static final String DEFAULT_SAMPLE_SIZE = "1000";
    protected static Integer sampleSize;


    protected void loadSampleData(Map<String, Object> params) throws FileNotFoundException, IOException {
        if ((firstNames == null)) { // load any data you might need (in this case only once)
            synchronized (orderEntryLock) {
                if (firstNames == null) {
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
                    logger.fine("Completed reading sample data files");
                }
            }
        }
    }

    protected void sampleCustomerIds(Map<String, Object> params) throws SQLException {
        if (sampledCustomerIds == null) { // load any data you might need (in this case only once)
            synchronized (orderEntryLock) {
                if (sampledCustomerIds == null) {
                    logger.fine("Staring sampling for Customer IDs");
                    sampleSize = Integer.parseInt(checkForNull((String) params.get("CustIDSampleSize"), DEFAULT_SAMPLE_SIZE));
                    sampledCustomerIds = new ArrayList<>();
                    PoolDataSource ods = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
                    String uuid = UUID.randomUUID().toString();
                    OracleShardingKey key = ods.createShardingKeyBuilder().subkey(uuid, JDBCType.VARCHAR).build();
                    try (Connection connection = ods.createConnectionBuilder().shardingKey(key).build();
                         PreparedStatement ps = connection.prepareStatement("select customer_Id from customers where rownum <= " + sampleSize);) {
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                String ci = rs.getString(1);
                                sampledCustomerIds.add(ci);
                            }
                        }
                    }
                    logger.fine("Completed reading sample Customer IDs. Size = " + sampledCustomerIds.size());
                }
            }
        }
    }



    public void commit(Connection connection) throws SQLException {
        if (commitClientSide) {
            connection.commit();
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
            return result;
        } catch (Exception e) {
            throw new Exception("Unable to parse string returned from OrderEntry Package. String = " + data, e);
        }
    }

    public static <T> T checkForNull(T value, T mydefault) {
        return (value == null) ? mydefault : value;
    }

    protected class NLSSupport {

        String language = null;
        String territory = null;

    }

}
