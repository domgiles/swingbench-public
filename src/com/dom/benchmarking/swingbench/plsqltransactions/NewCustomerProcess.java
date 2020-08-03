package com.dom.benchmarking.swingbench.plsqltransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleTypes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NewCustomerProcess extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(NewCustomerProcess.class.getName());
    private static final String NAMES_FILE = "data/names.txt";
    private static final String NLS_FILE = "data/nls.txt";
    private static ArrayList<String> firstNames = null;
    private static ArrayList<String> lastNames = null;
    private static ArrayList<NLSSupport> nlsInfo = null;
    private static final Object lock = new Object();

    public NewCustomerProcess() {
    }

    public void init(Map<String, Object> params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);


        boolean initCompleted = false;

        if ((firstNames == null) || !initCompleted) { // load any data you might need (in this case only once)
            synchronized (lock) {
                if (firstNames == null) {
                    firstNames = new ArrayList<>();
                    lastNames = new ArrayList<>();
                    nlsInfo = new ArrayList<>();

                    String value = (String) params.get("SOE_NAMESDATA_LOC");
                    File namesFile = new File((value == null) ? NAMES_FILE : value);
                    value = (String) params.get("SOE_NLSDATA_LOC");
                    File nlsFile = new File((value == null) ? NLS_FILE : value);

                    try {
                        BufferedReader br = new BufferedReader(new FileReader(namesFile));
                        String data = null;
                        String firstName = null;
                        String lastName = null;

                        while ((data = br.readLine()) != null) {
                            StringTokenizer st = new StringTokenizer(data, ",");
                            firstName = st.nextToken();
                            lastName = st.nextToken();
                            firstNames.add(firstName);
                            lastNames.add(lastName);
                        }
                        br.close();
                        value = (String) params.get("SOE_NLSDATA_LOC");
                        br = new BufferedReader(new FileReader(nlsFile));

                        while ((data = br.readLine()) != null) {
                            NLSSupport nls = new NLSSupport();
                            StringTokenizer st = new StringTokenizer(data, ",");
                            nls.language = st.nextToken();
                            nls.territory = st.nextToken();
                            nlsInfo.add(nls);
                        }
                        br.close();
                        this.parseCommitClientSide(params);
                        this.setCommitClientSide(connection, commitClientSide);
                        logger.fine("Successfully loaded transaction data");
                    } catch (java.io.FileNotFoundException fne) {
                        logger.log(Level.SEVERE, "Unable to open file : " + namesFile.getAbsolutePath() + " : " + nlsFile.getAbsolutePath(), fne);
                        throw new SwingBenchException(fne);
                    } catch (java.io.IOException ioe) {
                        logger.log(Level.SEVERE, "IO problems opening " + namesFile.getAbsolutePath(), ioe);
                        throw new SwingBenchException(ioe);
                    } catch (SQLException se) {
                        logger.log(Level.SEVERE, "Failed while attempting to set commit profile", se);
                        throw new SwingBenchException(se);
                    }
                    logger.fine("Completed initialisation of NewCustomerProcess");
                }

                initCompleted = true;
            }
        }
    }

    public void execute(Map params) throws SwingBenchException {
        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        int queryTimeOut = 60;

        if (params.get(SwingBenchTask.QUERY_TIMEOUT) != null) {
            queryTimeOut = (Integer) (params.get(SwingBenchTask.QUERY_TIMEOUT));
        }

        String firstName = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
        String lastName = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
        NLSSupport nls = nlsInfo.get(RandomGenerator.randomInteger(0, nlsInfo.size()));

        long executeStart = System.nanoTime();
        initJdbcTask();
        try {
            try (CallableStatement cs = connection.prepareCall("{? = call orderentry.newcustomer(?,?,?,?,?,?)}")) {
                cs.registerOutParameter(1, OracleTypes.VARCHAR);
                cs.setString(2, firstName);
                cs.setString(3, lastName);
                cs.setString(4, nls.language);
                cs.setString(5, nls.territory);
                cs.setInt(6, (int) this.getMinSleepTime());
                cs.setInt(7, (int) this.getMaxSleepTime());
                cs.setQueryTimeout(queryTimeOut);
                cs.executeUpdate();
                parseInfoArray(cs.getString(1));
                this.commit(connection);
            } catch (Exception se) {
                throw new SwingBenchException(se);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException ex) {
            addRollbackStatements(1);
            try {
                connection.rollback();
            } catch (SQLException ignore) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw ex;
        }
    }

    public void close() {
    }

    private class NLSSupport {

        String language = null;
        String territory = null;

    }

}
