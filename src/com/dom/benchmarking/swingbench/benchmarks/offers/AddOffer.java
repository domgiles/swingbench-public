package com.dom.benchmarking.swingbench.benchmarks.offers;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by dgiles on 25/09/2016.
 */
public class AddOffer extends DatabaseTransaction {

    /*
    procedure insert_offer(p_customer_id offers.customer_id%type,
    p_fname offers.fname%type,
    p_lname offers.lname%type,
    p_phone offers.phone%type,
    p_offer offers.offer%type,
    p_Ctimestamp offers.Ctimestamp%type);
    */

    private static final long MIN_CUSTID = 1;
    private static final long MAX_CUSTID = 100000;

    private static final Logger logger = Logger.getLogger(AddOffer.class.getName());
    private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
    private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
    private static final String ADJECTIVES_FILE = "data/1000mostpopularadjectives.txt";

    private static List<String> firstNames = null;
    private static List<String> lastNames = null;
    private static List<String> adjectives = null;

    private long startDate = 1991991;
    private long endDate = 2209999;


    private static final Lock lock = new ReentrantLock();

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        String value = (String) param.get("SOE_FIRST_NAMES_LOC");
        File firstNamesFile = new File((value == null) ? FIRST_NAMES_FILE : value);
        value = (String) param.get("SOE_LAST_NAMES_LOC");
        File lastNamesFile = new File((value == null) ? LAST_NAMES_FILE : value);
        value = (String) param.get("SOE_ADJECTIVES_LOC");
        File adjectivesFile = new File((value == null) ? ADJECTIVES_FILE : value);

        lock.lock();
        try {
            if (firstNames == null) {
                firstNames = Utilities.cacheFile(firstNamesFile);
                lastNames = Utilities.cacheFile(lastNamesFile);
                adjectives = Utilities.cacheFile(adjectivesFile);
                logger.fine("Reference data loaded for addOffer transaction");
            }

        } catch (IOException ie) {
            logger.log(Level.SEVERE, "Unable to open data seed files : ", ie);
            throw new SwingBenchException(ie);
        }
        lock.unlock();
    }

    @Override
    public void execute(Map<String, Object> param) throws SwingBenchException {
        Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);
        int queryTimeOut = 60;

        if (param.get(SwingBenchTask.QUERY_TIMEOUT) != null) {
            queryTimeOut = ((Integer) (param.get(SwingBenchTask.QUERY_TIMEOUT))).intValue();
        }

        long executeStart = System.nanoTime();
        long recordedTime = 0;

        try {
            CallableStatement cs = null;
            try {
                cs = connection.prepareCall("{call offer_processing.insert_offer(?,?,?,?,?,?)}");
                cs.setQueryTimeout(queryTimeOut);
                Long cid = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
                String fn = firstNames.get(RandomGenerator.randomInteger(0, firstNames.size()));
                String ln = lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()));
                String tn = RandomGenerator.randomAlpha(1, 9);
                for (int i = 0; i < 6; i++) {
                    String of = adjectives.get(RandomGenerator.randomInteger(0, adjectives.size()));
                    cs.setLong(1, cid);
                    cs.setString(2, fn);
                    cs.setString(3, ln);
                    cs.setString(4, tn);
                    cs.setString(5, of);
                    cs.setTimestamp(6, new Timestamp(RandomGenerator.randomLong(startDate, endDate)));
                    cs.executeUpdate();
                    addInsertStatements(1);
                }
                connection.commit();
                addCommitStatements(1);
                cs.close();
            } catch (SQLException se) {
                try {
                    cs.close();
                } catch (Exception e) {
                }
                throw new SwingBenchException(se);
                //throw new SwingBenchException(se.getMessage());
            } catch (Exception e) {
                throw new SwingBenchException(e.getMessage());
            }

            recordedTime = System.nanoTime() - executeStart;
            processTransactionEvent(new JdbcTaskEvent(this, getId(), recordedTime, true, getInfoArray()));
        } catch (SwingBenchException ex) {
            recordedTime = System.nanoTime() - executeStart;
            processTransactionEvent(new JdbcTaskEvent(this, getId(), recordedTime, false, getInfoArray()));
            throw new SwingBenchException(ex);
        }

    }

    @Override
    public void close(Map<String, Object> param) {

    }
}
