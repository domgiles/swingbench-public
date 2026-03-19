package com.dom.benchmarking.swingbench.benchmarks.offers;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by dgiles on 25/09/2016.
 */
public class FindOffers extends DatabaseTransaction {

    private static final long MIN_CUSTID = 1;
    private static final long MAX_CUSTID = 100000;

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {
    }

    @Override
    public void execute(Map<String, Object> param) throws SwingBenchException {
        long executeStart = 0;
        try {
            Connection connection = (Connection) param.get(SwingBenchTask.JDBC_CONNECTION);

            initJdbcTask();
            Long cid = RandomGenerator.randomLong(MIN_CUSTID, MAX_CUSTID);
            executeStart = System.nanoTime();
            try (OracleCallableStatement cs = (OracleCallableStatement)connection.prepareCall("{? = call OFFER_PROCESSING.GET_OFFERS(?)}")) {
                cs.registerOutParameter(1, OracleTypes.ARRAY, "OFFERS_TABLE_TYPE");
                cs.setLong(2,cid);
                cs.executeUpdate();
                Array result = cs.getARRAY(1);
                Object[] data = (Object[]) result.getArray(); // Get the data but no need to do anything with it.
                addSelectStatements(1);
            } catch (SQLException oe) {
                throw new SwingBenchException(oe.getMessage());
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException sbe) {
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe.getMessage());
        }

    }

    @Override
    public void close(Map<String, Object> param) {

    }
}
