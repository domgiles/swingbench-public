package com.dom.benchmarking.swingbench.plsqltransactions;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.Map;
import java.util.logging.Logger;

import oracle.jdbc.OracleTypes;


public class WarehouseOrdersQuery extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(WarehouseOrdersQuery.class.getName());

    public WarehouseOrdersQuery() {
        super();
    }

    public void init(Map params) {
    }

    public void execute(Map params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        int warehouseID = RandomGenerator.randomInteger(1, 1000);

        long executeStart = System.nanoTime();
        initJdbcTask();
        try {
            try (CallableStatement cs = connection.prepareCall("{? = call orderentry.WarehouseOrdersQuery(?,?,?)}")) {
                cs.registerOutParameter(1, OracleTypes.VARCHAR);
                cs.setInt(2, warehouseID);
                cs.setInt(3, (int) this.getMinSleepTime());
                cs.setInt(4, (int) this.getMaxSleepTime());
                cs.executeUpdate();
                parseInfoArray(cs.getString(1));
            } catch (Exception se) {
                throw new SwingBenchException(se);
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SwingBenchException ex) {
            addRollbackStatements(1);
            try {
                connection.rollback();
            } catch (SQLException ignored) {
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw ex;
        }
    }

    public void close() {
    }
}
