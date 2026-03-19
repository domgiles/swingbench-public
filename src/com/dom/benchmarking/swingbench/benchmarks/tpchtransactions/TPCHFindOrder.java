package com.dom.benchmarking.swingbench.benchmarks.tpchtransactions;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.util.RandomUtilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TPCHFindOrder extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(TPCHFindOrder.class.getName());

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {

    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long executeStart = System.nanoTime();
        try (PreparedStatement ps = connection.prepareStatement("select * from orders where o_orderkey = ?");
             PreparedStatement ps2 = connection.prepareStatement("select last_number from user_sequences where sequence_name='ORDERS_SEQ'")) {
            connection.setReadOnly(true);

            try (ResultSet rs = ps2.executeQuery()) {
                if (rs.next()) {
                    long id_search = RandomUtilities.randomLong(1, rs.getLong(1));
                    ps.setLong(1, id_search);
                    try (ResultSet rs2 = ps.executeQuery()) {
                        if (rs2.next()) {
                            rs2.getInt(2);
                        }
                    }
                }
            }
            addSelectStatements(2);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            logger.log(Level.FINE, String.format("Exception : ", sbe.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    @Override
    public void close(Map<String, Object> param) {

    }
}
