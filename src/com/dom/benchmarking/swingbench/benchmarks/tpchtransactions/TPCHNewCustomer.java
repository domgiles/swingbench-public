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

public class TPCHNewCustomer extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(TPCHNewCustomer.class.getName());

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {

    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long executeStart = System.nanoTime();
        try (PreparedStatement ps = connection.prepareStatement("select CUSTOMER_SEQ.nextval from dual");
             PreparedStatement ps2 = connection.prepareStatement("insert into CUSTOMER(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) values "+
                                                                     "(?,?,?,?,?,?,?,?)");
             PreparedStatement ps3 = connection.prepareStatement("select * from CUSTOMER where C_CUSTKEY = ?")) {

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long newID = rs.getLong(1);
                    ps2.setLong(1, newID);
                    ps2.setString(2, RandomUtilities.randomAlpha(10,25));
                    ps2.setString(3, RandomUtilities.randomAlpha(15,40));
                    ps2.setInt(4, RandomUtilities.randomInteger(1,255));
                    ps2.setString(5, RandomUtilities.randomAlpha(15,15));
                    ps2.setDouble(6, RandomUtilities.randomDouble(10.0f, 200.0f));
                    ps2.setString(7, RandomUtilities.randomAlpha(5, 10));
                    ps2.setString(8, RandomUtilities.randomAlpha(20,110));
                    ps2.executeUpdate();
                    ps3.setLong(1, newID);
                    try (ResultSet rs2 = ps3.executeQuery()) {
                        if (rs2.next()) {
                            rs2.getString(2);
                        }
                    }
                }
                connection.commit();
            }
            addInsertStatements(1);
            addSelectStatements(2);
            addCommitStatements(1);
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
