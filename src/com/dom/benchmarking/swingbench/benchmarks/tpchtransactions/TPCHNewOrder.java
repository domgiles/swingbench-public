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
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TPCHNewOrder extends DatabaseTransaction {

    private static final Logger logger = Logger.getLogger(TPCHNewOrder.class.getName());

    @Override
    public void init(Map<String, Object> param) throws SwingBenchException {

    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {

        Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
        initJdbcTask();

        long executeStart = System.nanoTime();
        try (PreparedStatement ps = connection.prepareStatement("select ORDERS_SEQ.nextval from dual");
             PreparedStatement ps2 = connection.prepareStatement("insert into ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) values "+
                     "(?,?,?,?,?,?,?,?,?)");
             PreparedStatement ps3 = connection.prepareStatement("select * from ORDERS where O_ORDERKEY = ?")) {

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long newID = rs.getLong(1);
                    ps2.setLong(1, newID);
                    ps2.setLong(2, RandomUtilities.randomLong(1,1500));
                    ps2.setString(3, "O");
                    ps2.setDouble(4, RandomUtilities.randomDouble(10.5,100.1));
                    //Ucccck!! Don't do the following
                    Date t =  RandomUtilities.randomDate(new Date(), 100);
                    ps2.setDate(5, new java.sql.Date(t.getTime()));
                    ps2.setString(6, RandomUtilities.randomAlpha(5, 15));
                    ps2.setString(7, RandomUtilities.randomAlpha(5, 15));
                    ps2.setLong(8, RandomUtilities.randomLong(1,10));
                    ps2.setString(9, RandomUtilities.randomAlpha(20,75));
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
