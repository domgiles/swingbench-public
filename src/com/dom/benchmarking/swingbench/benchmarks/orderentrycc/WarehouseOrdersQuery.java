package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class WarehouseOrdersQuery extends OrderEntryProcess {
    private static final Logger logger = Logger.getLogger(WarehouseOrdersQuery.class.getName());
    private PreparedStatement ps = null;

    public WarehouseOrdersQuery() {
        super();
    }

    public void init(Map<String, Object> params) {
    }

    public void execute(Map<String, Object> params) throws SwingBenchException {

        PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        long executeStart = System.nanoTime();

        try {
            initJdbcTask();
            Connection connection = pds.getConnection();
            int warehouseID = RandomGenerator.randomInteger(1, 1000);

            try (
                    PreparedStatement ps = connection.prepareStatement(
                            "SELECT order_mode,\n" +
                                    "  orders.warehouse_id,\n" +
                                    "  SUM(order_total),\n" +
                                    "  COUNT(1)\n" +
                                    "FROM orders,\n" +
                                    "  warehouses\n" +
                                    "WHERE orders.warehouse_id   = warehouses.warehouse_id\n" +
                                    "AND warehouses.warehouse_id = ?\n" +
                                    "GROUP BY cube(orders.order_mode, orders.warehouse_id)")
            ) {
                ps.setInt(1, warehouseID);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
            }
        } catch (SQLException sbe) {
            logger.log(Level.FINE, String.format("Exception : ", sbe.getMessage()));
            logger.log(Level.FINEST, "SQLException thrown : ", sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        }
    }

    public void close(Map<String, Object> param) {
    }
}
