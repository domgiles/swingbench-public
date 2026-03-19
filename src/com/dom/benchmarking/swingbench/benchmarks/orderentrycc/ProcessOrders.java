package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;


import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import oracle.jdbc.OracleShardingKey;
import oracle.jdbc.OracleType;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;


public class ProcessOrders extends OrderEntryProcess {

    private static final Logger logger = Logger.getLogger(ProcessOrders.class.getName());
    private static final Object lock = new Object();

    public ProcessOrders() {
    }

    @Override
    public void close(Map<String, Object> param) {
    }

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        this.initialiseBenchmark(params);
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        String threadCountryAlignment = (String) params.get("THREAD_COUNTRY_ALIGNMENT");
        String shardedConnection = (String) params.get("USE_SHARDED_CONNECTION");
        PoolDataSource pds = (PoolDataSource) params.get(SwingBenchTask.CONNECTION_POOL);
        String myCountry = ThreadToCountryCode.getCountryCode(Thread.currentThread().getName(), countryCodes);
        Connection connection = null;

        initJdbcTask();
        long executeStart = System.nanoTime();
        try {
            if (shardedConnection.equals("true")) {
                OracleShardingKey daffKey = pds.createShardingKeyBuilder().subkey(myCountry, OracleType.VARCHAR2).build();
                connection = pds.createConnectionBuilder().shardingKey(daffKey).build();
            } else {
                connection = pds.getConnection();
            }
            if (!threadCountryAlignment.equals("true")) {
                myCountry = countryCodes.get(RandomGenerator.randomInteger(0, countries.size()));
            }
            try (PreparedStatement
                         orderPs3 = connection.prepareStatement(
                    "WITH need_to_process AS            \n" +
                            "                          (SELECT order_id,\n" +
                            "                            /* we're only looking for unprocessed orders */            \n" +
                            "                            customer_id\n" +
                            "                          FROM orders\n" +
                            "                          WHERE order_status <= 4\n" +
                            "                          AND country_CODE = ?\n" +
                            "                          AND rownum         <  10\n" +
                            "                          )\n" +
                            "                        SELECT o.order_id,               \n" +
                            "                          oi.line_item_id,               \n" +
                            "                          oi.product_id,               \n" +
                            "                          oi.unit_price,               \n" +
                            "                          oi.quantity,               \n" +
                            "                          o.order_mode,               \n" +
                            "                          o.order_status,               \n" +
                            "                          o.order_total,               \n" +
                            "                          o.sales_rep_id,               \n" +
                            "                          o.promotion_id,               \n" +
                            "                          c.customer_id,               \n" +
                            "                          c.cust_first_name,               \n" +
                            "                          c.cust_last_name,               \n" +
                            "                          c.credit_limit,               \n" +
                            "                          c.cust_email,               \n" +
                            "                          o.order_date            \n" +
                            "                        FROM orders o,            \n" +
                            "                          need_to_process ntp,            \n" +
                            "                          customers c,            \n" +
                            "                          order_items oi            \n" +
                            "                        WHERE ntp.order_id = o.order_id            \n" +
                            "                        AND c.customer_id  = o.customer_id            \n" +
                            "                        AND c.country_code  = ?            \n" +
                            "                        AND o.country_code = ?     \n" +
                            "                        AND oi.country_code = ?        \n" +
                            "                        and oi.order_id (+) = o.order_id");
                 PreparedStatement updoPs = connection.prepareStatement("update /*+ index(orders, order_pk) */ \n" +
                         "orders \n" +
                         "set order_status = ? \n" +
                         "where order_id = ?" +
                         "and country_code = ?");
            ) {
                orderPs3.setString(1, myCountry);
                orderPs3.setString(2, myCountry);
                orderPs3.setString(3, myCountry);
                orderPs3.setString(4, myCountry);
                ResultSet rs = orderPs3.executeQuery();
                if (rs.next()) {
                    long orderID = rs.getLong(1);
                    addSelectStatements(1);
                    thinkSleep(); //update the order
                    updoPs.setLong(1, RandomGenerator.randomInteger(AWAITING_PROCESSING + 1, ORDER_PROCESSED));
                    updoPs.setLong(2, orderID);
                    updoPs.setString(3, myCountry);
                    updoPs.execute();
                    addUpdateStatements(1);
                    connection.commit();
                    addCommitStatements(1);
                }
            }
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
        } catch (SQLException sbe) {
            handleException(connection, sbe);
            processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
            throw new SwingBenchException(sbe);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
