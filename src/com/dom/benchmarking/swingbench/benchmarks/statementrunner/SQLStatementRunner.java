package com.dom.benchmarking.swingbench.benchmarks.statementrunner;

import com.dom.benchmarking.swingbench.jaxb.SQLStatements.SQLStatementListType;
import com.dom.benchmarking.swingbench.kernel.StatementRunner;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;

import java.io.File;
import java.sql.Connection;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by dgiles on 13/05/2016.
 */
public class SQLStatementRunner extends StatementRunner {

    private static final Logger logger = Logger.getLogger(SQLStatementRunner.class.getName());
    private RunMode runMode = RunMode.SERIAL_ONCE;
    private SQLStatementListType sqlStatements = null;
    private Connection connection = null;

    @Override
    public void init(Map<String, Object> params) throws SwingBenchException {
        try {
            String statementConfigFile = checkForNull((String) params.get("STATEMENTS_CONFIG_FILE"), "queries.xml");
            String runModeString = checkForNull((String) params.get("STATEMENTS_RUN_MODE"), "serial_once");
            runMode = RunMode.getModeForString(runModeString);
            File file = new File(statementConfigFile);

            if (file.exists()) {
                logger.fine(String.format("Config file, %s ,containing statements is present", statementConfigFile));
                sqlStatements = initStatements(file);
                connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);
            } else {
                throw new SwingBenchException("File containing SQL statements does not exist", SwingBenchException.UNRECOVERABLEERROR);
            }
            params.put("COUNTER", 0);
        } catch (Exception e) {
            throw new SwingBenchException(e);
        }
    }

    @Override
    public void execute(Map<String, Object> params) throws SwingBenchException {
        if (this.runMode == RunMode.SERIAL_ONCE) {
            int counter = (int) params.get("COUNTER");
            if (counter >= sqlStatements.getSQLStatement().size()) {
                throw new SwingBenchException("Finished running SQL Statements", SwingBenchException.UNRECOVERABLEERROR);
            }
        }
        executeStatements(connection, sqlStatements, runMode, params);
    }

    @Override
    public void close(Map<String, Object> param) {

    }
}
