package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.datagen.constants.Constants;
import com.dom.datagen.kernel.utilities.Random;
import com.dom.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryArticlePerPeople extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(QueryArticlePerPeople.class.getName());

	private static final Lock lock = new ReentrantLock();
	private static Boolean jdvMode = null;

	public QueryArticlePerPeople() {
		super();
	}

	@Override
	public void init(Map<String, Object> params) throws SwingBenchException {
		Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

		lock.lock();
		if (jdvMode == null) {
			try (Statement s = connection.createStatement()) {
				try (ResultSet rs = s.executeQuery("SELECT METADATA_VALUE\n" +
												   "FROM ARTICLES_METADATA\n" +
												   "WHERE METADATA_KEY = 'MODE'")) {
					if (rs.next()) {
						jdvMode = "JDV".equalsIgnoreCase("JDV");
					} else {
						jdvMode = false;
					}
				}
			}
			catch (SQLException se) {
				logger.log(Level.SEVERE, "Error initialising DeleteExistingArticle", se);
				throw new SwingBenchException(se);
			}
		}
		lock.unlock();

	}

	@Override
	public void execute(Map<String, Object> params) throws SwingBenchException {
		long executeStart = 0;
		try {
			Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

			initJdbcTask();

			executeStart = System.nanoTime();
			try {
				if(jdvMode) {
					// REALLY slow: select name, count(*) from articles_dv a nested data.peoples[*] columns (name) group by name
					try (PreparedStatement p = connection.prepareStatement("select p.name, count(*) from people p join people_articles_association paa on p.id=paa.people_id group by p.name")) {
						try (ResultSet r = p.executeQuery()) {
							while (r.next()) {
								r.getString(1);
								r.getLong(2);
							}
							addSelectStatements(1);
						}
					}
				}
				else {
					// create multivalue index midx on articles a (a.data.peoples.name.string()); ?
					try (PreparedStatement p = connection.prepareStatement("select name, count(*) from articles a nested data.peoples[*] columns (name) group by name")) {
						try (ResultSet r = p.executeQuery()) {
							while (r.next()) {
								r.getString(1);
								r.getLong(2);
							}
							addSelectStatements(1);
						}
					}
				}
			}
			catch (SQLException oe) {
				logger.log(Level.FINE, "Exception thrown in QueryArticlePerPeople", oe);
				throw new SwingBenchException(oe.getMessage());
			}
			processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), true, getInfoArray()));
		}
		catch (SwingBenchException sbe) {
			processTransactionEvent(new JdbcTaskEvent(this, getId(), (System.nanoTime() - executeStart), false, getInfoArray()));
			throw sbe;
		}
	}

	@Override
	public void close() {
	}
}
