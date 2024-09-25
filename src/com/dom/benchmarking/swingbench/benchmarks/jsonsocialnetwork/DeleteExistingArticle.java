package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteExistingArticle extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(DeleteExistingArticle.class.getName());

	private static final Lock lock = new ReentrantLock();
	private static Long articlesRange = null;
	private static Boolean jdvMode = false;

	public DeleteExistingArticle() {
		super();
	}

	@Override
	public void init(Map<String, Object> params) throws SwingBenchException {
		Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

		lock.lock();
		if (articlesRange == null) {
			logger.fine("Reference data loaded for DeleteExistingArticle transaction");
			try (Statement s = connection.createStatement()) {
				try (ResultSet rs = s.executeQuery("SELECT METADATA_VALUE\n" +
												   "FROM ARTICLES_METADATA\n" +
												   "WHERE METADATA_KEY = 'MAX_ARTICLES_COUNT'")) {
					if (rs.next()) {
						articlesRange = rs.getLong(1);
					}
				}
				try (ResultSet rs = s.executeQuery("SELECT METADATA_VALUE\n" +
												   "FROM ARTICLES_METADATA\n" +
												   "WHERE METADATA_KEY = 'MODE'")) {
					if (rs.next()) {
						jdvMode = "JDV".equalsIgnoreCase("JDV");
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
			final long articleKey = RandomGenerator.randomLong(1, articlesRange);

			initJdbcTask();

			executeStart = System.nanoTime();
			try {
				if(jdvMode) {
					try (PreparedStatement p2 = connection.prepareStatement("delete from articles_dv d where d.data.\"_id\"=?")) {
						p2.setLong(1, articleKey);
						p2.executeUpdate();
						addDeleteStatements(1);
					}
				} else {
					try (PreparedStatement p2 = connection.prepareStatement("delete from articles where id=?")) {
						p2.setLong(1, articleKey);
						p2.executeUpdate();
						addDeleteStatements(1);
					}
				}

				connection.commit();
				addCommitStatements(1);
			}
			catch (SQLException oe) {
				try {
					connection.rollback();
					addRollbackStatements(1);
				}
				catch (SQLException ignored) {
				}
				logger.log(Level.FINE, "Exception thrown in DeleteExistingArticle", oe);
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
