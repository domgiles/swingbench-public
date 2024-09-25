package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;

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

public class QueryTop10CommentsArticle extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(QueryTop10CommentsArticle.class.getName());

	private static final Lock lock = new ReentrantLock();
	private static Boolean jdvMode = null;

	public QueryTop10CommentsArticle() {
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
					// REALLY slow: select a.data."_id", a.data.comments.size() from articles_dv a
					//            		order by 2 DESC
					//            		fetch first 10 rows only
					try (PreparedStatement p = connection.prepareStatement("    			select c.article_id, count(*) from comments c\n" +
																		   "group by c.article_id\n" +
																		   "order by 2 DESC\n" +
																		   "fetch first 10 rows only\n")) {
						try (ResultSet r = p.executeQuery()) {
							while (r.next()) {
								r.getLong(1);
								r.getLong(2);
							}
							addSelectStatements(1);
						}
					}
				}
				else {
					try (PreparedStatement p = connection.prepareStatement("			select id, a.data.comments.size() from articles a\n" +
																		   "   		order by 2 DESC\n" +
																		   "   		fetch first 10 rows only\n")) {
						try (ResultSet r = p.executeQuery()) {
							while (r.next()) {
								r.getLong(1);
								r.getLong(2);
							}
							addSelectStatements(1);
						}
					}
				}
			}
			catch (SQLException oe) {
				logger.log(Level.FINE, "Exception thrown in QueryTop10CommentsArticle", oe);
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
