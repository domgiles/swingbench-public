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

public class QueryTop10Categories extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(QueryTop10Categories.class.getName());

	private static final Lock lock = new ReentrantLock();
	private static Boolean jdvMode = null;

	public QueryTop10Categories() {
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
					}
					else {
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
				if (jdvMode) {
					// REALLY slow:          			select category_name,
					//						   count(*) as total_articles,
					//						   sum(number_of_comments) as total_comments
					//												from articles_dv a nested data columns ( nested categories[*] columns (category_name path '$.name'),
					//												number_of_comments path
					//							'$.comments.size()'
					//					) group by
					//							category_name
					//					order by total_articles * 10000000000 +
					//					 	     total_comments desc
					//					fetch first 10 rows only;
					try (PreparedStatement p = connection.prepareStatement("			select c.name, count(distinct c.article_id) as total_articles,\n" +
																		   "                    sum((select count(*) from comments com where c.article_id=com.article_id)) as total_comments\n" +
																		   "                    from categories c\n" +
																		   "                    group by c.name\n" +
																		   "                    order by total_articles * 10000000000 + total_comments desc\n" +
																		   "                    fetch first 10 rows only\n")) {
						try (ResultSet
									 r = p.executeQuery()
						) {
							while (r.next()) {
								r.getString(1);
								r.getLong(2);
								r.getLong(3);
							}
							addSelectStatements(1);
						}
					}
				}
				else {
					try (PreparedStatement p = connection.prepareStatement("    			select category_name,\n" +
																		   "	   count(*) as total_articles,\n" +
																		   "	   sum(number_of_comments) as total_comments\n" +
																		   "							from articles a nested data columns ( nested categories[*] columns (category_name path '$.name'),\n" +
																		   "							number_of_comments path\n" +
																		   "		'$.comments.size()'\n" +
																		   ") group by\n" +
																		   "		category_name\n" +
																		   "order by total_articles * 10000000000 +\n" +
																		   " 	     total_comments desc\n" +
																		   "fetch first 10 rows only\n")) {
						try (ResultSet
									 r = p.executeQuery()
						) {
							while (r.next()) {
								r.getString(1);
								r.getLong(2);
								r.getLong(3);
							}
							addSelectStatements(1);
						}
					}
				}
			}
			catch (SQLException oe) {
				logger.log(Level.FINE, "Exception thrown in QueryTop10Categories", oe);
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
