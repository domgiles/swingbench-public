package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InsertNewComment extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(InsertNewComment.class.getName());

	private static final String FIRST_NAMES_FILE = "data/lowerfirstnames.txt";
	private static final String LAST_NAMES_FILE = "data/lowerlastnames.txt";
	private static final String SHORT_WORDS_FILE = "data/100mostpopularwords.txt";
	private static final String ADJECTIVES_FILE = "data/1000mostpopularadjectives.txt";
	private static final String WORDS_FILE = "data/1000mostpopularwords.txt";
	private static List<String> firstNames;
	private static List<String> lastNames;
	private static List<String> popularShortWords;
	private static List<String> popularAdjectives;
	private static List<String> popularWords;

	private static final Lock lock = new ReentrantLock();
	private static Long articlesRange = null;
	private static Boolean jdvMode = false;
	private static final List<Integer> peopleIds = new ArrayList<>();
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private Random random;

	public InsertNewComment() {
		super();
	}

	@Override
	public void init(Map<String, Object> params) throws SwingBenchException {

		sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
		random = new Random(Constants.RandomType.MERSENNE);

		File firstNamesFile = new File(FIRST_NAMES_FILE);
		File lastNamesFile = new File(LAST_NAMES_FILE);
		File shortWordsFile = new File(SHORT_WORDS_FILE);
		File adjectivesFile = new File(ADJECTIVES_FILE);
		File wordsFile = new File(WORDS_FILE);

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

				if(jdvMode) {
					try (ResultSet r = s.executeQuery("select id from people")) {
						while (r.next()) {
							peopleIds.add(r.getInt(1));
						}
					}
				}
			}
			catch (SQLException se) {
				logger.log(Level.SEVERE, "Error initialising DeleteExistingArticle", se);
				throw new SwingBenchException(se);
			}
		}

		try {
			if (firstNames == null) {
				firstNames = Utilities.cacheFile(firstNamesFile);
				lastNames = Utilities.cacheFile(lastNamesFile);
				popularShortWords = Utilities.cacheFile(shortWordsFile);
				popularAdjectives = Utilities.cacheFile(adjectivesFile);
				popularWords = Utilities.cacheFile(wordsFile);
				logger.fine("Reference data loaded for InsertNewComment transaction");
			}

		}
		catch (IOException ie) {
			logger.log(Level.SEVERE, "Unable to open data seed files : ", ie);
			throw new SwingBenchException(ie);
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
					// JSON Duality View
					try (PreparedStatement p2 = connection.prepareStatement("update articles_dv d set data = json_transform( data, \n" +
							"APPEND '$.comments' = json { 'peopleId': ?, 'comment': ?, 'url': './articles/' \n" +
							"|| ? || '/comments/' || d.data.comments.size()\n" +
							"} ) \n" +
							"where d.data.\"_id\"=?")) {
						p2.setInt(1, peopleIds.get(random.randomInteger(0,peopleIds.size())));
						p2.setString(2, getRandomText(random, 8, 500));
						p2.setString(3, UUID.randomUUID().toString());

						p2.setLong(4, articleKey);
						p2.executeUpdate();
						addUpdateStatements(1);
					}
				} else {
					// SODA collection
					try (PreparedStatement p = connection.prepareStatement("select a.data.\"_id\".string() from articles a where id=?")) {
						p.setLong(1, articleKey);
						try (ResultSet r = p.executeQuery()) {
							if (r.next()) {
								addSelectStatements(1);
								final String id = r.getString(1);

								try (PreparedStatement p2 = connection.prepareStatement("update articles d set data = json_transform( data, \n" +
										"APPEND '$.comments' = json { 'name': ?, 'comment': ?, 'url': './articles/' \n" +
										"|| ? || '/comments/' || d.data.comments.size()\n" +
										"} ) \n" +
										"where id=?")) {
									p2.setString(1, String.format("%s %s", firstNames.get(RandomGenerator.randomInteger(0, firstNames.size())), lastNames.get(RandomGenerator.randomInteger(0, lastNames.size()))));
									p2.setString(2, getRandomText(random, 8, 500));
									p2.setString(3, id);
									p2.setLong(4, articleKey);
									p2.executeUpdate();
									addUpdateStatements(1);
								}
							}
						}
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
				logger.log(Level.FINE, "Exception thrown in InsertNewComment", oe);
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

	private String getRandomText(Random random, int minLength, int maxLength) {
		final int size = random.randomInteger(minLength, maxLength);
		final StringBuilder s = new StringBuilder(size);

		int i = 0;
		while (s.length() < size) {
			switch (i % 3) {
				case 0:
					s.append(popularShortWords.get(random.randomInteger(0, popularShortWords.size())));
					break;
				case 1:
					s.append(popularAdjectives.get(random.randomInteger(0, popularAdjectives.size())));
					break;
				case 2:
					s.append(popularWords.get(random.randomInteger(0, popularWords.size())));
					break;
			}
			s.append(' ');
			i++;
		}

		return s.toString();
	}
}
