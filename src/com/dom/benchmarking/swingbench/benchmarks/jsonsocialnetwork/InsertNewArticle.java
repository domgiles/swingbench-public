package com.dom.benchmarking.swingbench.benchmarks.jsonsocialnetwork;

import com.dom.benchmarking.swingbench.event.JdbcTaskEvent;
import com.dom.benchmarking.swingbench.kernel.DatabaseTransaction;
import com.dom.benchmarking.swingbench.kernel.SwingBenchException;
import com.dom.benchmarking.swingbench.kernel.SwingBenchTask;
import com.dom.benchmarking.swingbench.utilities.RandomGenerator;
import com.dom.datagen.constants.Constants;
import com.dom.datagen.kernel.utilities.Random;
import com.dom.util.Utilities;
import oracle.jdbc.OracleTypes;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InsertNewArticle extends DatabaseTransaction {
	private static final Logger logger = Logger.getLogger(InsertNewArticle.class.getName());

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
	private static Boolean jdvMode = null;
	private static final List<Integer> peopleIds = new ArrayList<>();

	private OracleRDBMSClient client = null;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private JsonBuilderFactory jsonFactory;
	private Random random;

	public InsertNewArticle() {
		super();
	}

	@Override
	public void init(Map<String, Object> params) throws SwingBenchException {
		Properties prop = new Properties();
		prop.put("oracle.soda.sharedMetadataCache", "true");
		client = new OracleRDBMSClient(prop);
		sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT"));
		jsonFactory = Json.createBuilderFactory(null);
		random = new Random(Constants.RandomType.MERSENNE, System.nanoTime());

		File firstNamesFile = new File(FIRST_NAMES_FILE);
		File lastNamesFile = new File(LAST_NAMES_FILE);
		File shortWordsFile = new File(SHORT_WORDS_FILE);
		File adjectivesFile = new File(ADJECTIVES_FILE);
		File wordsFile = new File(WORDS_FILE);

		Connection connection = (Connection) params.get(SwingBenchTask.JDBC_CONNECTION);

		lock.lock();
		try {
			if (jdvMode == null) {
				try (Statement s = connection.createStatement()) {
					try (ResultSet rs = s.executeQuery(
							"SELECT METADATA_VALUE\n" +
							"FROM ARTICLES_METADATA\n" +
							"WHERE METADATA_KEY = 'MODE'")) {
						if (rs.next()) {
							jdvMode = "JDV".equalsIgnoreCase("JDV");
						}
						else {
							jdvMode = false;
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
					logger.log(Level.SEVERE, "Error initialising UpdateExistingArticle", se);
					throw new SwingBenchException(
							se);
				}
			}

			if (firstNames == null) {
				firstNames = Utilities.cacheFile(firstNamesFile);
				lastNames = Utilities.cacheFile(lastNamesFile);
				popularShortWords = Utilities.cacheFile(shortWordsFile);
				popularAdjectives = Utilities.cacheFile(adjectivesFile);
				popularWords = Utilities.cacheFile(wordsFile);
				logger.fine("Reference data loaded for InsertNewArticle transaction");
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

			initJdbcTask();

			executeStart = System.nanoTime();

			if (jdvMode) {
				try (PreparedStatement ps = connection.prepareStatement(String.format("insert into articles_dv(data) values (?)"))) {

						final String _id = UUID.randomUUID().toString();

						// tags
						final JsonArrayBuilder tagsBuilder = jsonFactory.createArrayBuilder();
						final int tags = random.randomInteger(0, 8);
						for (int j = 0; j < tags; j++) {
							String tag = popularWords.get(random.randomInteger(0, 100));
							tagsBuilder.add(jsonFactory.createObjectBuilder().add("tag", tag).add("url", String.format("./tag/%s", tag)));
						}
						final JsonArray arrayTags = tagsBuilder.build();

						// categories
						final JsonArrayBuilder categoriesBuilder = jsonFactory.createArrayBuilder();
						final int categories = random.randomInteger(1, 3);
						for (int j = 0; j < categories; j++) {
							String category = popularWords.get(random.randomInteger(100, 200));
							categoriesBuilder.add(jsonFactory.createObjectBuilder().add("category", category).add("url", String.format("./category/%s", category)));
						}
						final JsonArray arrayCategories = categoriesBuilder.build();

						// comments
						final JsonArrayBuilder commentsBuilder = jsonFactory.createArrayBuilder();
						final int comments = random.randomInteger(0, 200);
						for (int j = 0; j < comments; j++) {
							String comment = getRandomText(random, 8, 500);
							commentsBuilder.add(jsonFactory.createObjectBuilder().add("peopleId", peopleIds.get(random.randomInteger(0, peopleIds.size()))).add("comment", comment).add("url", String.format("./articles/%s/comments/%d", _id, j + 1)));
						}
						final JsonArray arrayComments = commentsBuilder.build();

						// author(s)
						final JsonArrayBuilder peoplesBuilder = jsonFactory.createArrayBuilder();
						final int peoples = random.randomInteger(1, 3);
						for (int j = 0; j < peoples; j++) {
							peoplesBuilder.add(jsonFactory.createObjectBuilder().add("_id", peopleIds.get(random.randomInteger(0, peopleIds.size()))));
						}
						final JsonArray arrayPeoples = peoplesBuilder.build();

						JsonObject articleJSON = jsonFactory.createObjectBuilder()
								//.add("_id", _id)
								.add("title", getRandomText(random, 20, 120))
								.add("createdOn", sdf.format(java.util.Date.from(Instant.ofEpochSecond(RandomGenerator.randomLong(1, System.currentTimeMillis() / 1000)))))
								.add("text", getRandomText(random, 500, 10000))
								.add("tags", arrayTags)
								.add("categories", arrayCategories)
								.add("comments", arrayComments)
								.add("peoples", arrayPeoples)
								.build();


//						ps.setObject(1, articleJSON.toString(), OracleTypes.JSON);

					ps.executeUpdate();
					addInsertStatements(1);

					connection.commit();
					addCommitStatements(1);
				}
				catch (SQLException sqle) {
					try {
						connection.rollback();
						addRollbackStatements(1);
					}
					catch (SQLException ignored) {
					}
					logger.log(Level.FINE, "Exception thrown in InsertNewArticle", sqle);
					throw new SwingBenchException(sqle.getMessage());
				}
			}
			else {
				try {
					OracleDatabase database = client.getDatabase(connection);
					OracleCollection collection = database.openCollection("articles");

					final String _id = UUID.randomUUID().toString();

					// tags
					final JsonArrayBuilder tagsBuilder = jsonFactory.createArrayBuilder();
					final int tags = random.randomInteger(0, 8);
					for (int j = 0; j < tags; j++) {
						String tag = popularWords.get(random.randomInteger(0, 100));
						tagsBuilder.add(jsonFactory.createObjectBuilder().add("tag", tag).add("url", String.format("./tag/%s", tag)));
					}
					final JsonArray arrayTags = tagsBuilder.build();

					// categories
					final JsonArrayBuilder categoriesBuilder = jsonFactory.createArrayBuilder();
					final int categories = random.randomInteger(1, 3);
					for (int j = 0; j < categories; j++) {
						String category = popularWords.get(random.randomInteger(100, 200));
						categoriesBuilder.add(jsonFactory.createObjectBuilder().add("category", category).add("url", String.format("./category/%s", category)));
					}
					final JsonArray arrayCategories = categoriesBuilder.build();

					// comments
					final JsonArrayBuilder commentsBuilder = jsonFactory.createArrayBuilder();
					final int comments = random.randomInteger(0, 200);
					for (int j = 0; j < comments; j++) {
						String comment = getRandomText(random, 8, 500);
						commentsBuilder.add(jsonFactory.createObjectBuilder().add("name", String.format("%s %s", firstNames.get(RandomGenerator.randomInteger(0, firstNames.size())), lastNames.get(RandomGenerator.randomInteger(0, lastNames.size())))).add("comment", comment).add("url", String.format("./articles/%s/comments/%d", _id, j + 1)));
					}
					final JsonArray arrayComments = commentsBuilder.build();

					// author(s)
					final JsonArrayBuilder peoplesBuilder = jsonFactory.createArrayBuilder();
					final int peoples = random.randomInteger(1, 3);
					for (int j = 0; j < peoples; j++) {
						String name = String.format("%s %s", firstNames.get(RandomGenerator.randomInteger(0, firstNames.size())), lastNames.get(RandomGenerator.randomInteger(0, lastNames.size())));

						JsonObjectBuilder people = jsonFactory.createObjectBuilder().add("name", name);

						switch (random.randomInteger(0, 2)) {
							case 1:
								if (random.randomInteger(0, 1) == 0) {
									people.add("homePhone", getRandomPhone(random));
								}
								else {
									people.add("workPhone", getRandomPhone(random));
								}
								break;
							case 2:
								people.add("homePhone", getRandomPhone(random));
								people.add("workPhone", getRandomPhone(random));
								break;
						}

						peoplesBuilder.add(people);
					}
					final JsonArray arrayPeoples = peoplesBuilder.build();

					JsonObject articleJSON = jsonFactory.createObjectBuilder()
							.add("_id", _id)
							.add("title", getRandomText(random, 20, 120))
							.add("createdOn", sdf.format(java.util.Date.from(Instant.ofEpochSecond(RandomGenerator.randomLong(1, System.currentTimeMillis() / 1000)))))
							.add("text", getRandomText(random, 500, 10000))
							.add("tags", arrayTags)
							.add("categories", arrayCategories)
							.add("comments", arrayComments)
							.add("peoples", arrayPeoples)
							.build();

					OracleDocument document = database.createDocumentFrom(articleJSON);
					collection.insert(document);
					addInsertStatements(1);

					connection.commit();
					addCommitStatements(1);
				}
				catch (OracleException | SQLException oe) {
					try {
						connection.rollback();
						addRollbackStatements(1);
					}
					catch (SQLException ignored) {
					}
					logger.log(Level.FINE, "Exception thrown in InsertNewArticle", oe);
					throw new SwingBenchException(oe.getMessage());
				}
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

	private String getRandomPhone(final Random random) {
		return String.format("%03d-%03d-%04d", random.randomInteger(0, 999), random.randomInteger(0, 999), random.randomInteger(0, 9999));
	}

	private String getRandomText(Random random, int minLength, int maxLength) {
		final int size = random.randomInteger(minLength, maxLength);
		final StringBuilder s = new StringBuilder(size);

		int i = 0;
		while (s.length() < size) {
			switch (i % 3) {
				case 1:
					s.append(popularShortWords.get(random.randomInteger(0, popularShortWords.size())));
					break;
				case 2:
					s.append(popularAdjectives.get(random.randomInteger(0, popularAdjectives.size())));
					break;
				case 0:
					s.append(popularWords.get(random.randomInteger(0, popularWords.size())));
					break;
			}
			s.append(' ');
			i++;
		}

		return s.toString();
	}
}
