package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages card ID generation using partitioned ranges for each country.
 */
public class CardIdManager {
    private static final Logger logger = Logger.getLogger(CardIdManager.class.getName());
    private static final Map<String, Range> countryRanges = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> nextIds = new ConcurrentHashMap<>();
    private static boolean rangesLoaded = false;

    private static class Range {
        final long min;
        final long max;

        Range(long min, long max) {
            this.min = min;
            this.max = max;
        }
    }

    public static synchronized void initializeRanges(List<String> countryCodes) {
        if (countryCodes == null || countryCodes.isEmpty()) {
            throw new IllegalArgumentException("Country codes list cannot be empty");
        }
        int numCountries = countryCodes.size();
        long rangeSize = Long.MAX_VALUE / numCountries;

        for (int i = 0; i < numCountries; i++) {
            String code = countryCodes.get(i);
            long min = (i * rangeSize) + 1;
            long max = (i == numCountries - 1) ? Long.MAX_VALUE : (i + 1) * rangeSize;
            countryRanges.put(code, new Range(min, max));
            nextIds.put(code, new AtomicLong(min));
        }
        rangesLoaded = true;
        logger.fine("Initialized card ID ranges for " + numCountries + " countries.");
    }

    public static long getNextCardId(String countryCode) {
        AtomicLong nextId = nextIds.get(countryCode);
        if (nextId == null) {
            throw new IllegalArgumentException("Unknown country code: " + countryCode);
        }
        long id = nextId.getAndIncrement();
        Range range = countryRanges.get(countryCode);
        if (id > range.max) {
            throw new RuntimeException("Exceeded card ID range for country: " + countryCode);
        }
        return id;
    }

    public static void storeRanges(Connection connection) throws SQLException {
        String deleteSql = "delete from ORDERENTRY_METADATA where metadata_key like 'CARD_RANGE_%'";
        String insertSql = "insert into ORDERENTRY_METADATA (metadata_key, metadata_value) values (?, ?)";

        try (PreparedStatement delPs = connection.prepareStatement(deleteSql)) {
            delPs.executeUpdate();
        }

        try (PreparedStatement insPs = connection.prepareStatement(insertSql)) {
            for (Map.Entry<String, Range> entry : countryRanges.entrySet()) {
                String countryCode = entry.getKey();
                Range range = entry.getValue();
                AtomicLong nextId = nextIds.get(countryCode);

                insPs.setString(1, "CARD_RANGE_MIN_" + countryCode);
                insPs.setString(2, String.valueOf(range.min));
                insPs.addBatch();

                insPs.setString(1, "CARD_RANGE_MAX_" + countryCode);
                insPs.setString(2, String.valueOf(range.max));
                insPs.addBatch();

                if (nextId != null) {
                    insPs.setString(1, "CARD_RANGE_CUR_" + countryCode);
                    insPs.setString(2, String.valueOf(nextId.get()));
                    insPs.addBatch();
                }
            }
            insPs.executeBatch();
        }
    }

    public static synchronized void loadRanges(Connection connection) throws SQLException {
        if (rangesLoaded) return;
        String sql = "select metadata_key, metadata_value from ORDERENTRY_METADATA where metadata_key like 'CARD_RANGE_%'";
        Map<String, Long> mins = new HashMap<>();
        Map<String, Long> maxs = new HashMap<>();
        Map<String, Long> curs = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String key = rs.getString(1);
                String val = rs.getString(2);
                if (key.startsWith("CARD_RANGE_MIN_")) {
                    mins.put(key.substring("CARD_RANGE_MIN_".length()), Long.parseLong(val));
                } else if (key.startsWith("CARD_RANGE_MAX_")) {
                    maxs.put(key.substring("CARD_RANGE_MAX_".length()), Long.parseLong(val));
                } else if (key.startsWith("CARD_RANGE_CUR_")) {
                    curs.put(key.substring("CARD_RANGE_CUR_".length()), Long.parseLong(val));
                }
            }
        }

        if (mins.isEmpty()) {
            checkCardRanges(connection);
        } else {
            for (String code : mins.keySet()) {
                long min = mins.get(code);
                long max = maxs.getOrDefault(code, min);
                long cur = curs.getOrDefault(code, min);
                countryRanges.put(code, new Range(min, max));
                nextIds.put(code, new AtomicLong(cur));
            }
            logger.fine("Loaded card ID ranges for " + countryRanges.size() + " countries.");
        }
        rangesLoaded = true;
    }

    public static synchronized void checkCardRanges(Connection connection) throws SQLException {
        logger.fine("Checking card ID ranges in database...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("alter session force parallel query parallel 8");
        }
        String sql = "select country_code, min(card_id), max(card_id) from card_details group by country_code";
        boolean updated = false;

        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String countryCode = rs.getString(1);
                long min = rs.getLong(2);
                long max = rs.getLong(3);

                Range range = countryRanges.get(countryCode);
                AtomicLong nextId = nextIds.get(countryCode);

                if (range == null || nextId == null) {
                    countryRanges.put(countryCode, new Range(min, Long.MAX_VALUE));
                    nextIds.put(countryCode, new AtomicLong(max + 1));
                    updated = true;
                } else {
                    if (max >= nextId.get()) {
                        nextId.set(max + 1);
                        updated = true;
                    }
                    if (min < range.min) {
                        countryRanges.put(countryCode, new Range(min, range.max));
                        updated = true;
                    }
                }
            }
        }

        if (updated) {
            storeRanges(connection);
        }
        rangesLoaded = true;
    }

    /**
     * Returns the min and max card ranges for a given country code.
     * @return a long array [min, max] or null if the country code is not found.
     */
    public static long[] getCardIdRange(String countryCode) {
        Range range = countryRanges.get(countryCode);
        if (range == null) {
            return null;
        }
        return new long[]{range.min, range.max};
    }
}
