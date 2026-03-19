package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages customer ID generation using partitioned ranges for each country.
 * Each country is allocated a slice of the Long.MAX_VALUE space.
 */
public class CustomerIdManager {
    private static final Logger logger = Logger.getLogger(CustomerIdManager.class.getName());
    private static final Map<String, Range> countryRanges = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> nextIds = new ConcurrentHashMap<>();
    private static List<String> countryCodesList = null;
    private static boolean rangesLoaded = false;
    public static List<String> getFallbackCountryCodes() {
        return java.util.Arrays.asList(
                "AFG", "ALB", "DZA", "ASM", "AND", "AGO", "AIA", "ATA", "ATG", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE", "BHS", "BHR", "BGD", "BRB", "BLR",
                "BEL", "BLZ", "BEN", "BMU", "BTN", "BOL", "BES", "BIH", "BWA", "BVT", "BRA", "IOT", "BRN", "BGR", "BFA", "BDI", "CPV", "KHM", "CMR", "CAN",
                "CYM", "CAF", "TCD", "CHL", "CHN", "CXR", "CCK", "COL", "COM", "COG", "COD", "COK", "CRI", "CIV", "HRV", "CUB", "CUW", "CYP", "CZE", "DNK",
                "DJI", "DMA", "DOM", "ECU", "EGY", "SLV", "GNQ", "ERI", "EST", "SWZ", "ETH", "FLK", "FRO", "FJI", "FIN", "FRA", "GUF", "PYF", "ATF", "GAB",
                "GMB", "GEO", "DEU", "GHA", "GIB", "GRC", "GRL", "GRD", "GLP", "GUM", "GTM", "GGY", "GIN", "GNB", "GUY", "HTI", "HMD", "VAT", "HND", "HKG",
                "HUN", "ISL", "IND", "IDN", "IRN", "IRQ", "IRL", "IMN", "ISR", "ITA", "JAM", "JPN", "JEY", "JOR", "KAZ", "KEN", "KIR", "PRK", "KOR", "KWT",
                "KGZ", "LAO", "LVA", "LBN", "LSO", "LBR", "LBY", "LIE", "LTU", "LUX", "MAC", "MDG", "MWI", "MYS", "MDV", "MLI", "MLT", "MHL", "MTQ", "MRT",
                "MUS", "MYT", "MEX", "FSM", "MDA", "MCO", "MNG", "MNE", "MSR", "MAR", "MOZ", "MMR", "NAM", "NRU", "NPL", "NLD", "NCL", "NZL", "NIC", "NER",
                "NGA", "NIU", "NFK", "MKD", "MNP", "NOR", "OMN", "PAK", "PLW", "PSE", "PAN", "PNG", "PRY", "PER", "PHL", "PCN", "POL", "PRT", "PRI", "QAT",
                "REU", "ROU", "RUS", "RWA", "BLM", "SHN", "KNA", "LCA", "MAF", "SPM", "VCT", "WSM", "SMR", "STP", "SAU", "SEN", "SRB", "SYC", "SLE", "SGP",
                "SXM", "SVK", "SVN", "SLB", "SOM", "ZAF", "SGS", "SSD", "ESP", "LKA", "SDN", "SUR", "SJM", "SWE", "CHE", "SYR", "TWN", "TJK", "TZA", "THA",
                "TLS", "TGO", "TKL", "TON", "TTO", "TUN", "TUR", "TKM", "TCA", "TUV", "UGA", "UKR", "ARE", "GBR", "USA", "UMI", "URY", "UZB", "VUT", "VEN",
                "VNM", "VGB", "VIR", "WLF", "ESH", "YEM", "ZMB", "ZWE"
        );
    }

    private static class Range {
        final long min;
        final long max;

        Range(long min, long max) {
            this.min = min;
            this.max = max;
        }
    }

    /**
     * Initializes the ranges for a given list of country codes.
     * The Long.MAX_VALUE space is partitioned equally among the countries.
     */
    public static synchronized void initializeRanges(List<String> countryCodes) {
        if (countryCodes == null || countryCodes.isEmpty()) {
            throw new IllegalArgumentException("Country codes list cannot be empty");
        }
        countryCodesList = new ArrayList<>(countryCodes);
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
        logger.fine("Initialized customer ID ranges for " + numCountries + " countries. Range size: " + rangeSize);
    }

    /**
     * Returns the next available customer_id for a given country code.
     */
    public static long getNextCustomerId(String countryCode) {
        AtomicLong nextId = nextIds.get(countryCode);
        if (nextId == null) {
            throw new IllegalArgumentException("Unknown country code: " + countryCode);
        }
        long id = nextId.getAndIncrement();
        Range range = countryRanges.get(countryCode);
        if (id > range.max) {
            throw new RuntimeException("Exceeded customer ID range for country: " + countryCode);
        }
        return id;
    }

    /**
     * Returns the country code for a given customer_id.
     */
    public static String getCountryCode(long customerId) {
        String bestMatch = null;
        long maxMin = -1;
        for (Map.Entry<String, Range> entry : countryRanges.entrySet()) {
            Range r = entry.getValue();
            if (customerId >= r.min && customerId <= r.max) {
                if (r.min > maxMin) {
                    maxMin = r.min;
                    bestMatch = entry.getKey();
                }
            }
        }
        return bestMatch;
    }

    /**
     * Returns a random valid customer_id from a randomly selected country.
     */
    public static long getRandomCustomerId() {
        if (countryCodesList == null || countryCodesList.isEmpty()) {
            countryCodesList = new ArrayList<>(countryRanges.keySet());
        }
        if (countryCodesList.isEmpty()) {
            return -1;
        }
        
        // Try up to 10 times to find a country that has generated IDs
        for (int i = 0; i < 10; i++) {
            String countryCode = countryCodesList.get(ThreadLocalRandom.current().nextInt(countryCodesList.size()));
            long customerId = getRandomCustomerId(countryCode);
            if (customerId != -1) {
                return customerId;
            }
        }
        return -1;
    }

    /**
     * Returns a random existing customerID for a given country code.
     */
    public static long getRandomCustomerId(String countryCode) {
        Range range = countryRanges.get(countryCode);
        AtomicLong nextId = nextIds.get(countryCode);

        if (range != null && nextId != null) {
            long min = range.min;
            long current = nextId.get();
            if (current > min) {
                return ThreadLocalRandom.current().nextLong(min, current);
            }
        }
        return -1;
    }

    /**
     * Stores the generated ranges and current state into the ORDERENTRY_METADATA table.
     */
    public static void storeRanges(Connection connection) throws SQLException {
        String deleteSql = "delete from ORDERENTRY_METADATA where metadata_key like 'COUNTRY_RANGE_%'";
        String insertSql = "insert into ORDERENTRY_METADATA (metadata_key, metadata_value) values (?, ?)";
        
        try (PreparedStatement delPs = connection.prepareStatement(deleteSql)) {
            delPs.executeUpdate();
        }

        try (PreparedStatement insPs = connection.prepareStatement(insertSql)) {
            for (Map.Entry<String, Range> entry : countryRanges.entrySet()) {
                String countryCode = entry.getKey();
                Range range = entry.getValue();
                AtomicLong nextId = nextIds.get(countryCode);
                
                insPs.setString(1, "COUNTRY_RANGE_MIN_" + countryCode);
                insPs.setString(2, String.valueOf(range.min));
                insPs.addBatch();
                
                insPs.setString(1, "COUNTRY_RANGE_MAX_" + countryCode);
                insPs.setString(2, String.valueOf(range.max));
                insPs.addBatch();

                if (nextId != null) {
                    insPs.setString(1, "COUNTRY_RANGE_CUR_" + countryCode);
                    insPs.setString(2, String.valueOf(nextId.get()));
                    insPs.addBatch();
                }
            }
            insPs.executeBatch();
        }
        logger.fine("Stored customer ID ranges for " + countryRanges.size() + " countries in ORDERENTRY_METADATA");
    }

    /**
     * Rehydrates range data from the ORDERENTRY_METADATA table.
     */
    public static synchronized void loadRanges(Connection connection) throws SQLException {
        if (rangesLoaded) return;
        String sql = "select metadata_key, metadata_value from ORDERENTRY_METADATA where metadata_key like 'COUNTRY_RANGE_%'";
        Map<String, Long> mins = new HashMap<>();
        Map<String, Long> maxs = new HashMap<>();
        Map<String, Long> curs = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String key = rs.getString(1);
                String val = rs.getString(2);
                if (key.startsWith("COUNTRY_RANGE_MIN_")) {
                    mins.put(key.substring("COUNTRY_RANGE_MIN_".length()), Long.parseLong(val));
                } else if (key.startsWith("COUNTRY_RANGE_MAX_")) {
                    maxs.put(key.substring("COUNTRY_RANGE_MAX_".length()), Long.parseLong(val));
                } else if (key.startsWith("COUNTRY_RANGE_CUR_")) {
                    curs.put(key.substring("COUNTRY_RANGE_CUR_".length()), Long.parseLong(val));
                }
            }
        }

        if (mins.isEmpty()) {
            checkCustomerRanges(connection);
        } else {
            for (String code : mins.keySet()) {
                long min = mins.get(code);
                long max = maxs.getOrDefault(code, min);
                long cur = curs.getOrDefault(code, min);
                countryRanges.put(code, new Range(min, max));
                nextIds.put(code, new AtomicLong(cur));
            }
            countryCodesList = new ArrayList<>(countryRanges.keySet());
            logger.fine("Loaded customer ID ranges for " + countryRanges.size() + " countries from ORDERENTRY_METADATA");
        }
        rangesLoaded = true;
    }

    /**
     * Checks the actual min and max customer IDs in the CUSTOMERS table for each country
     * and updates the in-memory state and the ORDERENTRY_METADATA table.
     */
    public static synchronized void checkCustomerRanges(Connection connection) throws SQLException {
        long startTime = System.currentTimeMillis();
        logger.fine("Checking customer ID ranges in database...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("alter session force parallel query parallel 8");
        }
        String sql = "select country_code, min(customer_id), max(customer_id) from customers group by country_code";
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
                    logger.finest("Found country code in database: " + countryCode + " that was not in memory. Initializing range.");
                    countryRanges.put(countryCode, new Range(min, Long.MAX_VALUE));
                    nextIds.put(countryCode, new AtomicLong(max + 1));
                    updated = true;
                } else {
                    if (max >= nextId.get()) {
                        logger.finest("Updating nextId for country " + countryCode + " from " + nextId.get() + " to " + (max + 1));
                        nextId.set(max + 1);
                        updated = true;
                    }
                    if (min < range.min) {
                        logger.finest("Updating min range for country " + countryCode + " from " + range.min + " to " + min);
                        countryRanges.put(countryCode, new Range(min, range.max));
                        updated = true;
                    }
                    if (max > range.max) {
                        logger.warning("Actual max ID for " + countryCode + " (" + max + ") exceeds theoretical max (" + range.max + ")");
                        countryRanges.put(countryCode, new Range(range.min, max + 1000000));
                        updated = true;
                    }
                }
            }
        }

        if (updated) {
            storeRanges(connection);
            logger.log(Level.FINE,"Updated ranges");
        }
        countryCodesList = new ArrayList<>(countryRanges.keySet());
        rangesLoaded = true;
        logger.log(Level.FINE,"Ranges checked in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * Returns the min and max customer ranges for a given country code.
     * @return a long array [min, max] or null if the country code is not found.
     */
    public static long[] getCustomerIdRange(String countryCode) {
        Range range = countryRanges.get(countryCode);
        if (range == null) {
            return null;
        }
        return new long[]{range.min, range.max};
    }

    /**
     * Returns the list of country codes that have been initialized with ranges.
     */
    public static List<String> getInitializedCountryCodes() {
        if (countryCodesList == null || countryCodesList.isEmpty()) {
            countryCodesList = new ArrayList<>(countryRanges.keySet());
        }
        return countryCodesList;
    }
}
