package com.dom.benchmarking.swingbench.benchmarks.orderentrycc;

import java.util.List;
import java.util.logging.Logger;

/**
 * Utility class to hash a thread name (e.g., "UserSession01") to a country code.
 * This can be used to ensure that a specific user session always operates within
 * the same country's data set.
 */
public class ThreadToCountryCode {

    private static final Logger logger = Logger.getLogger(ThreadToCountryCode.class.getName());

    /**
     * Hashes a thread name to a country code using the initialized country codes
     * from the OrderEntryProcess.
     *
     * @param threadName the name of the thread to hash
     * @return a country code as a String, or null if no country codes are available
     */
    public static String getCountryCode(String threadName) {
        List<String> countryCodes = OrderEntryProcess.countryCodes;
        if (countryCodes == null || countryCodes.isEmpty()) {
            return null;
        }
        return getCountryCode(threadName, countryCodes);
    }

    /**
     * Hashes a thread name to a country code from the provided list.
     *
     * @param threadName   the name of the thread to hash
     * @param countryCodes the list of country codes to hash into
     * @return a country code as a String, or null if the list is empty
     */
    public static String getCountryCode(String threadName, List<String> countryCodes) {
        if (countryCodes == null || countryCodes.isEmpty()) {
            return null;
        }

        // We want a deterministic hash that distributes well.
        // Using a similar approach to SplitMix64 mixing used in OrderEntryProcess.getMyLocation
        long h = hashString(threadName);

        int index = (int) Math.floorMod(h, countryCodes.size());
        logger.finest(String.format("Thread %s hashed to country code index %s", threadName, countryCodes.get(index)));
        return countryCodes.get(index);
    }

    private static long hashString(String s) {
        if (s == null) return 0;
        long h = 1125899906842597L; // prime
        int len = s.length();
        for (int i = 0; i < len; i++) {
            h = 31 * h + s.charAt(i);
        }

        // SplitMix64 mixing (deterministic, good avalanche, fast)
        h += 0x9E3779B97F4A7C15L;
        h = (h ^ (h >>> 30)) * 0xBF58476D1CE4E5B9L;
        h = (h ^ (h >>> 27)) * 0x94D049BB133111EBL;
        h = h ^ (h >>> 31);

        return h;
    }
}
