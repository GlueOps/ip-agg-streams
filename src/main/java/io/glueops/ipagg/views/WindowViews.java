package io.glueops.ipagg.views;

import java.util.Comparator;
import java.util.List;

/**
 * The bounded views of one closed window on this instance.
 *
 * @param totalRequests  requests counted in the window
 * @param uniqueIps      distinct client IPs in the window (exact; IPs never span instances)
 * @param distinctRoutes distinct (method, route) pairs, before the maxRoutes cap
 * @param topIps         top IPs by total requests over all methods and routes
 * @param topPairs       top (IP, method, route) combinations
 * @param routes         for the busiest routes, their top IPs
 */
public record WindowViews(
        long windowStartMs,
        long windowEndMs,
        long totalRequests,
        long uniqueIps,
        int distinctRoutes,
        List<IpCount> topIps,
        List<PairCount> topPairs,
        List<RouteTop> routes) {

    public record IpCount(String ip, long count) {
        /** Higher count is greater; on a tie the lexicographically smaller IP is greater, so results are stable. */
        public static final Comparator<IpCount> ORDER = Comparator.comparingLong(IpCount::count)
                .thenComparing(IpCount::ip, Comparator.reverseOrder());
    }

    public record PairCount(String ip, String method, String route, long count) {
        public static final Comparator<PairCount> ORDER = Comparator.comparingLong(PairCount::count)
                .thenComparing(PairCount::ip, Comparator.reverseOrder())
                .thenComparing(PairCount::method, Comparator.reverseOrder())
                .thenComparing(PairCount::route, Comparator.reverseOrder());
    }

    public record RouteTop(String method, String route, long requests, List<IpCount> topIps) {
    }
}
