package io.glueops.ipagg.emit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.glueops.ipagg.remotewrite.Series;
import io.glueops.ipagg.views.WindowViews;

/**
 * Turns the views of each closed window into remote-write series stamped with the window end. Remembers the previous
 * window's view series and sends a staleness marker for each one that dropped out, so an IP that left the top list
 * disappears from Grafana at once instead of lingering for Prometheus' 5-minute lookback. Not thread-safe: one emitter
 * thread calls it, window by window, in order.
 */
public final class ViewSeries {

    public static final String TOP_IP = "ip_agg_top_ip_requests";
    public static final String TOP_PAIR = "ip_agg_top_ip_route_requests";
    public static final String ROUTE_TOP_IP = "ip_agg_route_top_ip_requests";
    public static final String UNIQUE_IPS = "ip_agg_unique_ips";
    public static final String REQUESTS = "ip_agg_requests";
    public static final String DISTINCT_ROUTES = "ip_agg_distinct_routes";

    private final Map<String, String> common;
    private Set<List<Series.Label>> previous = Set.of();

    /** @param common labels on every series: job, instance, window */
    public ViewSeries(Map<String, String> common) {
        this.common = Map.copyOf(common);
    }

    public List<Series> next(WindowViews v) {
        long ts = v.windowEndMs();
        List<Series> out = new ArrayList<>();
        Set<List<Series.Label>> current = new HashSet<>();
        for (WindowViews.IpCount c : v.topIps()) {
            view(out, current, TOP_IP, Map.of("ip", c.ip()), c.count(), ts);
        }
        for (WindowViews.PairCount c : v.topPairs()) {
            view(out, current, TOP_PAIR, Map.of("ip", c.ip(), "method", c.method(), "route", c.route()), c.count(), ts);
        }
        for (WindowViews.RouteTop r : v.routes()) {
            for (WindowViews.IpCount c : r.topIps()) {
                view(out, current, ROUTE_TOP_IP, Map.of("ip", c.ip(), "method", r.method(), "route", r.route()),
                        c.count(), ts);
            }
        }
        out.add(Series.of(UNIQUE_IPS, common, v.uniqueIps(), ts));
        out.add(Series.of(REQUESTS, common, v.totalRequests(), ts));
        out.add(Series.of(DISTINCT_ROUTES, common, v.distinctRoutes(), ts));
        for (List<Series.Label> gone : previous) {
            if (!current.contains(gone)) {
                out.add(new Series(gone, Series.STALE_NAN, ts));
            }
        }
        previous = current;
        return out;
    }

    private void view(List<Series> out, Set<List<Series.Label>> current, String metric, Map<String, String> labels,
                      long value, long ts) {
        Map<String, String> all = new HashMap<>(common);
        all.putAll(labels);
        List<Series.Label> set = Series.labels(metric, all);
        if (current.add(set)) {
            out.add(new Series(set, value, ts));
        }
    }
}
