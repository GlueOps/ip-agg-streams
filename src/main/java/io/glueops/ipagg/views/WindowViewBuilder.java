package io.glueops.ipagg.views;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.glueops.ipagg.config.IpAggProperties;
import io.glueops.ipagg.parse.RequestKey;
import io.glueops.ipagg.views.WindowViews.IpCount;
import io.glueops.ipagg.views.WindowViews.PairCount;
import io.glueops.ipagg.views.WindowViews.RouteTop;

/**
 * Builds all views of one window in a single pass over its (ip␟method␟route, count) entries. Each view is computed
 * from the full data, not from another view: an IP spread over many routes can be a top IP without any top pair.
 */
public final class WindowViewBuilder {

    private static final Comparator<RouteAcc> ROUTE_ORDER = Comparator.comparingLong((RouteAcc r) -> r.requests)
            .thenComparing((RouteAcc r) -> r.method, Comparator.reverseOrder())
            .thenComparing((RouteAcc r) -> r.route, Comparator.reverseOrder());

    private final IpAggProperties.Views cfg;
    private final Map<String, long[]> ipTotals = new HashMap<>();
    private final Map<String, RouteAcc> routes = new HashMap<>();
    private final TopN<PairCount> pairs;
    private long total;

    public WindowViewBuilder(IpAggProperties.Views cfg) {
        this.cfg = cfg;
        this.pairs = new TopN<>(cfg.topPairs(), PairCount.ORDER);
    }

    public void add(String encodedKey, long count) {
        RequestKey key = RequestKey.decode(encodedKey);
        total += count;
        ipTotals.computeIfAbsent(key.ip(), k -> new long[1])[0] += count;
        pairs.offer(new PairCount(key.ip(), key.method(), key.route(), count));
        RouteAcc route = routes.computeIfAbsent(key.method() + RequestKey.SEP + key.route(),
                k -> new RouteAcc(key.method(), key.route(), new TopN<>(cfg.topIpsPerRoute(), IpCount.ORDER)));
        route.requests += count;
        route.top.offer(new IpCount(key.ip(), count)); // one entry per IP: (ip, method, route) keys are unique
    }

    public WindowViews build(long windowStartMs, long windowEndMs) {
        TopN<IpCount> topIps = new TopN<>(cfg.topIps(), IpCount.ORDER);
        ipTotals.forEach((ip, c) -> topIps.offer(new IpCount(ip, c[0])));
        TopN<RouteAcc> busiest = new TopN<>(cfg.maxRoutes(), ROUTE_ORDER);
        routes.values().forEach(busiest::offer);
        List<RouteTop> routeTops = busiest.descending().stream()
                .map(r -> new RouteTop(r.method, r.route, r.requests, r.top.descending()))
                .toList();
        return new WindowViews(windowStartMs, windowEndMs, total, ipTotals.size(), routes.size(),
                topIps.descending(), pairs.descending(), routeTops);
    }

    private static final class RouteAcc {
        final String method;
        final String route;
        final TopN<IpCount> top;
        long requests;

        RouteAcc(String method, String route, TopN<IpCount> top) {
            this.method = method;
            this.route = route;
            this.top = top;
        }
    }
}
