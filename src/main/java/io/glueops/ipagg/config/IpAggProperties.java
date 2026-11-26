package io.glueops.ipagg.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param inputTopic      topic the collector's kafka exporter writes spans to
 * @param inputFormat     otlp_proto or otlp_json (the kafka exporter's traces.encoding)
 * @param serverSpansOnly count only SERVER spans, so a request traced by both client and server is counted once
 * @param ipAttributes    span attributes holding the client IP, in priority order
 * @param instance        value of the instance label on pushed series (the pod name)
 */
@ConfigurationProperties("ipagg")
public record IpAggProperties(
        @DefaultValue("otlp-spans-ip") String inputTopic,
        @DefaultValue("otlp_proto") String inputFormat,
        @DefaultValue("true") boolean serverSpansOnly,
        @DefaultValue({"client.address", "http.client_ip", "net.peer.ip"}) List<String> ipAttributes,
        @DefaultValue("local") String instance,
        @DefaultValue Window window,
        @DefaultValue Views views,
        @DefaultValue RemoteWrite remoteWrite) {

    /**
     * @param size           tumbling window length
     * @param grace          how long after a window ends late records are still counted into it
     * @param maxWait        a window is emitted at the latest this long after end + grace, even when some partition's
     *                       stream time has not passed it (idle or lagging partition)
     * @param extraRetention how long a closed window stays readable in the store, to emit it after a restart or outage
     */
    public record Window(
            @DefaultValue("1m") Duration size,
            @DefaultValue("10s") Duration grace,
            @DefaultValue("60s") Duration maxWait,
            @DefaultValue("3m") Duration extraRetention) {

        public Duration retention() {
            return size.plus(grace).plus(maxWait).plus(extraRetention);
        }
    }

    /**
     * @param topIps         series in ip_agg_top_ip_requests
     * @param topPairs       series in ip_agg_top_ip_route_requests
     * @param topIpsPerRoute series per route in ip_agg_route_top_ip_requests
     * @param maxRoutes      routes (by request count) that get a per-route top list
     */
    public record Views(
            @DefaultValue("100") int topIps,
            @DefaultValue("100") int topPairs,
            @DefaultValue("10") int topIpsPerRoute,
            @DefaultValue("200") int maxRoutes) {
    }

    /**
     * @param urls        remote-write endpoints; every series goes to each one (one per Prometheus replica)
     * @param job         value of the job label on pushed series
     * @param maxAttempts tries per endpoint for retryable failures (5xx, 429, I/O errors)
     * @param headers     extra HTTP headers, e.g. Authorization or X-Scope-OrgID
     */
    public record RemoteWrite(
            @DefaultValue List<String> urls,
            @DefaultValue("ip-agg-streams") String job,
            @DefaultValue("10s") Duration timeout,
            @DefaultValue("3") int maxAttempts,
            @DefaultValue Map<String, String> headers) {

        public List<String> endpoints() {
            return urls == null ? List.of() : urls.stream().map(String::trim).filter(u -> !u.isEmpty()).toList();
        }
    }
}
