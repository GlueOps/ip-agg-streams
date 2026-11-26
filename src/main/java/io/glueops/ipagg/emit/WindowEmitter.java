package io.glueops.ipagg.emit;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.glueops.ipagg.config.IpAggProperties;
import io.glueops.ipagg.remotewrite.RemoteWriteClient;
import io.glueops.ipagg.remotewrite.Series;
import io.glueops.ipagg.streams.IpAggTopology;
import io.glueops.ipagg.streams.StreamTimeTracker;
import io.glueops.ipagg.views.WindowViewBuilder;
import io.glueops.ipagg.views.WindowViews;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Every few seconds: for each window that is now closed, read it from this instance's stores (all local partitions,
 * through interactive queries), build the views and push them, stamped with the window end. Windows are emitted in
 * order and exactly once per run; a window that cannot be read yet (rebalance, restore) is retried on the next tick.
 *
 * <p>A window is closed when the stream time of every active partition has passed end + grace (no record can still be
 * added), or at the latest maxWait after that by the wall clock, so an idle or stuck partition cannot block emission.
 */
@Component
public class WindowEmitter {

    private static final Logger log = LoggerFactory.getLogger(WindowEmitter.class);
    static final String HEARTBEAT = "ip_agg_last_push_timestamp_seconds";

    private final StreamsBuilderFactoryBean streams;
    private final IpAggProperties props;
    private final StreamTimeTracker tracker;
    private final RemoteWriteClient client;
    private final Clock clock;
    private final MeterRegistry registry;
    private final Map<String, String> common;
    private final ViewSeries viewSeries;
    private final AtomicLong lastEmittedEnd = new AtomicLong();
    private final AtomicLong lastEmitDelayMs = new AtomicLong();
    private long nextWindowStart = Long.MIN_VALUE;
    private volatile WindowViews latest;

    public WindowEmitter(StreamsBuilderFactoryBean streams, IpAggProperties props, StreamTimeTracker tracker,
                         RemoteWriteClient client, Clock clock, MeterRegistry registry) {
        this.streams = streams;
        this.props = props;
        this.tracker = tracker;
        this.client = client;
        this.clock = clock;
        this.registry = registry;
        this.common = Map.of(
                "job", props.remoteWrite().job(),
                "instance", props.instance(),
                "window", props.window().size().toSeconds() + "s");
        this.viewSeries = new ViewSeries(common);
        Gauge.builder("ipagg.window.last_emitted_end", lastEmittedEnd, v -> v.get() / 1000.0)
                .baseUnit("seconds").description("End of the last window pushed, Unix time").register(registry);
        Gauge.builder("ipagg.window.emit_delay", lastEmitDelayMs, v -> v.get() / 1000.0)
                .baseUnit("seconds").description("Wall clock minus window end when the last window was pushed")
                .register(registry);
    }

    public WindowViews latest() {
        return latest;
    }

    @Scheduled(initialDelayString = "${ipagg.emit-check-interval-ms:5000}",
            fixedDelayString = "${ipagg.emit-check-interval-ms:5000}")
    public void tick() {
        long now = clock.millis();
        long size = props.window().size().toMillis();
        long grace = props.window().grace().toMillis();
        long maxWait = props.window().maxWait().toMillis();
        if (nextWindowStart == Long.MIN_VALUE) {
            // Start with the window before the current one: after a restart it may not have been pushed yet. If it
            // was, the same values are written again at the same timestamps, which Prometheus accepts.
            nextWindowStart = Math.floorDiv(now, size) * size - size;
        }
        long watermark = tracker.watermark(now, 2 * size + maxWait);
        while (true) {
            long end = nextWindowStart + size;
            String trigger = trigger(end, grace, maxWait, now, watermark);
            if (trigger == null) {
                return;
            }
            if (now - end > props.window().retention().toMillis() - size) {
                count("expired");
                log.warn("window ending {} is past the store retention, skipped", Instant.ofEpochMilli(end));
                nextWindowStart += size;
                continue;
            }
            WindowViews views = read(nextWindowStart, end);
            if (views == null) {
                return; // store not queryable now (rebalance/restore): retry this window on the next tick
            }
            push(views, now, trigger);
            nextWindowStart += size;
        }
    }

    /** Why the window ending at {@code end} can be emitted now, or null if it cannot yet. */
    public static String trigger(long end, long grace, long maxWait, long now, long watermark) {
        if (watermark != Long.MIN_VALUE && watermark >= end + grace) {
            return "stream_time";
        }
        return now >= end + grace + maxWait ? "max_wait" : null;
    }

    private WindowViews read(long start, long end) {
        KafkaStreams ks = streams.getKafkaStreams();
        if (ks == null || ks.state() != KafkaStreams.State.RUNNING) {
            return null;
        }
        WindowViewBuilder builder = new WindowViewBuilder(props.views());
        try {
            ReadOnlyWindowStore<String, Long> store = ks.store(
                    StoreQueryParameters.fromNameAndType(IpAggTopology.STORE, QueryableStoreTypes.windowStore()));
            Instant at = Instant.ofEpochMilli(start);
            try (KeyValueIterator<Windowed<String>, Long> it = store.fetchAll(at, at)) {
                while (it.hasNext()) {
                    KeyValue<Windowed<String>, Long> kv = it.next();
                    builder.add(kv.key.key(), kv.value);
                }
            }
        } catch (InvalidStateStoreException e) {
            log.info("window store not queryable yet ({}), retrying", e.getMessage());
            return null;
        }
        return builder.build(start, end);
    }

    private void push(WindowViews views, long now, String trigger) {
        List<Series> series = new ArrayList<>(viewSeries.next(views));
        series.add(Series.of(HEARTBEAT, common, now / 1000.0, now));
        int accepted = client.push(series);
        int endpoints = props.remoteWrite().endpoints().size();
        String result = endpoints == 0 ? "no_endpoints" : accepted == endpoints ? "pushed"
                : accepted == 0 ? "push_failed" : "partially_pushed";
        count(result);
        Counter.builder("ipagg.windows.trigger").tag("trigger", trigger).register(registry).increment();
        latest = views;
        lastEmittedEnd.set(views.windowEndMs());
        lastEmitDelayMs.set(now - views.windowEndMs());
        log.info("window {} ({}): {} requests, {} unique IPs, {} routes, {} series -> {}",
                Instant.ofEpochMilli(views.windowStartMs()), trigger, views.totalRequests(), views.uniqueIps(),
                views.distinctRoutes(), series.size(), result);
    }

    private void count(String result) {
        Counter.builder("ipagg.windows").tag("result", result)
                .description("Closed windows handled, by outcome").register(registry).increment();
    }
}
