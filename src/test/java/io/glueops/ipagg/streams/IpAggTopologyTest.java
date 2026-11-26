package io.glueops.ipagg.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.glueops.ipagg.Spans;
import io.glueops.ipagg.config.IpAggProperties;
import io.glueops.ipagg.emit.WindowEmitter;
import io.glueops.ipagg.parse.OtlpSpanParser;
import io.glueops.ipagg.parse.RequestKey;
import io.glueops.ipagg.views.WindowViewBuilder;
import io.glueops.ipagg.views.WindowViews;
import io.glueops.ipagg.views.WindowViews.IpCount;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class IpAggTopologyTest {

    private static final Instant T0 = Instant.parse("2026-11-25T10:00:00Z");

    private final IpAggProperties props = new IpAggProperties("otlp-spans-ip", "otlp_proto", true,
            List.of("client.address", "http.client_ip", "net.peer.ip"), "test",
            new IpAggProperties.Window(Duration.ofMinutes(1), Duration.ofSeconds(10), Duration.ofSeconds(60),
                    Duration.ofMinutes(3)),
            new IpAggProperties.Views(10, 10, 3, 50),
            new IpAggProperties.RemoteWrite(List.of(), "ip-agg-streams", Duration.ofSeconds(1), 1, Map.of()));
    private final StreamTimeTracker tracker = new StreamTimeTracker();
    private TopologyTestDriver driver;
    private TestInputTopic<byte[], byte[]> input;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        IpAggTopology.define(builder, props, new OtlpSpanParser(OtlpSpanParser.Format.OTLP_PROTO, true,
                props.ipAttributes(), new SimpleMeterRegistry()), tracker);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ip-agg-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic("otlp-spans-ip", new ByteArraySerializer(), new ByteArraySerializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private WindowViews viewsOf(Instant windowStart) {
        WindowStore<String, Long> store = driver.getWindowStore(IpAggTopology.STORE);
        WindowViewBuilder b = new WindowViewBuilder(props.views());
        try (KeyValueIterator<Windowed<String>, Long> it = store.fetchAll(windowStart, windowStart)) {
            while (it.hasNext()) {
                KeyValue<Windowed<String>, Long> kv = it.next();
                b.add(kv.key.key(), kv.value);
            }
        }
        long start = windowStart.toEpochMilli();
        return b.build(start, start + 60_000);
    }

    @Test
    void countsPerIpMethodRouteInTumblingWindows() {
        input.pipeInput(null, Spans.batch()
                .server("11.0.0.1", "POST", "/api/login", 5)
                .server("11.0.0.2", "GET", "/api/items", 2).bytes(), T0.plusSeconds(5));
        input.pipeInput(null, Spans.batch()
                .server("11.0.0.1", "POST", "/api/login", 3)
                .server("11.0.0.1", "GET", "/api/items", 1).bytes(), T0.plusSeconds(50));
        input.pipeInput(null, Spans.batch().server("11.0.0.3", "GET", "/api/items", 4).bytes(), T0.plusSeconds(65));
        // late but inside the grace period of the first window: still counted there
        input.pipeInput(null, Spans.batch().server("11.0.0.2", "GET", "/api/items", 1).bytes(), T0.plusSeconds(59));
        // after end + grace: dropped by the window
        input.pipeInput(null, Spans.batch().server("11.0.0.9", "GET", "/x", 1).bytes(), T0.plusSeconds(80));
        input.pipeInput(null, Spans.batch().server("11.0.0.9", "GET", "/late", 99).bytes(), T0.plusSeconds(30));

        WindowViews first = viewsOf(T0);
        assertThat(first.totalRequests()).isEqualTo(12);
        assertThat(first.uniqueIps()).isEqualTo(2);
        assertThat(first.topIps()).containsExactly(new IpCount("11.0.0.1", 9), new IpCount("11.0.0.2", 3));
        assertThat(first.topPairs().get(0)).isEqualTo(new WindowViews.PairCount("11.0.0.1", "POST", "/api/login", 8));
        assertThat(first.routes()).extracting(WindowViews.RouteTop::route).containsExactly("/api/login", "/api/items");

        WindowViews second = viewsOf(T0.plusSeconds(60));
        assertThat(second.topIps()).containsExactly(new IpCount("11.0.0.3", 4), new IpCount("11.0.0.9", 1));
    }

    @Test
    void streamTimeDecidesWhenAWindowIsClosed() {
        long end = T0.plusSeconds(60).toEpochMilli();
        input.pipeInput(null, Spans.batch().server("11.0.0.1", "GET", "/a", 1).bytes(), T0.plusSeconds(69));
        long wall = System.currentTimeMillis();
        assertThat(WindowEmitter.trigger(end, 10_000, 60_000, end + 9_000, tracker.watermark(wall, 180_000))).isNull();

        input.pipeInput(null, Spans.batch().server("11.0.0.1", "GET", "/a", 1).bytes(), T0.plusSeconds(70));
        wall = System.currentTimeMillis();
        assertThat(WindowEmitter.trigger(end, 10_000, 60_000, end, tracker.watermark(wall, 180_000)))
                .isEqualTo("stream_time");
        assertThat(WindowEmitter.trigger(end, 10_000, 60_000, end + 70_000, Long.MIN_VALUE)).isEqualTo("max_wait");
    }

    @Test
    void allKeysOfAnIpShareAPartition() {
        int p = IpPartitioner.partition(new RequestKey("11.0.0.1", "GET", "/a").encode(), 12);
        for (String route : List.of("/b", "/c", "/api/items/{id}")) {
            assertThat(IpPartitioner.partition(new RequestKey("11.0.0.1", "POST", route).encode(), 12)).isEqualTo(p);
        }
    }
}
