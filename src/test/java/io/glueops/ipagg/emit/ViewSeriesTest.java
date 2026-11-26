package io.glueops.ipagg.emit;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.protobuf.CodedInputStream;

import io.glueops.ipagg.remotewrite.RemoteWriteEncoder;
import io.glueops.ipagg.remotewrite.Series;
import io.glueops.ipagg.views.WindowViews;
import io.glueops.ipagg.views.WindowViews.IpCount;
import io.glueops.ipagg.views.WindowViews.PairCount;
import io.glueops.ipagg.views.WindowViews.RouteTop;

class ViewSeriesTest {

    private final ViewSeries viewSeries = new ViewSeries(Map.of("job", "ip-agg-streams", "instance", "pod-0", "window", "60s"));

    private static WindowViews views(long end, List<IpCount> topIps) {
        return new WindowViews(end - 60_000, end, 100, 7, 1, topIps,
                List.of(new PairCount("11.0.0.1", "GET", "/api/items", 9)),
                List.of(new RouteTop("GET", "/api/items", 50, List.of(new IpCount("11.0.0.1", 9)))));
    }

    private static Map<String, String> labels(Series s) {
        Map<String, String> m = new LinkedHashMap<>();
        s.labels().forEach(l -> m.put(l.name(), l.value()));
        return m;
    }

    @Test
    void viewsBecomeSeriesAtTheWindowEnd() {
        List<Series> series = viewSeries.next(views(120_000, List.of(new IpCount("11.0.0.1", 9))));

        assertThat(series).hasSize(6).allMatch(s -> s.timestampMs() == 120_000);
        assertThat(series).anySatisfy(s -> {
            assertThat(labels(s)).containsExactly(
                    Map.entry("__name__", "ip_agg_top_ip_route_requests"), Map.entry("instance", "pod-0"),
                    Map.entry("ip", "11.0.0.1"), Map.entry("job", "ip-agg-streams"), Map.entry("method", "GET"),
                    Map.entry("route", "/api/items"), Map.entry("window", "60s"));
            assertThat(s.value()).isEqualTo(9);
        });
        assertThat(series).anySatisfy(s -> {
            assertThat(labels(s)).containsEntry("__name__", "ip_agg_unique_ips").doesNotContainKey("ip");
            assertThat(s.value()).isEqualTo(7);
        });
    }

    @Test
    void seriesThatLeaveAViewGetAStaleMarker() {
        viewSeries.next(views(120_000, List.of(new IpCount("11.0.0.1", 9), new IpCount("11.0.0.2", 8))));
        List<Series> next = viewSeries.next(views(180_000, List.of(new IpCount("11.0.0.1", 9))));

        List<Series> stale = next.stream().filter(s -> Series.isStale(s.value())).toList();
        assertThat(stale).singleElement().satisfies(s -> {
            assertThat(labels(s)).containsEntry("__name__", "ip_agg_top_ip_requests").containsEntry("ip", "11.0.0.2");
            assertThat(s.timestampMs()).isEqualTo(180_000);
        });
    }

    @Test
    void encodesAValidWriteRequest() throws IOException {
        Series stale = new Series(Series.labels("m", Map.of("ip", "1.2.3.4")), Series.STALE_NAN, 42);
        byte[] bytes = RemoteWriteEncoder.encode(List.of(Series.of("up", Map.of("job", "x"), 1.5, 1000), stale));

        List<Decoded> decoded = decodeWriteRequest(bytes);

        assertThat(decoded).hasSize(2);
        assertThat(decoded.get(0).labels).containsExactly("__name__", "up", "job", "x");
        assertThat(decoded.get(0).value).isEqualTo(1.5);
        assertThat(decoded.get(0).ts).isEqualTo(1000);
        assertThat(decoded.get(1).labels).containsExactly("__name__", "m", "ip", "1.2.3.4");
        assertThat(Double.doubleToRawLongBits(decoded.get(1).value)).isEqualTo(0x7ff0000000000002L);
        assertThat(decoded.get(1).ts).isEqualTo(42);
    }

    private record Decoded(List<String> labels, double value, long ts) {
    }

    /** Minimal reader for the WriteRequest wire format, independent of the encoder. */
    private static List<Decoded> decodeWriteRequest(byte[] bytes) throws IOException {
        List<Decoded> out = new ArrayList<>();
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        while (!in.isAtEnd()) {
            assertThat(in.readTag() >>> 3).isEqualTo(1);
            CodedInputStream ts = CodedInputStream.newInstance(in.readByteArray());
            List<String> labels = new ArrayList<>();
            double value = 0;
            long t = 0;
            while (!ts.isAtEnd()) {
                int field = ts.readTag() >>> 3;
                CodedInputStream sub = CodedInputStream.newInstance(ts.readByteArray());
                while (!sub.isAtEnd()) {
                    int tag = sub.readTag();
                    if (field == 1) {
                        labels.add(sub.readString());
                    } else if ((tag >>> 3) == 1) {
                        value = sub.readDouble();
                    } else {
                        t = sub.readInt64();
                    }
                }
            }
            out.add(new Decoded(labels, value, t));
        }
        return out;
    }
}
