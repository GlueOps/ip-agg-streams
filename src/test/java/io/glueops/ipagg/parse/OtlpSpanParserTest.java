package io.glueops.ipagg.parse;

import static io.glueops.ipagg.Spans.str;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

import io.glueops.ipagg.Spans;
import com.google.protobuf.CodedOutputStream;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.proto.trace.v1.Span;

class OtlpSpanParserTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<String> ipAttrs = List.of("client.address", "http.client_ip", "net.peer.ip");

    private OtlpSpanParser parser(OtlpSpanParser.Format format) {
        return new OtlpSpanParser(format, true, ipAttrs, registry);
    }

    private static Map<RequestKey, Long> decode(List<KeyValue<String, Long>> records) {
        Map<RequestKey, Long> out = new HashMap<>();
        records.forEach(kv -> out.put(RequestKey.decode(kv.key), kv.value));
        return out;
    }

    private double spans(String result) {
        return registry.get("ipagg.input.spans").tag("result", result).counter().count();
    }

    @Test
    void countsServerSpansPerKeyWithinOneMessage() {
        byte[] msg = Spans.batch()
                .server("11.0.0.1", "GET", "/api/items", 3)
                .server("11.0.0.1", "POST", "/api/cart", 1)
                .server("11.0.0.2", "GET", "/api/items", 2)
                .span(Span.SpanKind.SPAN_KIND_CLIENT, str("client.address", "11.0.0.9"), str("http.route", "/x"))
                .bytes();

        Map<RequestKey, Long> counts = decode(parser(OtlpSpanParser.Format.OTLP_PROTO).parse(msg));

        assertThat(counts).containsOnly(
                Map.entry(new RequestKey("11.0.0.1", "GET", "/api/items"), 3L),
                Map.entry(new RequestKey("11.0.0.1", "POST", "/api/cart"), 1L),
                Map.entry(new RequestKey("11.0.0.2", "GET", "/api/items"), 2L));
        assertThat(spans("counted")).isEqualTo(6);
        assertThat(spans("skipped_not_server")).isEqualTo(1);
    }

    @Test
    void ipAttributesAreTriedInOrderAndValidated() {
        byte[] msg = Spans.batch()
                .span(Span.SpanKind.SPAN_KIND_SERVER, str("net.peer.ip", "10.0.0.5"), str("client.address", "1.2.3.4"),
                        str("http.method", "get"), str("http.target", "/api/items/77?x=1"))
                .span(Span.SpanKind.SPAN_KIND_SERVER, str("net.peer.ip", "10.0.0.5:4431"))
                .span(Span.SpanKind.SPAN_KIND_SERVER, str("client.address", "not-an-ip"))
                .span(Span.SpanKind.SPAN_KIND_SERVER, str("http.route", "/no-ip"))
                .bytes();

        Map<RequestKey, Long> counts = decode(parser(OtlpSpanParser.Format.OTLP_PROTO).parse(msg));

        assertThat(counts).containsOnly(
                Map.entry(new RequestKey("1.2.3.4", "GET", "/api/items/{id}"), 1L),
                Map.entry(new RequestKey("10.0.0.5", "UNKNOWN", "unknown"), 1L));
        assertThat(spans("skipped_bad_ip")).isEqualTo(1);
        assertThat(spans("skipped_no_ip")).isEqualTo(1);
    }

    @Test
    void parsesOtlpJsonWithHexIds() {
        String json = """
                {"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"shop"}}]},
                 "scopeSpans":[{"scope":{"name":"x"},"spans":[
                  {"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","parentSpanId":"",
                   "name":"GET /api/items","kind":2,"startTimeUnixNano":"1700000000000000000",
                   "endTimeUnixNano":"1700000000040000000","attributes":[
                     {"key":"client.address","value":{"stringValue":"11.0.0.7"}},
                     {"key":"http.request.method","value":{"stringValue":"GET"}},
                     {"key":"http.route","value":{"stringValue":"/api/items"}},
                     {"key":"http.response.status_code","value":{"intValue":"200"}}],
                   "status":{}}]}]}]}
                """;

        Map<RequestKey, Long> counts =
                decode(parser(OtlpSpanParser.Format.OTLP_JSON).parse(json.getBytes(StandardCharsets.UTF_8)));

        assertThat(counts).containsOnly(Map.entry(new RequestKey("11.0.0.7", "GET", "/api/items"), 1L));
    }

    @Test
    void garbageIsCountedNotThrown() {
        assertThat(parser(OtlpSpanParser.Format.OTLP_PROTO).parse(new byte[] {(byte) 0xff, 0x01, 0x02})).isEmpty();
        assertThat(parser(OtlpSpanParser.Format.OTLP_JSON).parse("{not json".getBytes(StandardCharsets.UTF_8)))
                .isEmpty();
        assertThat(registry.get("ipagg.input.messages.failed").counter().count()).isEqualTo(2);
        assertThat(parser(OtlpSpanParser.Format.OTLP_PROTO).parse(null)).isEmpty();
    }

    /** A tempo-traces record as Tempo 3's distributor writes it: PushBytesRequest{traces=2, ids=3, skip=5}. */
    private static byte[] tempoRecord(byte[]... traces) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        for (int i = 0; i < traces.length; i++) {
            out.writeByteArray(2, traces[i]);
            out.writeByteArray(3, new byte[] {(byte) i, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        }
        out.writeBool(5, true);
        out.flush();
        return buf.toByteArray();
    }

    @Test
    void decodesTempoPushBytesRequests() throws IOException {
        byte[] record = tempoRecord(
                Spans.batch().server("11.0.0.1", "GET", "/api/items", 2).bytes(),
                Spans.batch().server("11.0.0.1", "GET", "/api/items", 1)
                        .server("11.0.0.2", "POST", "/api/cart", 1)
                        .span(Span.SpanKind.SPAN_KIND_CLIENT, str("client.address", "11.0.0.9")).bytes());

        Map<RequestKey, Long> counts = decode(parser(OtlpSpanParser.Format.TEMPO).parse(record));

        assertThat(counts).containsOnly(
                Map.entry(new RequestKey("11.0.0.1", "GET", "/api/items"), 3L),
                Map.entry(new RequestKey("11.0.0.2", "POST", "/api/cart"), 1L));
        assertThat(spans("skipped_not_server")).isEqualTo(1);
        assertThat(OtlpSpanParser.Format.of("tempo")).isEqualTo(OtlpSpanParser.Format.TEMPO);
    }

    @Test
    void badTempoRecordsAreCountedNotThrown() {
        assertThat(parser(OtlpSpanParser.Format.TEMPO).parse(new byte[] {0x12, 0x05, 0x01})).isEmpty();
        assertThat(registry.get("ipagg.input.messages.failed").counter().count()).isEqualTo(1);
    }
}
