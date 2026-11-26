package io.glueops.ipagg.parse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import com.google.protobuf.util.JsonFormat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.TracesData;

/**
 * Turns one Kafka message into request counts per encoded {@link RequestKey}. Counts are summed within the message, so
 * a heavy hitter's spans in one batch become one record.
 * <ul>
 *   <li>{@code otlp_proto} / {@code otlp_json}: a TracesData batch from the collector's kafka exporter.</li>
 *   <li>{@code tempo}: a record of Tempo's own {@code tempo-traces} topic, written by the distributor
 *   (Tempo 3, {@code pkg/ingest/encoding.go}): a {@code tempopb.PushBytesRequest} whose field 2 ({@code traces}) holds
 *   one marshalled {@code tempopb.Trace} per trace. {@code tempopb.Trace} has the same wire format as OTLP TracesData
 *   ({@code repeated ResourceSpans = 1}). This is Tempo's internal format, not a public contract: recheck it on a
 *   Tempo upgrade ({@code ipagg_input_messages_failed_total} rises if it changes).</li>
 * </ul>
 */
public class OtlpSpanParser {

    public enum Format {
        OTLP_PROTO, OTLP_JSON, TEMPO;

        public static Format of(String value) {
            return Format.valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    private static final JsonFormat.Parser JSON = JsonFormat.parser().ignoringUnknownFields();

    private final Format format;
    private final boolean serverSpansOnly;
    private final List<String> ipAttributes;
    private final Counter counted;
    private final Counter notServer;
    private final Counter noIp;
    private final Counter badIp;
    private final Counter badMessages;

    public OtlpSpanParser(Format format, boolean serverSpansOnly, List<String> ipAttributes, MeterRegistry registry) {
        this.format = format;
        this.serverSpansOnly = serverSpansOnly;
        this.ipAttributes = List.copyOf(ipAttributes);
        this.counted = spans(registry, "counted");
        this.notServer = spans(registry, "skipped_not_server");
        this.noIp = spans(registry, "skipped_no_ip");
        this.badIp = spans(registry, "skipped_bad_ip");
        this.badMessages = Counter.builder("ipagg.input.messages.failed")
                .description("Kafka messages that could not be decoded in the configured input format").register(registry);
    }

    private static Counter spans(MeterRegistry registry, String result) {
        return Counter.builder("ipagg.input.spans").tag("result", result)
                .description("Spans read from the input topic, by outcome").register(registry);
    }

    public List<KeyValue<String, Long>> parse(byte[] value) {
        if (value == null || value.length == 0) {
            return List.of();
        }
        TracesData data;
        try {
            data = decode(value);
        } catch (Exception e) {
            badMessages.increment();
            return List.of();
        }
        Map<String, Long> counts = new HashMap<>();
        long ok = 0, skippedKind = 0, skippedNoIp = 0, skippedBadIp = 0;
        for (ResourceSpans rs : data.getResourceSpansList()) {
            for (ScopeSpans ss : rs.getScopeSpansList()) {
                for (Span span : ss.getSpansList()) {
                    if (serverSpansOnly && span.getKind() != Span.SpanKind.SPAN_KIND_SERVER) {
                        skippedKind++;
                        continue;
                    }
                    Extracted x = extract(span);
                    if (x.rawIp == null) {
                        skippedNoIp++;
                        continue;
                    }
                    String ip = IpNormalizer.normalize(x.rawIp);
                    if (ip == null) {
                        skippedBadIp++;
                        continue;
                    }
                    String key = new RequestKey(ip, RouteNormalizer.method(x.method, x.legacyMethod),
                            RouteNormalizer.route(x.route, x.urlPath, x.target, x.urlFull)).encode();
                    counts.merge(key, 1L, Long::sum);
                    ok++;
                }
            }
        }
        counted.increment(ok);
        notServer.increment(skippedKind);
        noIp.increment(skippedNoIp);
        badIp.increment(skippedBadIp);
        List<KeyValue<String, Long>> out = new ArrayList<>(counts.size());
        counts.forEach((k, v) -> out.add(KeyValue.pair(k, v)));
        return out;
    }

    private TracesData decode(byte[] value) throws Exception {
        if (format == Format.OTLP_PROTO) {
            return TracesData.parseFrom(value);
        }
        if (format == Format.TEMPO) {
            return decodeTempo(value);
        }
        // OTLP JSON encodes trace/span IDs as hex, not base64. Hex is valid base64 of the same length class, so
        // JsonFormat decodes them into wrong bytes without failing; the IDs are never used here.
        TracesData.Builder builder = TracesData.newBuilder();
        JSON.merge(new String(value, StandardCharsets.UTF_8), builder);
        return builder.build();
    }

    /** PushBytesRequest: 2 = repeated bytes traces, 3 = repeated bytes ids, 5 = bool skipMetricsGeneration. */
    private static final int TEMPO_TRACES_FIELD = 2;

    static TracesData decodeTempo(byte[] value) throws Exception {
        TracesData.Builder all = TracesData.newBuilder();
        CodedInputStream in = CodedInputStream.newInstance(value);
        int tag;
        while ((tag = in.readTag()) != 0) {
            if (WireFormat.getTagFieldNumber(tag) == TEMPO_TRACES_FIELD
                    && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                all.addAllResourceSpans(TracesData.parseFrom(in.readBytes()).getResourceSpansList());
            } else if (!in.skipField(tag)) {
                break;
            }
        }
        return all.build();
    }

    private static final class Extracted {
        String[] ips;
        String rawIp;
        String method, legacyMethod, route, urlPath, target, urlFull;
    }

    private Extracted extract(Span span) {
        Extracted x = new Extracted();
        x.ips = new String[ipAttributes.size()];
        for (io.opentelemetry.proto.common.v1.KeyValue kv : span.getAttributesList()) {
            switch (kv.getKey()) {
                case "http.request.method" -> x.method = str(kv.getValue());
                case "http.method" -> x.legacyMethod = str(kv.getValue());
                case "http.route" -> x.route = str(kv.getValue());
                case "url.path" -> x.urlPath = str(kv.getValue());
                case "http.target" -> x.target = str(kv.getValue());
                case "url.full", "http.url" -> x.urlFull = str(kv.getValue());
                default -> { }
            }
            int i = ipAttributes.indexOf(kv.getKey());
            if (i >= 0) {
                x.ips[i] = str(kv.getValue());
            }
        }
        for (String ip : x.ips) {
            if (ip != null && !ip.isBlank()) {
                x.rawIp = ip;
                break;
            }
        }
        return x;
    }

    private static String str(AnyValue v) {
        return switch (v.getValueCase()) {
            case STRING_VALUE -> v.getStringValue();
            case INT_VALUE -> Long.toString(v.getIntValue());
            default -> null;
        };
    }
}
