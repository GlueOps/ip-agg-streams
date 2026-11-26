package io.glueops.ipagg;

import java.util.ArrayList;
import java.util.List;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.TracesData;

/** Builds OTLP payloads like the collector's kafka exporter writes them. */
public final class Spans {

    private final List<Span> spans = new ArrayList<>();

    public static Spans batch() {
        return new Spans();
    }

    /** A SERVER span with the attributes loadgen.py sets. */
    public Spans server(String ip, String method, String route, int times) {
        for (int i = 0; i < times; i++) {
            span(Span.SpanKind.SPAN_KIND_SERVER,
                    str("client.address", ip), str("net.peer.ip", ip), str("http.client_ip", ip),
                    str("http.request.method", method), str("http.route", route), str("http.target", route),
                    num("http.response.status_code", 200));
        }
        return this;
    }

    public Spans span(Span.SpanKind kind, KeyValue... attributes) {
        spans.add(Span.newBuilder().setName("op").setKind(kind).addAllAttributes(List.of(attributes)).build());
        return this;
    }

    public Spans add(Span span) {
        spans.add(span);
        return this;
    }

    public TracesData data() {
        return TracesData.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addAllSpans(spans)))
                .build();
    }

    public byte[] bytes() {
        return data().toByteArray();
    }

    public static KeyValue str(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value)).build();
    }

    public static KeyValue num(String key, long value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setIntValue(value)).build();
    }
}
