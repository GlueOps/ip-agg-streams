package io.glueops.ipagg.remotewrite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** One sample of one time series, labels sorted by name as remote write requires. */
public record Series(List<Label> labels, double value, long timestampMs) {

    /** Prometheus' staleness marker: tells Prometheus the series ended, instead of showing its last value for 5m. */
    public static final double STALE_NAN = Double.longBitsToDouble(0x7ff0000000000002L);

    public record Label(String name, String value) {
    }

    public static Series of(String metric, Map<String, String> labels, double value, long timestampMs) {
        return new Series(labels(metric, labels), value, timestampMs);
    }

    /** Sorted label set including __name__; labels with empty values are left out (Prometheus treats them as unset). */
    public static List<Label> labels(String metric, Map<String, String> labels) {
        List<Label> list = new ArrayList<>(labels.size() + 1);
        list.add(new Label("__name__", metric));
        labels.forEach((k, v) -> {
            if (v != null && !v.isEmpty()) {
                list.add(new Label(k, v));
            }
        });
        list.sort(Comparator.comparing(Label::name));
        return List.copyOf(list);
    }

    public static boolean isStale(double value) {
        return Double.doubleToRawLongBits(value) == 0x7ff0000000000002L;
    }
}
