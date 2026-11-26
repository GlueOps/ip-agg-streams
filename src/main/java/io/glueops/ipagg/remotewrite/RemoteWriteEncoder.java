package io.glueops.ipagg.remotewrite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.google.protobuf.CodedOutputStream;

/**
 * Encodes a Prometheus remote write 1.0 WriteRequest by hand, so no generated prompb classes (and no gogoproto) are
 * needed:
 * <pre>
 * message WriteRequest { repeated TimeSeries timeseries = 1; }
 * message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
 * message Label        { string name = 1; string value = 2; }
 * message Sample       { double value = 1; int64 timestamp = 2; }   // timestamp in ms
 * </pre>
 * The HTTP body is this, snappy block-compressed.
 */
public final class RemoteWriteEncoder {

    private RemoteWriteEncoder() {
    }

    public static byte[] encode(List<Series> series) {
        return write(out -> {
            for (Series s : series) {
                out.writeByteArray(1, timeSeries(s));
            }
        });
    }

    private static byte[] timeSeries(Series s) {
        return write(out -> {
            for (Series.Label l : s.labels()) {
                out.writeByteArray(1, write(lo -> {
                    lo.writeString(1, l.name());
                    lo.writeString(2, l.value());
                }));
            }
            out.writeByteArray(2, write(so -> {
                so.writeDouble(1, s.value()); // raw bits, so the stale NaN payload survives
                so.writeInt64(2, s.timestampMs());
            }));
        });
    }

    private interface Body {
        void writeTo(CodedOutputStream out) throws IOException;
    }

    private static byte[] write(Body body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(bytes);
            body.writeTo(out);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
