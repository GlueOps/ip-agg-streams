package io.glueops.ipagg.parse;

/**
 * One counted dimension combination. Encoded as a single string key for Kafka ("ip␟method␟route"); the IP comes first
 * so the partitioner can hash it alone.
 */
public record RequestKey(String ip, String method, String route) {

    public static final char SEP = '\u001f'; // ASCII unit separator: cannot appear in an IP, a method or a normalized route

    public String encode() {
        return ip + SEP + method + SEP + route;
    }

    public static RequestKey decode(String encoded) {
        int a = encoded.indexOf(SEP);
        int b = a < 0 ? -1 : encoded.indexOf(SEP, a + 1);
        if (a < 0 || b < 0) {
            return new RequestKey(a < 0 ? encoded : encoded.substring(0, a), "UNKNOWN", "unknown");
        }
        return new RequestKey(encoded.substring(0, a), encoded.substring(a + 1, b), encoded.substring(b + 1));
    }

    /** The IP part of an encoded key, without allocating the other parts. */
    public static String ipOf(String encoded) {
        int a = encoded.indexOf(SEP);
        return a < 0 ? encoded : encoded.substring(0, a);
    }
}
