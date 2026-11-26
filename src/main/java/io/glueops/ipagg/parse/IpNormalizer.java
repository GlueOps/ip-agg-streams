package io.glueops.ipagg.parse;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Validates and normalizes a client IP attribute value without DNS lookups. Returns null for anything else. */
public final class IpNormalizer {

    private static final Pattern IPV4 = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
    private static final Pattern IPV6 = Pattern.compile("[0-9A-Fa-f:.]{2,45}");

    private IpNormalizer() {
    }

    public static String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.trim();
        int comma = s.indexOf(','); // X-Forwarded-For style list: the first entry is the original client
        if (comma >= 0) {
            s = s.substring(0, comma).trim();
        }
        if (s.isEmpty() || s.length() > 64) {
            return null;
        }
        if (s.startsWith("[")) { // [v6]:port
            int end = s.indexOf(']');
            if (end < 0) {
                return null;
            }
            s = s.substring(1, end);
        } else {
            int colon = s.indexOf(':');
            if (colon > 0 && colon == s.lastIndexOf(':') && s.indexOf('.') > 0) { // v4:port
                s = s.substring(0, colon);
            }
        }
        Matcher v4 = IPV4.matcher(s);
        if (v4.matches()) {
            for (int g = 1; g <= 4; g++) {
                if (Integer.parseInt(v4.group(g)) > 255) {
                    return null;
                }
            }
            return s;
        }
        if (s.indexOf(':') >= 0 && s.indexOf(':') != s.lastIndexOf(':') && IPV6.matcher(s).matches()) {
            return s.toLowerCase(Locale.ROOT);
        }
        return null;
    }
}
