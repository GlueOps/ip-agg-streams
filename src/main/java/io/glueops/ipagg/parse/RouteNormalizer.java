package io.glueops.ipagg.parse;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Bounded route and method values. http.route is a template (/api/items/{id}) and is used as is. Without it the raw
 * path (url.path, http.target, url.full) is normalized: query and fragment dropped, ID-like segments replaced by {id},
 * depth and length capped, so raw URLs cannot multiply the per-IP state.
 */
public final class RouteNormalizer {

    static final int MAX_LENGTH = 128;
    static final int MAX_SEGMENTS = 8;
    static final String UNKNOWN_ROUTE = "unknown";
    static final String UNKNOWN_METHOD = "UNKNOWN";
    static final String OTHER_METHOD = "_OTHER";

    private static final Set<String> METHODS =
            Set.of("GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH");
    private static final Pattern NUMERIC = Pattern.compile("\\d+");
    private static final Pattern UUID =
            Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern HEX = Pattern.compile("[0-9a-fA-F]{12,}");
    private static final Pattern TOKEN = Pattern.compile("[A-Za-z0-9_\\-.~=]{24,}");
    private static final Pattern HAS_DIGIT = Pattern.compile(".*\\d.*");

    private RouteNormalizer() {
    }

    public static String method(String requestMethod, String legacyMethod) {
        String m = notBlank(requestMethod) ? requestMethod : legacyMethod;
        if (!notBlank(m)) {
            return UNKNOWN_METHOD;
        }
        m = m.trim().toUpperCase(Locale.ROOT);
        return METHODS.contains(m) ? m : OTHER_METHOD;
    }

    public static String route(String httpRoute, String urlPath, String target, String urlFull) {
        if (notBlank(httpRoute)) {
            return cap(httpRoute.trim());
        }
        String raw = notBlank(urlPath) ? urlPath : notBlank(target) ? target : urlFull;
        return notBlank(raw) ? normalizePath(raw) : UNKNOWN_ROUTE;
    }

    static String normalizePath(String raw) {
        String p = raw.trim();
        int scheme = p.indexOf("://");
        if (scheme >= 0) {
            int slash = p.indexOf('/', scheme + 3);
            p = slash < 0 ? "/" : p.substring(slash);
        }
        int cut = indexOfAny(p, '?', '#');
        if (cut >= 0) {
            p = p.substring(0, cut);
        }
        StringBuilder sb = new StringBuilder();
        int segments = 0;
        for (String seg : p.split("/")) {
            if (seg.isEmpty()) {
                continue;
            }
            if (++segments > MAX_SEGMENTS) {
                sb.append("/...");
                break;
            }
            sb.append('/').append(isIdLike(seg) ? "{id}" : seg);
        }
        return sb.isEmpty() ? "/" : cap(sb.toString());
    }

    static boolean isIdLike(String seg) {
        if (NUMERIC.matcher(seg).matches() || UUID.matcher(seg).matches() || seg.indexOf('@') >= 0) {
            return true;
        }
        boolean digit = HAS_DIGIT.matcher(seg).matches();
        return digit && (HEX.matcher(seg).matches() || TOKEN.matcher(seg).matches());
    }

    private static int indexOfAny(String s, char a, char b) {
        int i = s.indexOf(a);
        int j = s.indexOf(b);
        return i < 0 ? j : j < 0 ? i : Math.min(i, j);
    }

    private static String cap(String s) {
        return s.length() <= MAX_LENGTH ? s : s.substring(0, MAX_LENGTH);
    }

    static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }
}
