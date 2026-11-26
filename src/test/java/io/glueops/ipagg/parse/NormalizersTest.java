package io.glueops.ipagg.parse;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NormalizersTest {

    @Test
    void routePrefersHttpRoute() {
        assertThat(RouteNormalizer.route("/api/items/{id}", "/api/items/42", null, null)).isEqualTo("/api/items/{id}");
    }

    @Test
    void rawPathsAreNormalized() {
        assertThat(RouteNormalizer.route(null, null, "/api/items/83721?ref=abc#x", null)).isEqualTo("/api/items/{id}");
        assertThat(RouteNormalizer.route(null, "/u/3f2504e0-4f89-11d3-9a0c-0305e82c3301/orders", null, null))
                .isEqualTo("/u/{id}/orders");
        assertThat(RouteNormalizer.route(null, null, null, "https://shop.example.com/c/5f1d7a9b3c2e4d6f/p?q=1"))
                .isEqualTo("/c/{id}/p");
        assertThat(RouteNormalizer.route(null, null, "/reset/eyJhbGciOiJIUzI1NiJ9abc123xyz", null))
                .isEqualTo("/reset/{id}");
        assertThat(RouteNormalizer.route(null, null, "/users/jane@example.com", null)).isEqualTo("/users/{id}");
        assertThat(RouteNormalizer.route(null, null, "//api//cart/", null)).isEqualTo("/api/cart");
        assertThat(RouteNormalizer.route(null, null, "/a/b/c/d/e/f/g/h/i/j", null)).isEqualTo("/a/b/c/d/e/f/g/h/...");
        assertThat(RouteNormalizer.route(null, null, "?x=1", null)).isEqualTo("/");
        assertThat(RouteNormalizer.route(null, null, null, null)).isEqualTo("unknown");
    }

    @Test
    void wordsAreNotIds() {
        assertThat(RouteNormalizer.route(null, null, "/api/v2/checkout/confirm", null))
                .isEqualTo("/api/v2/checkout/confirm");
        assertThat(RouteNormalizer.route(null, null, "/static/deadbeef", null)).isEqualTo("/static/deadbeef");
    }

    @Test
    void methodsAreBounded() {
        assertThat(RouteNormalizer.method("get", null)).isEqualTo("GET");
        assertThat(RouteNormalizer.method(null, "POST")).isEqualTo("POST");
        assertThat(RouteNormalizer.method("PROPFIND", null)).isEqualTo("_OTHER");
        assertThat(RouteNormalizer.method(null, null)).isEqualTo("UNKNOWN");
    }

    @Test
    void ipsAreValidatedAndNormalized() {
        assertThat(IpNormalizer.normalize(" 11.0.0.1 ")).isEqualTo("11.0.0.1");
        assertThat(IpNormalizer.normalize("11.0.0.1:52344")).isEqualTo("11.0.0.1");
        assertThat(IpNormalizer.normalize("203.0.113.9, 10.0.0.1")).isEqualTo("203.0.113.9");
        assertThat(IpNormalizer.normalize("2001:DB8::1")).isEqualTo("2001:db8::1");
        assertThat(IpNormalizer.normalize("[2001:db8::1]:443")).isEqualTo("2001:db8::1");
        assertThat(IpNormalizer.normalize("::ffff:10.1.2.3")).isEqualTo("::ffff:10.1.2.3");
        assertThat(IpNormalizer.normalize("256.1.1.1")).isNull();
        assertThat(IpNormalizer.normalize("example.com")).isNull();
        assertThat(IpNormalizer.normalize("")).isNull();
        assertThat(IpNormalizer.normalize("dead:beef")).isNull();
    }

    @Test
    void requestKeyRoundTrips() {
        RequestKey key = new RequestKey("11.0.0.1", "GET", "/api/items/{id}");
        assertThat(RequestKey.decode(key.encode())).isEqualTo(key);
        assertThat(RequestKey.ipOf(key.encode())).isEqualTo("11.0.0.1");
    }
}
