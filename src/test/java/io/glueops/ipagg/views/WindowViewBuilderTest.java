package io.glueops.ipagg.views;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.glueops.ipagg.config.IpAggProperties;
import io.glueops.ipagg.parse.RequestKey;
import io.glueops.ipagg.views.WindowViews.IpCount;
import io.glueops.ipagg.views.WindowViews.PairCount;

class WindowViewBuilderTest {

    private static String key(String ip, String method, String route) {
        return new RequestKey(ip, method, route).encode();
    }

    @Test
    void eachViewIsComputedFromTheFullData() {
        WindowViewBuilder b = new WindowViewBuilder(new IpAggProperties.Views(2, 2, 1, 10));
        // "spread" sends 10 to each of 5 routes (50 total) but never tops a single route
        for (int r = 0; r < 5; r++) {
            b.add(key("spread", "GET", "/r" + r), 10);
        }
        b.add(key("login-bot", "POST", "/login"), 40);
        b.add(key("a", "GET", "/r0"), 25);
        b.add(key("b", "GET", "/r1"), 20);

        WindowViews v = b.build(0, 60_000);

        assertThat(v.totalRequests()).isEqualTo(135);
        assertThat(v.uniqueIps()).isEqualTo(4);
        assertThat(v.distinctRoutes()).isEqualTo(6);
        assertThat(v.topIps()).containsExactly(new IpCount("spread", 50), new IpCount("login-bot", 40));
        assertThat(v.topPairs()).containsExactly(
                new PairCount("login-bot", "POST", "/login", 40), new PairCount("a", "GET", "/r0", 25));
        assertThat(v.routes()).extracting(WindowViews.RouteTop::route)
                .containsExactly("/login", "/r0", "/r1", "/r2", "/r3", "/r4");
        assertThat(v.routes().get(1).requests()).isEqualTo(35);
        assertThat(v.routes().get(1).topIps()).containsExactly(new IpCount("a", 25));
        assertThat(v.routes().get(2).topIps()).containsExactly(new IpCount("b", 20));
    }

    @Test
    void routeListIsCappedByVolumeAndTiesAreStable() {
        WindowViewBuilder b = new WindowViewBuilder(new IpAggProperties.Views(3, 3, 3, 2));
        b.add(key("z", "GET", "/quiet"), 1);
        b.add(key("y", "GET", "/busy"), 5);
        b.add(key("x", "GET", "/medium"), 3);
        b.add(key("c", "GET", "/busy"), 5);

        WindowViews v = b.build(0, 60_000);

        assertThat(v.routes()).extracting(WindowViews.RouteTop::route).containsExactly("/busy", "/medium");
        assertThat(v.routes().get(0).topIps()).containsExactly(new IpCount("c", 5), new IpCount("y", 5));
        assertThat(v.distinctRoutes()).isEqualTo(3);
    }
}
