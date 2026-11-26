package io.glueops.ipagg.remotewrite;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.xerial.snappy.Snappy;

import io.glueops.ipagg.config.IpAggProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Sends series to every configured remote-write endpoint (one per Prometheus replica). Endpoints are independent: one
 * failing does not stop the others. 5xx, 429 and I/O errors are retried with backoff; other 4xx are not (the data is
 * rejected and would be rejected again).
 */
@Component
public class RemoteWriteClient {

    private static final Logger log = LoggerFactory.getLogger(RemoteWriteClient.class);

    private final IpAggProperties.RemoteWrite cfg;
    private final MeterRegistry registry;
    private final HttpClient http;

    public RemoteWriteClient(IpAggProperties props, MeterRegistry registry) {
        this.cfg = props.remoteWrite();
        this.registry = registry;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        if (cfg.endpoints().isEmpty()) {
            log.warn("ipagg.remote-write.urls is empty: views are computed but not pushed");
        }
    }

    /** @return how many endpoints accepted the batch */
    public int push(List<Series> series) {
        if (series.isEmpty() || cfg.endpoints().isEmpty()) {
            return 0;
        }
        byte[] body;
        try {
            body = Snappy.compress(RemoteWriteEncoder.encode(series));
        } catch (IOException e) {
            throw new IllegalStateException("snappy compression failed", e);
        }
        int accepted = 0;
        for (String url : cfg.endpoints()) {
            if (send(url, body, series.size())) {
                accepted++;
            }
        }
        return accepted;
    }

    private boolean send(String url, byte[] body, int seriesCount) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .timeout(cfg.timeout())
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("User-Agent", "ip-agg-streams")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        cfg.headers().forEach(request::header);
        Timer.Sample timer = Timer.start(registry);
        String result = "error";
        try {
            for (int attempt = 1; attempt <= cfg.maxAttempts(); attempt++) {
                try {
                    HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
                    int code = response.statusCode();
                    if (code / 100 == 2) {
                        result = "success";
                        return true;
                    }
                    if (code != 429 && code / 100 == 4) {
                        result = "rejected";
                        log.warn("remote write to {} rejected ({} series): HTTP {} {}", url, seriesCount, code,
                                abbreviate(response.body()));
                        return false;
                    }
                    log.warn("remote write to {} failed, attempt {}/{}: HTTP {} {}", url, attempt, cfg.maxAttempts(),
                            code, abbreviate(response.body()));
                } catch (IOException e) {
                    log.warn("remote write to {} failed, attempt {}/{}: {}", url, attempt, cfg.maxAttempts(),
                            e.toString());
                }
                if (attempt < cfg.maxAttempts()) {
                    Thread.sleep(500L << (attempt - 1));
                }
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            timer.stop(registry.timer("ipagg.remote_write.duration", "endpoint", url, "result", result));
            Counter.builder("ipagg.remote_write.series").tag("endpoint", url).tag("result", result)
                    .register(registry).increment(seriesCount);
        }
    }

    private static String abbreviate(String s) {
        return s == null ? "" : s.length() <= 300 ? s.strip() : s.substring(0, 300) + "...";
    }
}
