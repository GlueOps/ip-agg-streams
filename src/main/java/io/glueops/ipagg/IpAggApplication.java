package io.glueops.ipagg;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Counts requests per (client IP, HTTP method, route) from OTLP spans on a Kafka topic, in 1-minute windows, and pushes
 * bounded top-N views of each closed window to Prometheus remote write. Prometheus never sees one series per IP.
 */
@SpringBootApplication
@EnableKafkaStreams
@EnableScheduling
@ConfigurationPropertiesScan
public class IpAggApplication {

    public static void main(String[] args) {
        SpringApplication.run(IpAggApplication.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
