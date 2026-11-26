package io.glueops.ipagg.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * DOWN once Kafka Streams has stopped for good (ERROR / PENDING_ERROR, e.g. after an uncaught exception or a vanished
 * internal topic). It is part of the liveness group, so Kubernetes restarts the pod instead of leaving it running idle.
 * REBALANCING, CREATED and RUNNING are UP.
 */
@Component("kafkaStreams")
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factory;

    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }

    @Override
    public Health health() {
        KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null) {
            return Health.up().withDetail("state", "NOT_CREATED").build();
        }
        KafkaStreams.State state = streams.state();
        Health.Builder builder = switch (state) {
            case ERROR, PENDING_ERROR -> Health.down();
            case NOT_RUNNING, PENDING_SHUTDOWN -> factory.isRunning() ? Health.down() : Health.up();
            default -> Health.up();
        };
        return builder.withDetail("state", state.name()).build();
    }
}
