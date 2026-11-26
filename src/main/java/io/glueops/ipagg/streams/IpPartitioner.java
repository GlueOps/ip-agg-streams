package io.glueops.ipagg.streams;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

import io.glueops.ipagg.parse.RequestKey;

/**
 * Partitions encoded request keys by their IP alone. Every (ip, method, route) key of one IP lands on the same
 * partition, so each IP's full traffic lives on exactly one task and instance. That is what makes the per-instance top
 * lists exact once merged with topk() in PromQL: the global top N is always inside the union of the local top Ns.
 */
public class IpPartitioner implements StreamPartitioner<String, Long> {

    @Override
    public Optional<Set<Integer>> partitions(String topic, String key, Long value, int numPartitions) {
        return Optional.of(Set.of(partition(key, numPartitions)));
    }

    public static int partition(String encodedKey, int numPartitions) {
        byte[] ip = RequestKey.ipOf(encodedKey).getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(ip)) % numPartitions;
    }
}
