package xyz.techtwins.kafka.demo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class UserIdPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionsForTopic(topic).size();
        if (keyBytes == null) {
            int p = ThreadLocalRandom.current().nextInt(numPartitions);
            System.out.println("NULL KEY → Partition=" + p);
            return p;
        }
        int hash = Utils.murmur2(keyBytes);
        int positive = hash & 0x7fffffff;
        int partition = positive % numPartitions;

        System.out.printf("Key=%s Bytes=%s Hash=%d Partition=%d%n",
                key, Arrays.toString(keyBytes), positive, partition);

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
