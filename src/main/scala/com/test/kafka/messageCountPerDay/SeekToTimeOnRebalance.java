package com.test.kafka.messageCountPerDay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SeekToTimeOnRebalance implements ConsumerRebalanceListener {
    private Consumer<?, ?> consumer;
    private final Long startTimestamp;

    public SeekToTimeOnRebalance(Consumer<?, ?> consumer, Long startTimestamp) {
        this.consumer = consumer;
        this.startTimestamp = startTimestamp;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition,  startTimestamp);
        }
        // for each assigned partition, find the earliest offset in that partition with a timestamp
        // greater than or equal to the input timestamp
        Map<TopicPartition, OffsetAndTimestamp> outOffsets = consumer.offsetsForTimes(timestampsToSearch);
        for (TopicPartition partition : partitions) {
            Long seekOffset = outOffsets.get(partition).offset();
            Long currentPosition = consumer.position(partition);
            // seek to the offset returned by the offsetsForTimes API
            // if it is beyond the current position
            if (seekOffset.compareTo(currentPosition) > 0) {
                consumer.seek(partition, seekOffset);
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

}
