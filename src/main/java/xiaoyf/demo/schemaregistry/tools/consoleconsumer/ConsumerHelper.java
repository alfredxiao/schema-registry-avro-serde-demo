package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_END_OFFSET;

public class ConsumerHelper {
    private final ConsumerConfigHelper config;
    private final Consumer<Object, Object> consumer;

    @Getter
    private int visitCount;
    @Getter
    private int grepHit;

    @Getter
    private final Map<TopicPartition, Long> latestVisitedOffsets;
    @Getter
    private final Map<TopicPartition, Long> earliestVisitedOffsets;

    ConsumerHelper(Consumer<Object, Object> consumer, ConsumerConfigHelper config, ConsoleHelper console) {
        this.consumer = consumer;
        this.config = config;
        this.visitCount = 0;
        this.grepHit = 0;
        this.latestVisitedOffsets = new HashMap<>();
        this.earliestVisitedOffsets = new HashMap<>();
    }


    void waitForPartitionsAssigned(Duration timeout) throws InterruptedException {

        Integer partition = config.getPartitionOrNull();

        Set<TopicPartition> expectedAssignment = consumer.partitionsFor(config.getTopic())
                .stream()
                .filter(pi -> partition == null || pi.partition() == partition)
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toSet());

        consumer.assign(expectedAssignment);

        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.toMillis()) {

            if (expectedAssignment.equals(consumer.assignment())) {
                return;
            }

            Thread.sleep(1000);
        }

        throw new RuntimeException("timeout waiting for assignment");
    }

    public void seekToExpectedStartingPoint() {
        Long fromEpoch = config.getFromEpochOrNull();
        Long backwardDuration = config.getBackwardDurationOrNull();
        Integer offset = config.geOffsetOrNull();

        if (fromEpoch != null) {
            seekToEpoch(fromEpoch);
            return;
        }

        if (backwardDuration != null) {
            seekToEpoch(Instant.now().toEpochMilli() - backwardDuration);
            return;
        }

        seekToOffset(offset);
    }

    private void seekToEpoch(long fromEpoch) {
        Map<TopicPartition, Long> epochMap = new HashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            epochMap.put(tp, fromEpoch);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(epochMap);
        offsets.forEach((tp, offsetAndTimestamp) -> {
            if (offsetAndTimestamp == null) {
                // this can happen if expected fromEpoch > latestEpoch in topic
                consumer.seekToEnd(Collections.singleton(tp));
            } else {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        });
    }

    private void seekToOffset(Integer offset) {
        if (offset == null) {
            consumer.seekToBeginning(consumer.assignment());
            return;
        }

        if (offset == FROM_END_OFFSET) {
            consumer.seekToEnd(consumer.assignment());
            return;
        }

        if (config.getPartitionOrNull() == null) {
            throw new RuntimeException("when specifying an offset, should also specify a partition number");
        }

        consumer.seek(new TopicPartition(config.getTopic(), config.getPartitionOrNull()), offset);
    }

    public boolean hasReachedGrepOrTotalLimit() {
        return (config.getLimit() != null && visitCount >= config.getLimit())
                || (config.getGrepLimit() != null && grepHit >= config.getGrepLimit());
    }

    public boolean hasReachedTopicEnd() {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            return false;
        }

        consumer.endOffsets(consumer.assignment());
        for (TopicPartition tp : assignment) {
            OptionalLong lag = consumer.currentLag(tp);

            if (lag.isEmpty() || lag.getAsLong() != 0) {
                return false;
            }
        }

        return true;
    }

    public void visitRecord(ConsumerRecord<Object, Object> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        earliestVisitedOffsets.putIfAbsent(tp, record.offset());
        latestVisitedOffsets.put(tp, record.offset());
        visitCount++;
    }

    public void increaseGrepHit() {
        grepHit++;
    }
}
