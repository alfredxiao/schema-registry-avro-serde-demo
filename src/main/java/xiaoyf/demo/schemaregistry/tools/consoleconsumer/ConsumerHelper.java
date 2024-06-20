package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_BEGINNING_OFFSET;
import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_END_OFFSET;

public class ConsumerHelper {
    private final ConsumerConfigHelper config;
    private final Consumer<Object, Object> consumer;
    private int recordCount;
    private int grepHit;

    @Getter
    private final Map<TopicPartition, Long> visitedOffsets;;

    ConsumerHelper(Consumer<Object, Object> consumer, ConsumerConfigHelper config) {
        this.consumer = consumer;
        this.config = config;
        this.recordCount = 0;
        this.grepHit = 0;
        this.visitedOffsets = new HashMap<>();
    }


    void waitForPartitionsAssigned(Duration timeout) throws InterruptedException {

        int partition = config.getPartition();

        Set<TopicPartition> expectedAssignment = consumer.partitionsFor(config.getTopic())
                .stream()
                .filter(pi -> partition < 0 || pi.partition() == partition)
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

    public void seekToExpectedOffset() {
        int offset = config.getFromOffset();

        switch (offset) {
            case FROM_BEGINNING_OFFSET:
                consumer.seekToBeginning(consumer.assignment());
                return;
            case FROM_END_OFFSET:
                consumer.seekToEnd(consumer.assignment());
                return;
            default:
                if (config.getPartition() < 0) {
                    throw new RuntimeException("when specifying an offset, should also specify a partition number");
                }

                consumer.seek(new TopicPartition(config.getTopic(), config.getPartition()), offset);
        }
    }

    public boolean hasReachedGrepOrTotalLimit() {
        return (config.getLimit() > 0 && recordCount >= config.getLimit())
                || (config.getGrepLimit() > 0 && grepHit >= config.getGrepLimit());
    }

    public boolean hasReachedTopicEnd() {
        boolean endReached = !consumer.assignment().isEmpty();
        for (TopicPartition tp : consumer.assignment()) {
            OptionalLong lag = consumer.currentLag(tp);
            if (lag.isEmpty() || lag.getAsLong() != 0) {
                endReached = false;
                break;
            }
        }

        return endReached;
    }

    public void visitRecord(ConsumerRecord<Object, Object> record) {
        visitedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
        recordCount++;
    }

    public void increaseGrepHit() {
        grepHit++;
    }
}
