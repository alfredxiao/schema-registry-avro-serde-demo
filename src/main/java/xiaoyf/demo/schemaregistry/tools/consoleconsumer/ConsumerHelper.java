package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_BEGINNING;
import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_BEGINNING_OFFSET;
import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_END;
import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.ConsumerConfigHelper.FROM_END_OFFSET;

public class ConsumerHelper {
    private ConsoleHelper console;
    private ConsumerConfigHelper config;
    private Consumer<Object, Object> consumer;
    private Set<TopicPartition> expectedAssignment;

    ConsumerHelper(Consumer<Object, Object> consumer, ConsumerConfigHelper config, ConsoleHelper console) {
        this.consumer = consumer;
        this.config = config;
        this.console = console;
    }


    void waitForPartitionsAssigned(Duration timeout) throws InterruptedException {

        int partition = config.getPartition();

        expectedAssignment = consumer.partitionsFor(config.getTopic())
                .stream()
                .filter(pi -> partition < 0 || pi.partition() == partition)
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toSet());

        consumer.assign(expectedAssignment);

        console.println("expected assignment: " + expectedAssignment);
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.toMillis()) {
            console.println("current assignment: " + consumer.assignment());

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
}
