package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class LogicalTypeConsoleConsumer {


    public static void main(String[] args) throws Exception {
        // create Options object
        final CommandLineHelper command = new CommandLineHelper(args);
        final ConsumerConfigHelper config = new ConsumerConfigHelper(command);
        final ConsoleHelper console = new ConsoleHelper();

        Properties consumerConfig = config.getConsumerConfig();
        console.println(consumerConfig.toString());

        final Consumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig);
        final ConsumerHelper helper = new ConsumerHelper(consumer, config, console);

        helper.waitForPartitionsAssigned(Duration.ofSeconds(30));
        helper.seekToExpectedOffset();

        int seenCount = 0;
        Map<TopicPartition, Long> seenOffsetsPlusOne = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<Object, Object> record : records) {
                    Object key = record.key();
                    Object value = record.value();

                    Object valueForLogging = valueToLog(value, config.valueFields());
                    console.printf("p=%d,o=%d,t=%d,k=%s,v=%s\n",
                            record.partition(), record.offset(), record.timestamp(), key, valueForLogging);

                    seenOffsetsPlusOne.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);

                    seenCount++;
                    if (config.getLimit() > 0 && seenCount >= config.getLimit()) {
                        console.printf("# limit %d reached, exit", config.getLimit());
                        return;
                    }
                }

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
                if (!endOffsets.isEmpty() && seenOffsetsPlusOne.equals(endOffsets)) {
                    console.println("All records consumed, exit");
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }
    

    private static Object valueToLog(Object value, Set<String> valueFields) {
        if (value == null) {
            return null;
        }

        if (!(value instanceof GenericRecord record)) {
            return value;
        }

        if (valueFields.isEmpty()) {
            return value;
        }

        return GenericRecordHelper.selectFields(record, valueFields);
    }
}

// todo
// feature: counts, size, filter, print offset/partition
// add --consumer-config support