package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;

import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.CommandLineHelper._GREP;
import static xiaoyf.demo.schemaregistry.tools.consoleconsumer.MatchHelper.match;


public class LogicalTypeConsoleConsumer {

    public static void main(String[] args) throws Exception {
        final CommandLineHelper command = new CommandLineHelper(args);
        final ConsumerConfigHelper config = new ConsumerConfigHelper(command);
        final ConsoleHelper console = new ConsoleHelper();

        Properties consumerConfig = config.getConsumerConfig();

        final Consumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig);
        final ConsumerHelper helper = new ConsumerHelper(consumer, config, console);

        helper.waitForPartitionsAssigned(Duration.ofSeconds(30));
        helper.seekToExpectedStartingPoint();

        try {
            final String grep = command.getOptionOrNull(_GREP);

            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<Object, Object> record : records) {
                    Object key = record.key();
                    Object value = record.value();

                    String keyString = valueStringToLog(key);
                    String valueString = valueStringToLog(value, config.valueFields());

                    if (StringUtils.isEmpty(grep)
                            || match(keyString, grep)
                            || match(valueString, grep)) {

                        console.logf("p=%d,o=%d,ts=%d,t=%s,k=%s,v=%s\n",
                                record.partition(), record.offset(), record.timestamp(),
                                Instant.ofEpochMilli(record.timestamp()), keyString, valueString);

                        if (!StringUtils.isEmpty(grep)) {
                            helper.increaseGrepHit();
                        }
                    }

                    helper.visitRecord(record);

                    if (helper.hasReachedGrepOrTotalLimit()) {
                        report(console, config, helper, consumer, grep);
                        return;
                    }
                }

                if (command.isExitWhenEndReached() && helper.hasReachedTopicEnd()) {
                    report(console, config, helper, consumer, grep);
                    return;
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void report(ConsoleHelper console, ConsumerConfigHelper config, ConsumerHelper helper, Consumer<Object, Object> consumer, String grep) {
        console.log("\n------------------------------Summary------------------------------");
        console.logf("# topic: %s, partition: %s, offset: %s, grep: %s",
                config.getTopic(), config.getPartitionOrNull(), config.geOffsetOrNull(), grep);
        console.logf(" from_epoch: %d, backward_duration: %d\n",
                config.getFromEpochOrNull(), config.getBackwardDurationOrNull());
        console.logf("# total limit: %d, grep limit: %d\n", config.getLimit(), config.getGrepLimit());
        console.logf("# total visited: %d: grep hit: %d\n", helper.getVisitCount(), helper.getGrepHit());
        console.log("# earliest visited offsets:" + helper.getEarliestVisitedOffsets());
        console.log("# latest visited offsets:" + helper.getLatestVisitedOffsets());
        console.log("# end offsets of the topic:" + consumer.endOffsets(consumer.assignment()));
    }

    private static String valueStringToLog(Object value) {
        return valueStringToLog(value, null);
    }

    private static String valueStringToLog(Object value, Set<String> valueFields) {
        if (value == null) {
            return null;
        }

        if (!(value instanceof GenericRecord record) || ObjectUtils.isEmpty(valueFields)) {
            return value.toString();
        }

        return GenericRecordHelper.selectFields(record, valueFields).toString();
    }
}

// todo
// feature: counts, size, filter, print offset/partition
// add --consumer-config support