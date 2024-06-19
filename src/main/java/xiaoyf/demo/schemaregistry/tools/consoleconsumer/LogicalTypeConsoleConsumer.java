package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

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
        helper.seekToExpectedOffset();

        try {
            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<Object, Object> record : records) {
                    Object key = record.key();
                    Object value = record.value();

                    String keyString = valueStringToLog(key);
                    String valueString = valueStringToLog(value, config.valueFields());

                    if (StringUtils.isEmpty(command.getGrep())
                            || match(keyString, command.getGrep())
                            || match(valueString, command.getGrep())) {

                        helper.increaseGrepHit();
                        console.printf("p=%d,o=%d,t=%d,k=%s,v=%s\n",
                                record.partition(), record.offset(), record.timestamp(), keyString, valueString);
                    }

                    helper.visitRecord(record);

                    if (helper.hasReachedGrepOrTotalLimit()) {
                        console.printf("# total limit %d or grep limit %d reached, exit", config.getLimit(), config.getGrepLimit());
                        return;
                    }
                }

                if (command.isExitWhenEndReached() && helper.hasReachedTopicEnd()) {
                    console.println("Topic offset reached, exit");
                    return;
                }
            }
        } finally {
            consumer.close();
        }
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