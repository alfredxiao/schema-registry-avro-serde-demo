package xiaoyf.demo.schemaregistry.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class LogicalGenericConsumerTool {
    public static void main(String[] args) throws Exception {
        // create Options object
        Options options = new Options();
        options.addOption("b", true, "bootstrap server");
        options.addOption("t", true, "topic");
        options.addOption("k", true, "key deserializer");
        options.addOption("sr", true, "schema registry url");
        options.addOption("fb", false, "from beginning");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String bootStrapServers = cmd.getOptionValue("b");
        String topic = cmd.getOptionValue("t");
        String keyDeserializer = cmd.getOptionValue("k", StringDeserializer.class.getName());
        String schemaRegistryUrl = cmd.getOptionValue("sr");
        String fromBeginning = cmd.getOptionValue("fb", "false");

        System.out.println("bootstrap server: " + bootStrapServers);
        System.out.println("topic: " + topic);
        System.out.println("key deserializer: " + keyDeserializer);

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning.equals("true") ? "earliest" : "latest");
        props.put("avro.use.logical.type.converters", true);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
