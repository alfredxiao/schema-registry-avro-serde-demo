package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;


public class ConsumerConfigHelper {
    public static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY = "http://localhost:8081";
    public static final String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
    //  avro uses io.confluent.kafka.serializers.KafkaAvroDeserializer.class
    public static final String DEFAULT_VALUE_DESERIALIZER = StringDeserializer.class.getName();
    public static final String ALL_PARTITIONS = "all";
    public static final int ALL_PARTITION = -1;
    public static final String FROM_BEGINNING = "beginning";
    public static final int FROM_BEGINNING_OFFSET = -1;
    public static final String FROM_END = "end";
    public static final int FROM_END_OFFSET = -2;
    public static final String UNLIMITED = "-1";

    private final CommandLineHelper command;

    public ConsumerConfigHelper(CommandLineHelper command) {
        this.command = command;
    }

    public Properties getConsumerConfig() throws IOException {
        Properties config = defaultConsumerConfig();
        Properties fileConfig = command.getConsumerConfigFromFile();
        Properties commandLineConfig = command.getConsumerConfigFromCommandLine();

        config.putAll(fileConfig);
        config.putAll(commandLineConfig);

        return config;
    }

    private Properties defaultConsumerConfig() {
        Properties config = new Properties();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
        config.put("schema.registry.url", DEFAULT_SCHEMA_REGISTRY);
        config.put("avro.use.logical.type.converters", true);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return config;
    }

    public Set<String> valueFields() {
        Set<String> fields = new HashSet<>();

        String valueFieldsArg = command.getValueFields();
        if (!valueFieldsArg.isBlank()) {
            fields = Arrays.stream(valueFieldsArg.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet());
        }

        return fields;
    }

    public String getTopic() {
        return command.getTopic();
    }

    public int getFromOffset() {
        String offset = command.getFromOffset(FROM_BEGINNING);
        switch (offset) {
            case FROM_BEGINNING:
                return FROM_BEGINNING_OFFSET;
            case FROM_END:
                return FROM_END_OFFSET;
            default:
                return Integer.parseInt(offset);
        }
    }

    public int getPartition() {
        String partition = command.getPartition(ALL_PARTITIONS);

        if (ALL_PARTITIONS.equals(partition)) {
            return ALL_PARTITION;
        }

        return Integer.parseInt(partition);
    }

    public int getLimit() {
        return Integer.parseInt(command.getLimit(UNLIMITED));
    }

    public int getGrepLimit() {
        return Integer.parseInt(command.getGrepLimit(UNLIMITED));
    }
}
