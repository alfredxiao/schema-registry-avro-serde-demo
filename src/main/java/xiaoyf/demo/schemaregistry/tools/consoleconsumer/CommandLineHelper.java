package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class CommandLineHelper {
    public static final String CONSUMER_CONFIG = "c";
    public static final String BOOTSTRAP_SERVER = "b";
    public static final String TOPIC = "t";
    public static final String KEY_DESERIALIZER = "k";
    public static final String VALUE_DESERIALIZER = "v";
    public static final String SCHEMA_REGISTRY = "s";
    public static final String PARTITION = "p";
    public static final String OFFSET = "o";
    public static final String EXIT_WHEN_END_REACHED = "e";
    public static final String VALUE_FIELDS = "vf";
    public static final String GREP = "g";
    public static final String GREP_FIRST = "gf";
    public static final String GREP_LAST = "gl";
    public static final String FOLLOW = "f";
    public static final String LIMIT = "l";


    final CommandLine cmd;

    CommandLineHelper(String[] args) throws ParseException {
        this.cmd = commandLine(args);
    }

    private Options options() {
        Options opts = new Options();
        opts.addOption(CONSUMER_CONFIG, true, "config file for consumer");
        opts.addOption(BOOTSTRAP_SERVER, true, "bootstrap server");
        opts.addOption(TOPIC, true, "topic");
        opts.addOption(KEY_DESERIALIZER, true, "key deserializer");
        opts.addOption(VALUE_DESERIALIZER, true, "value deserializer"); // io.confluent.kafka.serializers.KafkaAvroDeserializer.class
        opts.addOption(SCHEMA_REGISTRY, true, "schema registry url");
        opts.addOption(PARTITION, true, "from partition, 'all' means all partitions");
        opts.addOption(OFFSET, true, "from offset, 'beginning' means from beginning，'end' means from end");
        opts.addOption(EXIT_WHEN_END_REACHED, false, "exit when reaching end offset");
        opts.addOption(VALUE_FIELDS, true, "selected fields are display (only when value is avro)");
        opts.addOption(GREP, true, "filter records like grep");
        opts.addOption(FOLLOW, false, "don't exit, keep following");
        opts.addOption(LIMIT, true, "stops at number of records seen");

        return opts;
    }

    private CommandLine commandLine(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options(), args);
    }

    public String getTopic() {
        return cmd.getOptionValue(TOPIC);
    }

    public String getPartition(String def) {
        return cmd.getOptionValue(PARTITION, def);
    }

    public Properties getConsumerConfigFromFile() throws IOException {
        final String paramValue = cmd.getOptionValue(CONSUMER_CONFIG);

        if (StringUtils.isBlank(paramValue)) {
            return new Properties();
        }

        Properties props = new Properties();
        props.load(new FileInputStream(paramValue));

        return props;
    }

    public String getLimit(String def) {
        return cmd.getOptionValue(LIMIT, def);
    }

    public String getValueFields() {
        return cmd.getOptionValue(VALUE_FIELDS, "");
    }

    public String getFromOffset(String def) {
        return cmd.getOptionValue(OFFSET, def);
    }

    private void putIfParameterIsNotNull(Properties config, String key, String value) {
        if (value != null) {
            config.put(key, value);
        }
    }
    
    public Properties getConsumerConfigFromCommandLine() {
        
        Properties config = new Properties();

        putIfParameterIsNotNull(config, BOOTSTRAP_SERVERS_CONFIG, cmd.getOptionValue(BOOTSTRAP_SERVER));
        putIfParameterIsNotNull(config, KEY_DESERIALIZER_CLASS_CONFIG, cmd.getOptionValue(KEY_DESERIALIZER));
        putIfParameterIsNotNull(config, VALUE_DESERIALIZER_CLASS_CONFIG, cmd.getOptionValue(VALUE_DESERIALIZER));
        putIfParameterIsNotNull(config, "schema.registry.url", cmd.getOptionValue(SCHEMA_REGISTRY));

        return config;
    }
}
