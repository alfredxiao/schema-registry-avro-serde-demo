package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class CommandLineHelper {
    public static final String _CONSUMER_CONFIG = "c";
    public static final String _BOOTSTRAP_SERVER = "b";
    public static final String _TOPIC = "t";
    public static final String _KEY_DESERIALIZER = "k";
    public static final String _VALUE_DESERIALIZER = "v";
    public static final String _SCHEMA_REGISTRY = "s";
    public static final String _PARTITION = "p";
    public static final String _OFFSET = "o";
    public static final String _FROM_EPOCH = "f";
    public static final String _BACKWARD_DURATION = "bd";
    public static final String _EXIT_WHEN_END_REACHED = "e";
    public static final String _VALUE_FIELDS = "vf";
    public static final String _GREP = "g";
    public static final String _GREP_LIMIT = "gl";
    public static final String _LIMIT = "l";


    final CommandLine cmd;

    CommandLineHelper(String[] args) throws ParseException {
        this.cmd = commandLine(args);
    }

    private Options options() {
        Options opts = new Options();
        opts.addOption(_CONSUMER_CONFIG, true, "config file for consumer");
        opts.addOption(_BOOTSTRAP_SERVER, true, "bootstrap server");
        opts.addRequiredOption(_TOPIC, "--topic", true, "topic");
        opts.addOption(_KEY_DESERIALIZER, true, "key deserializer");
        opts.addOption(_VALUE_DESERIALIZER, true, "value deserializer");
        opts.addOption(_SCHEMA_REGISTRY, true, "schema registry url");
        opts.addOption(_PARTITION, true, "from partition, 'all' means all partitions");
        opts.addOption(_OFFSET, true, "from offset, 'beginning' means from beginning，'end' means from end");
        opts.addOption(_FROM_EPOCH, true, "from epoch timestamp");
        opts.addOption(_BACKWARD_DURATION, true, "goes back specified duration, e.g. 1 hour (PT1H), PT2M PT1D");
        opts.addOption(_EXIT_WHEN_END_REACHED, false, "exit when reaching end offset");
        opts.addOption(_VALUE_FIELDS, true, "selected fields are display (only when value is avro)");
        opts.addOption(_GREP, true, "filter records like grep");
        opts.addOption(_GREP_LIMIT, true, "exit when specified number of greps found");
        opts.addOption(_LIMIT, true, "stops at number of records seen");

        return opts;
    }

    private CommandLine commandLine(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options(), args);
    }

    public Properties getConsumerConfigFromFile() throws IOException {
        final String paramValue = cmd.getOptionValue(_CONSUMER_CONFIG);

        if (StringUtils.isBlank(paramValue)) {
            return new Properties();
        }

        Properties props = new Properties();
        props.load(new FileInputStream(paramValue));

        return props;
    }

    public String getOptionOrNull(String optName) {
        String value = cmd.getOptionValue(optName);

        return StringUtils.isEmpty(value) ? null : value;
    }

    private void putIfAvailable(Properties config, String key, String value) {
        if (!StringUtils.isEmpty(value)) {
            config.put(key, value);
        }
    }
    
    public Properties getConsumerConfigFromCommandLine() {
        
        Properties config = new Properties();

        putIfAvailable(config, BOOTSTRAP_SERVERS_CONFIG, getOptionOrNull(_BOOTSTRAP_SERVER));
        putIfAvailable(config, KEY_DESERIALIZER_CLASS_CONFIG, getOptionOrNull(_KEY_DESERIALIZER));
        putIfAvailable(config, VALUE_DESERIALIZER_CLASS_CONFIG, getOptionOrNull(_VALUE_DESERIALIZER));
        putIfAvailable(config, "schema.registry.url", getOptionOrNull(_SCHEMA_REGISTRY));

        return config;
    }

    public boolean isExitWhenEndReached() {
        return cmd.hasOption(_EXIT_WHEN_END_REACHED);
    }
}
