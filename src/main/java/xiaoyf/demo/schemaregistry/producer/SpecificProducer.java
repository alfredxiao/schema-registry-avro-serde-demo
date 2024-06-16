package xiaoyf.demo.schemaregistry.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import xiaoyf.demo.schemaregistry.model.User;

import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.schemaregistry.helper.Constants.SCHEMA_REGISTRY_URL;
import static xiaoyf.demo.schemaregistry.helper.Constants.USER_TOPIC;

public class SpecificProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", true);
        KafkaProducer<String, User> producer = new KafkaProducer<>(props);

        String key = "k2";

        User user = new User(1, "Alfred");

        ProducerRecord<String, User> record = new ProducerRecord<>(USER_TOPIC, key, user);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}

/*
** Interaction with Schema Registry is the same as 'GenericProducer'

** Serializer
 1. writes a '0' then four bytes for the id value, e.g. '0 0 0 1' if id=1
    which means the first 5 bytes of a record has the id of the schema of the record
 2. For SpecificRecord producer, it creates SpecificDatumWriter to do the bytes assembly
 3. SpecificDatumWriter's logic
    for each field in schema (fields are ordered in schema)
      get source value from generic record by index of a field
      apply logical type conversion if necessary (e.g. Instant to long)
      write the bytes for this field with this source value
    end for
 */