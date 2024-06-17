package xiaoyf.demo.schemaregistry.consumer.basic;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.schemaregistry.helper.Constants.SCHEMA_REGISTRY_URL;
import static xiaoyf.demo.schemaregistry.helper.Constants.USER_TOPIC;

public class GenericConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GenericConsumer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Collections.singletonList(USER_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}

/*
GET /schemas/ids/1?fetchMaxId=false HTTP/1.1
HTTP/1.1 200 OK
{"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}

GenericDatumReader<>(writerSchema, readerSchema)
 writerSchema - used by writer to produce the bytes, is actual schema associated with the bytes
 readerSchema - used by reader to generate a record from bytes, is expected schema associated with record,
                in the case of generic consumer because consumer does not hold an expected schema at all, it is
                the same as writerSchema

Algorithm to generate a record:
  for each field in readerSchema
    read value from bytes according to field definition
  end for

 */