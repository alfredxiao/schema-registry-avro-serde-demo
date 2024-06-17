package xiaoyf.demo.schemaregistry.consumer.uselatestversion;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import xiaoyf.demo.schemaregistry.model.LatestTest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.schemaregistry.helper.Constants.LATEST_TEST_TOPIC;
import static xiaoyf.demo.schemaregistry.helper.Constants.SCHEMA_REGISTRY_URL;

public class LatestTestSpecificConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, LatestTestSpecificConsumer.class.getName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final Consumer<String, LatestTest> consumer = new KafkaConsumer<String, LatestTest>(props);
		consumer.subscribe(Collections.singletonList(LATEST_TEST_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, LatestTest> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, LatestTest> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
					System.out.println("Value of schema:" + record.value().getSchema());
				}
				consumer.commitSync();
			}
		} finally {
			consumer.close();
		}
	}
}

/*
When Consuming v1 object
- HTTP Request to Schema Registry
  GET /schemas/ids/1?fetchMaxId=false HTTP/1.1
- HTTP Response
  HTTP/1.1 200 OK
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 */

/*
When consuming v2 using `use.latest.version=false` (v2 already registered)
- HTTP Request
  GET /schemas/ids/2?fetchMaxId=false HTTP/1.1
- HTTP Response
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
 */

/*
When consuming v2 using `use.latest.version=true` (v2 already registered)
- GET /schemas/ids/2?fetchMaxId=false HTTP/1.1
- HTTP/1.1 200 OK
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
 */

/*
When consuming v1 using `use.latest.version=true` (both v1 & v2 registered)
- GET /schemas/ids/1?fetchMaxId=false HTTP/1.1
- HTTP/1.1 200 OK
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 */

/*
Conclusion
- consumer uses the schema id embedded in the actual bytes it is consuming to retrieve schema definition
 */