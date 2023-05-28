package xiaoyf.demo.schemaregistry.producer;


import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.Type1;
import xiaoyf.demo.schemaregistry.model.Type2;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.schemaregistry.helper.Constants.SCHEMA_REGISTRY_URL;

public class MultiTypeProducer {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
		props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

		final String timestamp = System.currentTimeMillis() + "";

		Type1 type1 = Type1.newBuilder()
				.setId("id1")
				.setName("ID1")
				.build();

		Type2 type2 = Type2.newBuilder()
				.setSize(20)
				.setDate("anydate")
				.setCity("melb")
				.build();

		ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("multitype", timestamp, type1);
		ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>("multitype", timestamp, type2);
		try {
			producer.send(record1).get();
			producer.send(record2).get();
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "multitype-consumer");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

		KafkaConsumer<String, Object> byteConsumer = new KafkaConsumer<>(consumerProps);
		byteConsumer.subscribe(Collections.singleton("multitype"));
		ConsumerRecords<String, Object> records = byteConsumer.poll(Duration.ofMillis(500));
		for (ConsumerRecord<String, Object> record : records) {
			Logger.log("Seen Record:" + record);

			byte[] bytes = (byte[]) record.value();
			logBytesHex(bytes);
		}
	}
}


/*
First, need to register the two subjects separately.
POST http://localhost:8081/subjects/multitype-1/versions
{
    "schema": "{\"type\": \"record\",\"name\": \"Type1\",\"namespace\": \"xiaoyf.demo.schemaregistry.model\",\"fields\": [{\"name\" : \"id\",\"type\" : \"string\"},{\"name\" : \"name\",\"type\" : \"string\"}]}",
    "schemaType": "AVRO"
}
POST http://localhost:8081/subjects/multitype-2/versions
{
    "schema": "{\"type\": \"record\",\"name\": \"Type2\",\"namespace\": \"xiaoyf.demo.schemaregistry.model\",\"fields\": [{\"name\" : \"size\",\"type\" : \"int\"},{\"name\" : \"date\",\"type\" : \"string\"},{\"name\" : \"city\",\"type\" : \"string\"}]}",
    "schemaType": "AVRO"
}

Then register the union type by
POST http://localhost:8081/subjects/multitype-value/versions
{
    "schema": "[\"xiaoyf.demo.schemaregistry.model.Type1\",\"xiaoyf.demo.schemaregistry.model.Type2\"]",
    "schemaType": "AVRO",
    "references" : [
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type1",
            "subject":  "multitype-1",
            "version": 1
        },
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type2",
            "subject":  "multitype-2",
            "version": 1
        }
    ]
}

The producer (auto.register.schemas=false,use.latest.version=true) then produces the two records produced into the topic, with below bytes
 ## BYTES: 00 00 00 00 04 00 06 69 64 31 06 49 44 31
 ## BYTES: 00 00 00 00 04 02 28 0E 61 6E 79 64 61 74 65 08 6D 65 6C 62
 -> 00 00 00 00 04 is magic number 00 plus schema id (04)
 -> 00 (after the first 04) means 0, which is the first type in the union type
 -> 02 (after the second 04) means 1, which is the second type in the union type

meaning the schema id used is 4, when GET http://localhost:8081/schemas/ids/4, it is
 {
    "schema": "[\"xiaoyf.demo.schemaregistry.model.Type1\",\"xiaoyf.demo.schemaregistry.model.Type2\"]",
    "references": [
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type1",
            "subject": "multitype-1",
            "version": 1
        },
        {
            "name": "xiaoyf.demo.schemaregistry.model.Type2",
            "subject": "multitype-2",
            "version": 1
        }
    ]
}
 */