package xiaoyf.demo.schemaregistry.producer;


import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.Constants.BOOTSTRAP_SERVERS;
import static xiaoyf.demo.schemaregistry.helper.Constants.LATEST_TEST_TOPIC;
import static xiaoyf.demo.schemaregistry.helper.Constants.SCHEMA_REGISTRY_URL;

public class LatestTestProducer {
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

		String avsc = new String(Files.readAllBytes(Paths.get("./src/main/avro/LatestTest.avsc1")));

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(avsc);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("id", "01-" + timestamp);
		avroRecord.put("name", "N1-" + timestamp);
		//avroRecord.put("location", "L1-" + timestamp);

		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(LATEST_TEST_TOPIC, timestamp, avroRecord);
		try {
			producer.send(record);
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}

/* # When `auto.register.schemas=true`, First time & Each time afterwards
- HTTP Request to Schema Registry:
	POST /subjects/latest-test-value/versions HTTP/1.1
	{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP Response:
	HTTP/1.1 200 OK
	{"id":1}
*/

/* # When `auto.register.schemas=false` (and `use.latest.version=false` by default), there exists the schema in registry
- HTTP Request to Schema Registry:
	POST /subjects/latest-test-value?deleted=false HTTP/1.1
	{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP Response:
	HTTP/1.1 200 OK
	{"subject":"latest-test-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
*/

/* # When `auto.register.schemas=true`, Publish v2 object which registers v2 schema
POST /subjects/latest-test-value/versions HTTP/1.1
{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
HTTP/1.1 200 OK
{"id":2}
 */

/* # When `auto.register.schemas=false` (and `use.latest.version=false` by default), publishing v1 object while v2 already registered
POST /subjects/latest-test-value?deleted=false HTTP/1.1
{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
HTTP/1.1 200 OK
{"subject":"latest-test-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
*/

/* # When `auto.register.schemas=false` (and `use.latest.version=true`), publishing v1 object while v2 already registered
GET /subjects/latest-test-value/versions/latest HTTP/1.1
{"subject":"latest-test-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
POST /subjects/latest-test-value?deleted=false HTTP/1.1
{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
{"subject":"latest-test-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}

HOWEVER!!
org.apache.kafka.common.errors.SerializationException: Error serializing Avro message
Caused by: java.lang.ArrayIndexOutOfBoundsException: Index 2 out of bounds for length 2
	at org.apache.avro.generic.GenericData$Record.get(GenericData.java:263)
	at org.apache.avro.generic.GenericData.getField(GenericData.java:827)
	at org.apache.avro.generic.GenericData.getField(GenericData.java:846)
	at org.apache.avro.generic.GenericDatumWriter.writeField(GenericDatumWriter.java:219)
	at org.apache.avro.generic.GenericDatumWriter.writeRecord(GenericDatumWriter.java:210)
	at org.apache.avro.generic.GenericDatumWriter.writeWithoutConversion(GenericDatumWriter.java:131)
	at org.apache.avro.generic.GenericDatumWriter.write(GenericDatumWriter.java:83)
	at org.apache.avro.generic.GenericDatumWriter.write(GenericDatumWriter.java:73)
	at io.confluent.kafka.serializers.AbstractKafkaAvroSerializer.serializeImpl(AbstractKafkaAvroSerializer.java:118)
	at io.confluent.kafka.serializers.KafkaAvroSerializer.serialize(KafkaAvroSerializer.java:59)
	at org.apache.kafka.common.serialization.Serializer.serialize(Serializer.java:62)
	at org.apache.kafka.clients.producer.KafkaProducer.doSend(KafkaProducer.java:926)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:886)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:774)
	at xiaoyf.demo.avrokafka.partyv1.producer.LatestTestProducer.main(LatestTestProducer.java:46)

*/

/* # When `auto.register.schemas=true` , publishing v1 object while v2 already registered
POST /subjects/latest-test-value/versions HTTP/1.1
{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
HTTP/1.1 200 OK
{"id":1}

Conclusion:
- 'use.latest.version' is only applies when 'auto.register.schemas' is set to false.
-
 */