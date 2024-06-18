package xiaoyf.demo.schemaregistry.producer.uselatestversion;


import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.ProducerHelper;

import java.util.Properties;

import static xiaoyf.demo.schemaregistry.helper.Constants.LATEST_TEST_TOPIC;

/*
 Purpose: Demonstrates how 'auto.register.schemas' and 'use.latest.version' relate.

 Conclusion:
  - When 'auto.register.schemas'=true, producer always produce with the schema associated with the object being
    produced, meaning 'use.latest.version' is not relevant in such case
  - When 'auto.register.schemas'=false, 'use.latest.version' is relevant
    = If 'use.latest.version'=false, producer then expects that the schema associated with the object being produced
      is already registered, otherwise -> fail!
    = If 'use.latest.version'=true, producer fetches the latest schema version
      # If latest schema=the schema associated with the object being produced, no problem
      # If latest schema > the schema associated with the object being produced, no problem
      # If latest schema < the schema associated with the object being produced, fail!
      # Note: '>' and '<' is not simply who is larger, but essentially it is whether the producer is able to generate
        bytes/object conforming to target version (as recipe) while sourcing from an object made out of a different
        recipe. For example, User v3 has all v2 fields, then it is a success, but v1 lacking one field in v2 (even
        though it has default value), would still fail
  - When setting use.latest.version=true, it means
     1. You have control over schema registration while disallow applications to register them
     2. You expect all records in the topic has to conform to the 'latest' version
     3. If producer fail to produce conforming records, let them fail
     4. You're kind of 'locking' a topic to a particular schema version
       - however, this is enforced by producers, not by broker,
- 'use.latest.version=true' is dangerous when there are differences between local version and the latest version
- 'use.latest.version=true' is necessary when using union types at top level so that we can send objects of multiple
  types to the same topic (as union)

 */
public class UseLatestVersionProducer {

    public static void main(String[] args) throws Exception {
        Properties props = ProducerHelper.defaultProperties();
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);

        // at this point, v1 and v2 are already registered and v2 is 'latest'
        // if not, run pre-register-schemas.sh to register them

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        GenericRecord user = null;

        final String versionToProduce = "v1"; // v1 or v3

        if ("v1".equals(versionToProduce)) {

            GenericRecord avroRecord = new GenericData.Record(Utilities.asSchema("uselatestversion/user_v1.avsc"));
            avroRecord.put("id", "01");
            avroRecord.put("name", "alfred");
        }

        if ("v3".equals(versionToProduce)) {
            GenericRecord avroRecord = new GenericData.Record(Utilities.asSchema("uselatestversion/user_v3.avsc"));
            avroRecord.put("id", "03");
            avroRecord.put("name", "alfred");
            avroRecord.put("age", 25);
        }

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(LATEST_TEST_TOPIC, user);
        try {
            producer.send(record).get();
        } catch (SerializationException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}

/*
When `auto.register.schemas=true`, this happens for the first time & each time afterwards
- HTTP Request to Schema Registry:
	POST /subjects/latest-test-value/versions HTTP/1.1
	{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP Response:
	HTTP/1.1 200 OK
	{"id":1}
*/

/*
When `auto.register.schemas=false` (and `use.latest.version=false` by default), there has to preexists the same schema in registry
- HTTP Request to Schema Registry:
	POST /subjects/latest-test-value?deleted=false HTTP/1.1
	{"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP Response:
	HTTP/1.1 200 OK
	{"subject":"latest-test-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
*/

/*
When `auto.register.schemas=true`, Publish v2 object which registers v2 schema
- POST /subjects/latest-test-value/versions HTTP/1.1
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
- HTTP/1.1 200 OK
  {"id":2}
 */

/*
When `auto.register.schemas=false` (and `use.latest.version=false` by default), we publish a v1 object while v2 already registered
- POST /subjects/latest-test-value?deleted=false HTTP/1.1
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP/1.1 200 OK
  {"subject":"latest-test-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}

In this case, v1 schema id is returned, producer can then publish v1 object with this v1 id.
*/

/*
When `auto.register.schemas=false` (and `use.latest.version=true`), we publish a v1 object while v2 already registered
- GET /subjects/latest-test-value/versions/latest HTTP/1.1
  {"subject":"latest-test-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}

  = this means, it asks for the latest version first
- POST /subjects/latest-test-value?deleted=false HTTP/1.1
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
  {"subject":"latest-test-value","version":2,"id":2,"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}
 = then it double verifies with the version that it is going to publish objects with (since we say use.latest.version=true)

HOWEVER, it is NOT ABLE to assembly the record bytes for the record, because
- it has a v1 object (not having the field required in v2)
- it uses v2 schema as assembly recipe
org.apache.kafka.common.errors.SerializationException: Error serializing Avro message
	at io.confluent.kafka.serializers.AbstractKafkaAvroSerializer.serializeImpl(AbstractKafkaAvroSerializer.java:163)
	at io.confluent.kafka.serializers.KafkaAvroSerializer.serialize(KafkaAvroSerializer.java:67)
	at org.apache.kafka.clients.producer.KafkaProducer.doSend(KafkaProducer.java:1015)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:962)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:847)
	at xiaoyf.demo.schemaregistry.producer.LatestTestProducer.main(LatestTestProducer.java:48)
Caused by: java.lang.ArrayIndexOutOfBoundsException: Index 2 out of bounds for length 2
	at org.apache.avro.generic.GenericData$Record.get(GenericData.java:275)
	at org.apache.avro.generic.GenericData.getField(GenericData.java:846)
	at org.apache.avro.generic.GenericData.getField(GenericData.java:865)
	at org.apache.avro.generic.GenericDatumWriter.writeField(GenericDatumWriter.java:243)
	at org.apache.avro.generic.GenericDatumWriter.writeRecord(GenericDatumWriter.java:234)
	at org.apache.avro.generic.GenericDatumWriter.writeWithoutConversion(GenericDatumWriter.java:145)
	at org.apache.avro.generic.GenericDatumWriter.write(GenericDatumWriter.java:95)
	at org.apache.avro.generic.GenericDatumWriter.write(GenericDatumWriter.java:82)
	at io.confluent.kafka.serializers.AbstractKafkaAvroSerializer.writeDatum(AbstractKafkaAvroSerializer.java:181)
	at io.confluent.kafka.serializers.AbstractKafkaAvroSerializer.serializeImpl(AbstractKafkaAvroSerializer.java:151)
	... 5 more

Also SpecificRecord has the same behaviour in this case
*/

/*
When `auto.register.schemas=true` , publishing v1 object while v2 already registered
- POST /subjects/latest-test-value/versions HTTP/1.1
  {"schema":"{\"type\":\"record\",\"name\":\"LatestTest\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
- HTTP/1.1 200 OK
  {"id":1}
 */