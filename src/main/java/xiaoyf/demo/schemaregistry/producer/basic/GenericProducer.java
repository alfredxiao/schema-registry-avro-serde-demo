package xiaoyf.demo.schemaregistry.producer.basic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.helper.Constants.USER_TOPIC;
import static xiaoyf.demo.schemaregistry.helper.ProducerHelper.defaultProperties;

public class GenericProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(defaultProperties());

        String key = "k1";

        Schema schema = asSchema("basic/user.avsc");
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", "001");
        avroRecord.put("name", "alfred");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(USER_TOPIC, key, avroRecord);
        producer.send(record).get();
        producer.flush();
        producer.close();
    }
}

/*

** Interaction with Schema Registry

When 'auto.register.schema=true'
 - First run (when the schema has NOT been registered)
 1. This registers a new version when this producer runs for the first time by POST
 2. In the response it receives an id identifying an avro schema version;
 3. The ID becomes the first part of the bytes of the record written to Kafka topic.

 - Runs after this First run (where the schema version has been registered, or even there is a even newer version registered)
 1. This does not register a new version, but it does the same POST anyway;
 2. The very same id is returned from schema registry.

 * HTTP Interaction with Schema Registry
 1. Request
 POST /subjects/users-value/versions HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 200 OK
 {"id":1}

When 'auto.register.schema=false'
 - When the schema has NOT yet been registered
 1. It fails to find the schema from schema registry;
 2. As a result, it cannot send record to Kafka topic;
 * HTTP Interaction
 1. Request
 POST /subjects/user-value?deleted=false HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 404 Not Found
 {"error_code":40401,"message":"Subject 'test1-value' not found."}

 - When the schema HAS been already registered
 1. It does not register a new version, but POST a schema anyway;
 2. It receives version details including an id;
 3. Same flow as above, which is it using the id to encode and send record out;
 * HTTP Interaction
 1. Request
 POST /subjects/users-value?deleted=false HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 200 OK
 {"subject":"users-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}

** Serializer
 1. writes a '0' then four bytes for the id value, e.g. '0 0 0 1' if id=1
    which means the first 5 bytes of a record has the id of the schema of the record
 2. For GenericRecord producer, it creates GenericDatumWriter to do the bytes assembly
 3. GenericDatumWriter's logic
    for each field in schema (fields are ordered in schema)
      get source value from generic record by index of a field
      write the bytes for this field with this source value
    end
*/