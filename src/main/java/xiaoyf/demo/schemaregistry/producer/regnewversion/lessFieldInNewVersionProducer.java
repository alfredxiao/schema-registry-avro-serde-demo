package xiaoyf.demo.schemaregistry.producer.regnewversion;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;
import static xiaoyf.demo.schemaregistry.helper.ProducerHelper.defaultProperties;
import static xiaoyf.demo.schemaregistry.producer.regnewversion.Constants.USER_REG_NEW_VERSION_TOPIC;

public class lessFieldInNewVersionProducer {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(defaultProperties());

        String key = "k2";

        final String SCHEMA = """
            {
                 "type": "record",
                 "name": "User",
                 "namespace": "xiaoyf.demo.schemaregistry.model",
                 "fields": [
                     {
                         "name" : "id",
                         "type" : "string"
                     }
                 ]
            }
        """;

        Schema schema = stringToSchema(SCHEMA);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", "011");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(USER_REG_NEW_VERSION_TOPIC, key, avroRecord);
        producer.send(record).get();
        producer.close();
    }
}

/*

** Interaction with Schema Registry

 # When 'auto.register.schema=true'
 ## First run (when the schema has NOT been registered)
 1. This registers a new version when this producer runs for the first time;
 2. It receives an id identifying an avro schema version;
 ## Runs after first one (when the schema version has been registered, or even there is a newer version registered)
 1. This does not register a new version;
 2. The same id is returned from schema registry.
 ## HTTP Interaction with Schema Registry
 1. Request
 POST /subjects/users-value/versions HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 200 OK
 {"id":1}

 # When 'auto.register.schema=false'
 ## When the schema has NOT yet been registered
 1. It fails to find the schema from schema registry
 ### HTTP Interaction
 1. Request
 POST /subjects/user-value?deleted=false HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 404 Not Found
 {"error_code":40401,"message":"Subject 'test1-value' not found."}
 ## When the schema HAS been already registered
 1. It does not register a new version, but queries for an id for a schema that it has in hand
 2. It receives version details including an id
 ### HTTP Interaction
 1. Request
 POST /subjects/users-value?deleted=false HTTP/1.1
 {"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}
 2. Response
 HTTP/1.1 200 OK
 {"subject":"users-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"xiaoyf.demo.schemaregistry.model\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"}

** Serializer
 1. writes a '0' then four bytes for the id value, e.g. '0 0 0 1' if id=1
 2. For GenericRecord producer, it creates GenericDatumWriter to do the bytes assembly
 3. GenericDatumWriter's logic
    for each field in schema (fields are ordered in schema)
      get source value from generic record by index of a field
      write the bytes for this field with this source value
    end for
*/