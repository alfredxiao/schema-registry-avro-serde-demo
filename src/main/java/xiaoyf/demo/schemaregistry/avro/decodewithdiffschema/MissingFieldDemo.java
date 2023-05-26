package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

/**
 * MissingFieldDemo demonstrates decoding with a schema with fewer fields defined
 */
public class MissingFieldDemo {

    final static String SCHEMA_NAME1 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"UserWithFieldOrder\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"name\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"location\"," +
            "            \"default\" : \"World\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    final static String SCHEMA_NAME2 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"UserWithFieldOrder\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"name\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schemaOrder1 = new Parser().parse(SCHEMA_NAME1);

        GenericRecord userOrder1 = new GenericData.Record(schemaOrder1);
        userOrder1.put("id", "001");
        userOrder1.put("name", "alfred");
        userOrder1.put("location", "XXX");

        byte[] bytes = genericRecordToBytes(schemaOrder1, userOrder1);
        Utilities.logBytesHex(bytes);

        GenericRecord user1ReadViaSchemaOrder1 = bytesToGenericRecord(schemaOrder1, bytes);
        Logger.log("user1 read via schemaOrder1:" + user1ReadViaSchemaOrder1);

        Schema schemaOrder2 = new Parser().parse(SCHEMA_NAME2);
        GenericRecord user1ReadViaSchemaOrder2 = bytesToGenericRecord(schemaOrder2, bytes);
        Logger.log("user2 read via schemaOrder2:" + user1ReadViaSchemaOrder2);
    }
}

/*
 ## BYTES: 06 30 30 31 0C 61 6C 66 72 65 64 06 58 58 58
 ## user1 read via schemaOrder1:{"id": "001", "name": "alfred", "location": "XXX"}
 ## user2 read via schemaOrder2:{"id": "001", "name": "alfred"}

 user2 decoding process IGNORES extra bytes '06 58 58 58'
 */
