package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.extractGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.recordToBytes;

/**
 * FieldTypeMismatchDemo demonstrates decoding with type mismatch
 */
public class FieldTypeMismatchDemo {

    final static String SCHEMA_ORDER1 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"User\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"age\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    final static String SCHEMA_ORDER2 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"User\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"age\"," +
            "            \"type\" : \"long\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schemaOrder1 = new Parser().parse(SCHEMA_ORDER1);

        GenericRecord userOrder1 = new GenericData.Record(schemaOrder1);
        userOrder1.put("id", "XYZ");
        userOrder1.put("age", "0");

        byte[] bytes = recordToBytes(schemaOrder1, userOrder1);

        GenericRecord user1ReadViaSchemaOrder1 = extractGenericRecord(schemaOrder1, bytes);
        Logger.log("user1 read via schemaOrder1:" + user1ReadViaSchemaOrder1);
        logBytesHex(bytes);

        Schema schemaOrder2 = new Parser().parse(SCHEMA_ORDER2);
        GenericRecord user1ReadViaSchemaOrder2 = extractGenericRecord(schemaOrder2, bytes);
        Logger.log("user2 read via schemaOrder2:" + user1ReadViaSchemaOrder2);
        logBytesHex(recordToBytes(schemaOrder2, user1ReadViaSchemaOrder2));
    }
}

/*
 ## user1 read via schemaOrder1:{"id": "XYZ", "age": "0"}
 ## BYTES: 06 58 59 5A 02 30
 ## user2 read via schemaOrder2:{"id": "XYZ", "age": 1}
 ## BYTES: 06 58 59 5A 02

 !! NOTE the decoding process uses a different schema, it interprets as this
 02: zigzag maps to 1
 30: extra bytes that gets IGNORED!!

 Conclusion: it is very dangerous to decode bytes using a schema different from the one used for generating the bytes
 */