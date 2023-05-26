package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

public class FieldOrderMismatchDemo {

    final static String SCHEMA_ORDER1 = "{" +
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

    final static String SCHEMA_ORDER2 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"UserWithFieldOrder\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"name\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schemaOrder1 = new Parser().parse(SCHEMA_ORDER1);

        GenericRecord userOrder1 = new GenericData.Record(schemaOrder1);
        userOrder1.put("id", "001");
        userOrder1.put("name", "alfred");

        byte[] userOrder1Bytes = genericRecordToBytes(schemaOrder1, userOrder1);

        GenericRecord user1ReadViaSchemaOrder1 = bytesToGenericRecord(schemaOrder1, userOrder1Bytes);
        Logger.log("user1 read via schemaOrder1:" + user1ReadViaSchemaOrder1);

        Schema schemaOrder2 = new Parser().parse(SCHEMA_ORDER2);
        GenericRecord user1ReadViaSchemaOrder2 = bytesToGenericRecord(schemaOrder2, userOrder1Bytes);
        Logger.log("user1 read via schemaOrder2:" + user1ReadViaSchemaOrder2);
    }
}

/*
 ## user1 read via schemaOrder1:{"id": "001", "name": "alfred"}
 ## user1 read via schemaOrder2:{"name": "001", "id": "alfred"}

 Conclusion: field order MATTERS
 */