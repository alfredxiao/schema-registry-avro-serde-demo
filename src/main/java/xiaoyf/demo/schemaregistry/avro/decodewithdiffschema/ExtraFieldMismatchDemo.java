package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

/**
 * ExtraFieldMismatchDemo demonstrates decoding with a schema with extra filed will FAIL.
 */
public class ExtraFieldMismatchDemo {

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
            "        }," +
            "        {" +
            "            \"name\" : \"location\"," +
            "            \"default\" : \"World\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schemaOrder1 = new Parser().parse(SCHEMA_NAME1);

        GenericRecord userOrder1 = new GenericData.Record(schemaOrder1);
        userOrder1.put("id", "001");
        userOrder1.put("name", "alfred");

        byte[] userOrder1Bytes = genericRecordToBytes(schemaOrder1, userOrder1);

        GenericRecord user1ReadViaSchemaOrder1 = bytesToGenericRecord(schemaOrder1, userOrder1Bytes);
        Logger.log("user1 read via schemaOrder1:" + user1ReadViaSchemaOrder1);

        Schema schemaOrder2 = new Parser().parse(SCHEMA_NAME2);
        GenericRecord user1ReadViaSchemaOrder2 = bytesToGenericRecord(schemaOrder2, userOrder1Bytes);
        Logger.log("user1 read via schemaOrder2:" + user1ReadViaSchemaOrder2);
    }
}

/*
Exception in thread "main" java.io.EOFException
	at org.apache.avro.io.BinaryDecoder.ensureBounds(BinaryDecoder.java:542)
	at org.apache.avro.io.BinaryDecoder.readLong(BinaryDecoder.java:205)

	Conclusion: the decoding process uses a schema with an extra field that does not exist in the bytes, it hence throws
	            an exception EVEN IF the extra field is defined with DEFAULT value.
 */
