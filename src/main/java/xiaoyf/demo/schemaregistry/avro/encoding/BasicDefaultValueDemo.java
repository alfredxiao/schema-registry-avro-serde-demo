package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

public class BasicDefaultValueDemo {

    final static String SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"User\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"id\"," +
            "            \"type\" : \"string\"," +
            "            \"default\" : \"NO_ID\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"name\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schema = stringToSchema(SCHEMA);

        GenericRecord user = new GenericData.Record(schema);
        user.put("id", "001");
        // user.put("id", schema.getField("id").defaultVal());
        user.put("name", "alfred");

        byte[] bytes = genericRecordToBytes(schema, user);
        Utilities.logBytesHex(bytes);

        GenericRecord read = bytesToGenericRecord(schema, bytes);
        Logger.log("user read from bytes:" + read);
    }
}

/*
 ## BYTES: 06 30 30 31 0C 61 6C 66 72 65 64
 ## user read from bytes:{"id": "001", "name": "alfred"}

 Conclusion: Default value does not get populated automatically. One has to set field value explicitly.
 */