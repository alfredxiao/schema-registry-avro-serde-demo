package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.extractGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.recordToBytes;

public class BasicEncodeDecodeDemo {

    final static String SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"User\"," +
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
        Schema schema = new Parser().parse(SCHEMA);

        GenericRecord user = new GenericData.Record(schema);
        user.put("id", "001");
        user.put("name", "alfred");

        byte[] bytes = recordToBytes(schema, user);
        Utilities.logBytesHex(bytes);

        GenericRecord read = extractGenericRecord(schema, bytes);
        Logger.log("user read from bytes:" + read);
    }
}

/*
 ## BYTES: 06 30 30 31 0C 61 6C 66 72 65 64
 ## user read from bytes:{"id": "001", "name": "alfred"}
 06 -> zigzag algorithm maps to 3 (as length of string)
 30 30 31 -> "001"
 0C -> zigzag maps to 6 (as length of second string)
 61 6C 66 72 65 64 -> "alfred"
 */