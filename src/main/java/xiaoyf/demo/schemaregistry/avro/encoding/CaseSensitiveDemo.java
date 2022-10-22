package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.extractGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.recordToBytes;

public class CaseSensitiveDemo {

    static String schemaString(
            final String name,
            final String field1Name,
            final String field2Name
    ) {
        return "{" +
                "    \"type\": \"record\"," +
                "    \"name\": \"" + name + "\"," +
                "    \"fields\": [" +
                "        {" +
                "            \"name\" : \"" + field1Name + "\"," +
                "            \"type\" : \"string\"" +
                "        }," +
                "        {" +
                "            \"name\" : \"" + field2Name + "\"," +
                "            \"type\" : \"string\"" +
                "        }" +
                "    ]" +
                "}";
    }

    final static String SCHEMA = schemaString("User", "id", "ID");

    public static void main(String[] args) throws Exception {
        Schema schema = new Parser().parse(SCHEMA);

        GenericRecord user = new GenericData.Record(schema);
        user.put("id", "AB");
        user.put("ID", "XY");
        Logger.log("user created: " + user);

        byte[] bytes = recordToBytes(schema, user);


        GenericRecord read = extractGenericRecord(schema, bytes);
        Logger.log("user read from bytes:" + read);
    }
}

/* NOTE
 - field names are case-sensitive
 */
