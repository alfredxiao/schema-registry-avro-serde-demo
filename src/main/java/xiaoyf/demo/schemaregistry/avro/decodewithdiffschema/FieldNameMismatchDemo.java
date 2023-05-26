package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

/**
 * FieldNameMismatchDemo demonstrates decoding using a schema with different field name.
 */
public class FieldNameMismatchDemo {

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

    final static String SCHEMA1 = schemaString("User", "id", "name");

    final static String SCHEMA2 = schemaString("Person", "title", "occupation");

    public static void main(String[] args) throws Exception {
        Schema schema1 = new Parser().parse(SCHEMA1);

        GenericRecord user = new GenericData.Record(schema1);
        user.put("id", "001");
        user.put("name", "alfred");
        Logger.log("user created: " + user);

        byte[] userBytes = genericRecordToBytes(schema1, user);

        Schema schema2 = new Parser().parse(SCHEMA2);
        GenericRecord userRead = bytesToGenericRecord(schema2, userBytes);
        Logger.log("user read from bytes:" + userRead);
    }
}
