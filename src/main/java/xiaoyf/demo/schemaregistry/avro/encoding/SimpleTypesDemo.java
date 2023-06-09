package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

public class SimpleTypesDemo {

    final static String SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"Demo\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"f1\"," +
            "            \"type\" : \"int\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"f2\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"f3\"," +
            "            \"type\" : \"long\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"f4\"," +
            "            \"type\" : " + "{ \"type\": \"enum\"," +
            "                 \"name\": \"Suit\"," +
            "                 \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]" +
            "            }"+
            "        }," +
            "        {" +
            "            \"name\" : \"f5\"," +
            "            \"type\" : [\"null\", \"string\"]" +
            "        }," +
            "        {" +
            "            \"name\" : \"f6\"," +
            "            \"type\" : [\"null\", \"string\"]" +
            "        }," +
            "        {" +
            "            \"name\" : \"f7\"," +
            "            \"type\" : \"string\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schema = stringToSchema(SCHEMA);

        GenericRecord record = new GenericData.Record(schema);
        record.put("f1", 1);     // 02       which is the first byte in output bytes
        record.put("f2", "XY");  // 04 58 59 (04 means 2, which is length of the string)
        record.put("f3", 2L);    // 04       which means 2 here

        record.put("f4", new GenericData.EnumSymbol(SchemaBuilder.enumeration("Suit").symbols("SPADES", "HEARTS", "DIAMONDS", "CLUBS"), "CLUBS"));
                                 // 06             which means 3 (index 3, which is the fourth element)
        record.put("f5", null);  // 00             (0 as index in the union types)
        record.put("f6", "XYZ"); // 02 06 58 59 5A (02 means 1, so 1 is index in the union types; 06 means 3, string length is 3)
        record.put("f7", "");    // 00 -> zigzag 0 (string length is 0)

        // above used zigzag numbering scheme: (0 = 0, -1 = 1, 1 = 2, -2 = 3, 2 = 4, -3 = 5, 3 = 6 ...)
        // where for example 4 is encoded as 2

        Logger.log("Avro Record: " + record);

        byte[] bytes = genericRecordToBytes(schema, record);
        logBytesHex(bytes);

        GenericRecord recordRead = bytesToGenericRecord(schema, bytes);
        Logger.log("record read from bytes:" + recordRead);
    }
}

/*
- The bytes itself does NOT convey information about field names or types, they are from the schema.
- int/long encoding is using some zig-zag technique, e.g. 1 encoded as 02, -1 as 03, 2 as 04, etc. this is to save space?
- bytes are assembled one field after another, while the schema is the recipe (for both encoding and decoding)

 ## Avro Record:            {"f1": 1, "f2": "XY", "f3": 2, "f4": "CLUBS", "f5": null, "f6": "XYZ", "f7": ""}
 ## BYTES: 02 04 58 59 04 06 00 02 06 58 59 5A 00
 ## record read from bytes: {"f1": 1, "f2": "XY", "f3": 2, "f4": "CLUBS", "f5": null, "f6": "XYZ", "f7": ""}

 */