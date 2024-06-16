package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

/*
 Purpose: Demonstrates avro encoding of few more types
 Conclusion:
 - The bytes itself does NOT convey information about field names or types, they are from the schema.
 - bytes are assembled one field after another, while the schema is the recipe (for both encoding and decoding)
 - Int/Long: encoding is using some zigzag technique, e.g. 1 encoded as 02, -1 as 03, 2 as 04, etc
 - String: starts with length (in zigzag encoding)
 - Union: starts with index of the type (also in zigzag encoding), followed by the string itself (length + string)
 - Enum: just encodes the index is enough

 Output:
 ## Avro Record: {"myInt": 1, "myFloat": 2.382, "myString": "XY", "myLong": 2, "myEnum": "CLUBS", "myNullableString": null, "yetNullableString": "XYZ"}
 ## BYTES: 02 B0 72 18 40 04 58 59 04 06 00 02 06 58 59 5A
 ## record read from bytes:{"myInt": 1, "myFloat": 2.382, "myString": "XY", "myLong": 2, "myEnum": "CLUBS", "myNullableString": null, "yetNullableString": "XYZ"}

 */
public class MiscTypesDemo {

    final static String SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"MiscTypesDemo\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"myInt\"," +
            "            \"type\" : \"int\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"myFloat\"," +
            "            \"type\" : \"float\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"myString\"," +
            "            \"type\" : \"string\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"myLong\"," +
            "            \"type\" : \"long\"" +
            "        }," +
            "        {" +
            "            \"name\" : \"myEnum\"," +
            "            \"type\" : " + "{ \"type\": \"enum\"," +
            "                 \"name\": \"Suit\"," +
            "                 \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]" +
            "            }"+
            "        }," +
            "        {" +
            "            \"name\" : \"myNullableString\"," +
            "            \"type\" : [\"null\", \"string\"]" +
            "        }," +
            "        {" +
            "            \"name\" : \"yetNullableString\"," +
            "            \"type\" : [\"null\", \"string\"]" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schema = stringToSchema(SCHEMA);

        GenericRecord record = new GenericData.Record(schema);
        record.put("myInt", 1);                 // 02               which is the first byte in output bytes
        record.put("myFloat", 2.382F);          // B0 72 18 40      4 bytes used for float
        record.put("myString", "XY");           // 04 58 59         (04 means 2, which is length of the string)
        record.put("myLong", 2L);               // 04               which means 2 here

        record.put("myEnum", new GenericData.EnumSymbol(SchemaBuilder.enumeration("Suit").symbols("SPADES", "HEARTS", "DIAMONDS", "CLUBS"), "CLUBS"));
                                                // 06               which means 3 (index 3, which is the fourth element)
        record.put("myNullableString", null);   // 00               (0 as index in the union types)
        record.put("yetNullableString", "XYZ"); // 02 06 58 59 5A   (02 means 1, so 1 is index in the union types; 06 means 3, string length is 3)

        Logger.log("Avro Record: " + record);

        byte[] bytes = genericRecordToBytes(schema, record);
        logBytesHex(bytes);

        GenericRecord recordRead = bytesToGenericRecord(schema, bytes);
        Logger.log("record read from bytes:" + recordRead);
    }
}

