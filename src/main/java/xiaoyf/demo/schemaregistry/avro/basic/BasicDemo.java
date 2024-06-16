package xiaoyf.demo.schemaregistry.avro.basic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;


/*
 Purpose: Demonstrates basic avro encoding, int/long and string length use zigzag encoding,

 Output:
 ## BYTES: 06 0C 61 6C 66 72 65 64
 ## generic user read from bytes:{"id": 3, "name": "alfred"}

 06 -> zigzag algorithm maps to 3 (as length of string)

 0C -> zigzag maps to 6 (as length of second string)
 61 6C 66 72 65 64 -> "alfred"
 */
public class BasicDemo {

    public static void main(String[] args) throws Exception {
        Schema schema = asSchema("basic/user.avsc");
        schema.getFields().get(0).schema().getType();

        GenericRecord user = new GenericData.Record(schema);
        user.put("id", 3);
        user.put("name", "alfred");

        byte[] bytes = genericRecordToBytes(schema, user);
        Utilities.logBytesHex(bytes);

        GenericRecord read = bytesToGenericRecord(schema, bytes);
        Logger.log("generic user read from bytes:" + read);
    }
}
