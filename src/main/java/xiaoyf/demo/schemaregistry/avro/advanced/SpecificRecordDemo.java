package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.specificRecordToBytes;

/*
 Purpose: Demonstrates how specific record can be encoded/decoded.

 Output:
  ## BYTES: 02 0C 61 6C 66 72 65 64
  ## generic record created from bytes:{"id": 1, "name": "alfred"}
  ## specific user record created from bytes:{"id": 1, "name": "alfred"}

 */
public class SpecificRecordDemo {


    public static void main(String[] args) throws Exception {
        Schema schema = asSchema("basic/user.avsc");

        User user = new User(1, "alfred");

        byte[] bytes = specificRecordToBytes(schema, user);
        logBytesHex(bytes);

        GenericRecord record = bytesToGenericRecord(schema, bytes);
        Logger.log("generic record created from bytes:" + record);

        User userRead = (User) SpecificData.get().deepCopy(schema, record);
        Logger.log("specific user record created from bytes:" + userRead);
    }
}
