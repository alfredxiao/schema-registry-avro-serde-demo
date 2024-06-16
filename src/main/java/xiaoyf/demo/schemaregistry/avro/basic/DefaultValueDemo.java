package xiaoyf.demo.schemaregistry.avro.basic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;

import java.io.File;

import static xiaoyf.demo.schemaregistry.avro.Utilities.*;

/*
 Purpose: Demonstrates avro's default value in Generic and Specific records.

 Conclusion:
  - Default value behaves differently in Generic and Specific records.
    = In the case of GenericRecord, you set it explicitly;
    = In the case of SpecificRecord, the generated Java object sets it for you.
  - GenericRecord instance may be invalid (if missing default value) which then fails at serializing as bytes

 Output:
 ## generic user created: {"id": "001", "name": null}
 ## specific user created: {"id": "001", "name": "DEFAULT"}
 ## specific user bytes: 06 30 30 31 0E 44 45 46 41 55 4C 54
 ## generic user read from bytes:{"id": "001", "name": "DEFAULT"}
 ## specific user read from bytes:{"id": "001", "name": "DEFAULT"}
 */
public class DefaultValueDemo {

    public static void main(String[] args) throws Exception {
        Schema schema = asSchema("basic/user-with-default.avsc");

        // generic user
        GenericRecord genericUser = new GenericData.Record(schema);
        genericUser.put("id", "001");
        Logger.log("generic user created: " + genericUser);

        // fail with NPE "null value for (non-nullable) string at User.name"
        // byte[] bytes = genericRecordToBytes(schema, genericUser);

        // specific user
        User specificUser = User.newBuilder()
                .setId(1)
                .build();

        Logger.log("specific user created: " + specificUser);

        byte[] bytes = genericRecordToBytes(schema, specificUser);
        Utilities.logBytesHex("specific user bytes", bytes);

        GenericRecord genericRead = bytesToGenericRecord(schema, bytes);
        Logger.log("generic user read from bytes:" + genericRead);

        final User specificRead = (User) bytesToSpecificRecord(schema, bytes);
        Logger.log("specific user read from bytes:" + specificRead);
    }
}
