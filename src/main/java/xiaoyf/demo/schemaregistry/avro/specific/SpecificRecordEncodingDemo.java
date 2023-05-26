package xiaoyf.demo.schemaregistry.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;

import java.io.File;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.specificRecordToBytes;

public class SpecificRecordEncodingDemo {


    public static void main(String[] args) throws Exception {
        Schema schema = new Parser().parse(new File("./src/main/avro/user.avsc"));

        User user = new User("001", "alfred");

        byte[] bytes = specificRecordToBytes(schema, user);
        logBytesHex(bytes);

        GenericRecord record = bytesToGenericRecord(schema, bytes);
        Logger.log("generic record created from bytes:" + record);

        User userRead = (User) SpecificData.get().deepCopy(schema, record);
        Logger.log("specific user record created from bytes:" + userRead);
    }
}
