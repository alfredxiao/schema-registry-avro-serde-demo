package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.DefaultUser;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

public class BasicDefaultValueDemo {

    public static void main(String[] args) throws Exception {
        DefaultUser defaultUser = DefaultUser.newBuilder()
                .setId("001")
                .build();

        byte[] bytes = genericRecordToBytes(defaultUser.getSchema(), defaultUser);
        Utilities.logBytesHex(bytes);

        GenericRecord read = bytesToGenericRecord(defaultUser.getSchema(), bytes);
        Logger.log("user read from bytes:" + read);
    }
}

/*
 ## BYTES: 06 30 30 31 0E 44 45 46 41 55 4C 54
 ## user read from bytes:{"id": "001", "name": "DEFAULT"}

 Conclusion:
  - Default value does not get populated automatically. One has to set field value explicitly.
    = In the case of GenericRecord, you set it explicitly;
    = In the case of SpecificRecord, the generated Java object sets it for you.
  - the default values get populated in the bytes
 */