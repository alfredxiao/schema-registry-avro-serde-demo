package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.nio.ByteBuffer;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

public class PureRawBytesDemo {

    final static String SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"MyType\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\" : \"bytes\"," +
            "            \"type\" : \"bytes\"" +
            "        }" +
            "    ]" +
            "}";

    public static void main(String[] args) throws Exception {
        Schema schema = stringToSchema(SCHEMA);

        byte[] rawBytes = new byte[]{0,1,2};
        ByteBuffer bf = ByteBuffer.wrap(rawBytes);
        schema.getFields().get(0).schema().getType();
        GenericRecord user = new GenericData.Record(schema);
        user.put("bytes", bf);

        byte[] bytes = genericRecordToBytes(schema, user);
        Utilities.logBytesHex(bytes);

        GenericRecord read = bytesToGenericRecord(schema, bytes);
        Logger.log("user read from bytes:" + read);
    }
}

/*
 ## BYTES: 06 00 01 02
 ## user read from bytes:{"bytes": "\u0000\u0001\u0002"}
 */