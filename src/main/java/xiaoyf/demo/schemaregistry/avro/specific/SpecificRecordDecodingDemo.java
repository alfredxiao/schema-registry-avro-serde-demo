package xiaoyf.demo.schemaregistry.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;

import java.io.File;

import static xiaoyf.demo.schemaregistry.avro.Utilities.stringToSchema;

public class SpecificRecordDecodingDemo {


    public static void main(String[] args) throws Exception {
        Schema schema = stringToSchema("{" +
                "    \"type\": \"record\"," +
                "    \"name\": \"Unknown\"," +
                "    \"fields\": [" +
                "        {" +
                "            \"name\" : \"id\"," +
                "            \"type\" : \"string\"" +
                "        }," +
                "        {" +
                "            \"name\" : \"name\"," +
                "            \"type\" : \"string\"" +
                "        }" +
                "    ]" +
                "}");

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", "001");
        genericRecord.put("name", "alfred");

        Schema schema2 = new Parser().parse(new File("./src/main/avro/user.avsc"));
        User user = (User) SpecificData.get().deepCopy(schema2, genericRecord);
        Logger.log("specific user record created from bytes:" + user);
    }
}
