package xiaoyf.demo.schemaregistry.avro.decodewithdiffschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.extractGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.recordToBytes;

public class RecordNameMismatchDemo {

    static String schemaString(final String name) {
        return "{" +
                "    \"type\": \"record\"," +
                "    \"name\": \"" + name + "\"," +
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
                "}";
    }

    final static String SCHEMA1 = schemaString("User");

    final static String SCHEMA2 = schemaString("WhatEver");

    public static void main(String[] args) throws Exception {
        Schema schema1 = new Parser().parse(SCHEMA1);

        GenericRecord user = new GenericData.Record(schema1);
        user.put("id", "001");
        user.put("name", "alfred");
        Logger.log("user created: " + user);

        byte[] userBytes = recordToBytes(schema1, user);

        Schema schema2 = new Parser().parse(SCHEMA2);
        GenericRecord userRead = extractGenericRecord(schema2, userBytes);
        Logger.log("user read from bytes:" + userRead);
    }
}

/*
 ## user created: {"id": "001", "name": "alfred"}
 ## user read from bytes:{"id": "001", "name": "alfred"}

 Conclusion: record name does not matter (to generic record)
 */