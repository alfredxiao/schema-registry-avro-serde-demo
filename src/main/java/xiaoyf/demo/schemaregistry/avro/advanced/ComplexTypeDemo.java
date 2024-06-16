package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

/*
 Purpose: Demonstrates nested type, and maps

 Conclusion: Complex type encoding is not too different from simple types.

 Output:
 ## Product read:{"id": "123", "name": "iPhone", "attributes": {"vendor": "Apple"}, "category": {"categoryId": "890", "categoryName": "Mobile"}}
 ## BYTES: 06 31 32 33 0C 69 50 68 6F 6E 65 02 0C 76 65 6E 64 6F 72 0A 41 70 70 6C 65 00 06 38 39 30 0C 4D 6F 62 69 6C 65
 */

public class ComplexTypeDemo {

    public static void main(String[] args) throws Exception {
        Schema productSchema = asSchema("advanced/complex.avsc");

        GenericRecord product = new GenericData.Record(productSchema);
        product.put("id", "123");           // 06 31 32 33          len=3
        product.put("name", "iPhone");      // 0C 69 50 68 6F 6E 65 len=6

        // 02 0C 76 65 6E 64 6F 72 0A 41 70 70 6C 65 00
        // 02                       -> One entry (in the map)
        // 0C 76 65 6E 64 6F 72     -> key "vendor" (len=6)
        // 0A 41 70 70 6C 65        -> value "Apple" (len=5)
        // 00                       -> close map
        product.put("attributes", Map.of("vendor", "Apple"));

        Schema categorySchema = productSchema.getField("category").schema();
        GenericRecord category = new GenericData.Record(categorySchema);
        category.put("categoryId", "890");         // 06 38 39 30 (len=3)
        category.put("categoryName", "Mobile");    // 0C 4D 6F 62 69 6C 65 (len=6)
        product.put("category", category);

        byte[] bytes = genericRecordToBytes(productSchema, product);
        logBytesHex(bytes);

        GenericRecord read = bytesToGenericRecord(productSchema, bytes);
        Logger.log("Product read:" + read);
    }
}

