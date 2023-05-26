package xiaoyf.demo.schemaregistry.avro.encoding;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.io.File;
import java.util.Map;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

public class ComplexTypeDemo {

    public static void main(String[] args) throws Exception {
        Schema productSchema = new Parser().parse(new File("./src/main/avro/complex.avsc"));

        GenericRecord product = new GenericData.Record(productSchema);
        product.put("id", "123");
        product.put("name", "XY");

        Schema categorySchema = productSchema.getField("category").schema();
        GenericRecord category = new GenericData.Record(categorySchema);
        category.put("catId", "456");
        category.put("catName", "YZ");
        category.put("additional", Map.of("a", "AB", "b", "CD"));

        product.put("category", category);

        byte[] bytes = genericRecordToBytes(productSchema, product);

        GenericRecord read = bytesToGenericRecord(productSchema, bytes);
        Logger.log("Product read:" + read);

        logBytesHex(bytes);
    }
}

/*
 Complex type encoding is not too different from simple types.

 ## Product read:{"id": "123", "name": "XY", "category": {"catId": "456", "catName": "YZ", "additional": {"a": "AB", "b": "CD"}}}
 ## BYTES: 06 31 32 33 04 58 59 06 34 35 36 04 59 5A 04 02 62 04 43 44 02 61 04 41 42 00
 06 31 32 33 04 58 59 -> "id": "123", "name": "XY"
 06 34 35 36 -> "456"

 */
