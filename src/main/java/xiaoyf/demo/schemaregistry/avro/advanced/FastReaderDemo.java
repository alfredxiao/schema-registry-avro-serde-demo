package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Map;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;

/*
 Purpose: demonstrates how to enable fast reader (just by assigning system property)
 */
public class FastReaderDemo {

    public static void main(String[] args) throws Exception {
        Schema productSchema = asSchema("advanced/complex.avsc");
        byte[] bytes = bytes(productSchema);

        final int warmupRepeat = 5000000;
        final String flag = "false";

        // warmup
        demo(warmupRepeat, bytes, productSchema, flag);

        final int demoRepeat = 5000000;
//        long t = demo(demoRepeat, bytes, productSchema, flag);
//        System.out.printf("%s : %d", flag, t);

        String flag1 = "true";
        long t1 = demo(demoRepeat, bytes, productSchema, flag1);

        String flag2 = "false";
        long t2 = demo(demoRepeat, bytes, productSchema, flag2);

        System.out.printf("%s: %d, %s: %d%n", flag1, t1, flag2, t2);
    }

    static long demo(int repeat, byte[] bytes, Schema productSchema, String fastReadEnabled) throws Exception {
        long start = System.currentTimeMillis();
        System.setProperty("org.apache.avro.specific.use_custom_coders", fastReadEnabled);
        System.setProperty("org.apache.avro.fastread", fastReadEnabled);
        for (int i=0;i<repeat;i++) {
            run(productSchema, bytes);
        }
        return System.currentTimeMillis() - start;
    }

    static byte[] bytes(Schema productSchema) throws Exception {

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

        return genericRecordToBytes(productSchema, product);
    }

    static void run(Schema productSchema, byte[] bytes) throws IOException {
        GenericRecord read = bytesToGenericRecord(productSchema, bytes);
    }
}
