package xiaoyf.demo.schemaregistry.avro.basic;

import org.apache.avro.Schema;
import xiaoyf.demo.schemaregistry.avro.Utilities;
import xiaoyf.demo.schemaregistry.helper.Logger;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToPrimitive;
import static xiaoyf.demo.schemaregistry.avro.Utilities.primitiveToBytes;


/*
 Purpose: Demonstrates encoding primitives in avro
 Conclusion: You can encode/decode record type, complex type, or primitive types

 Output:
  ## 2 encoded as: 04
  ## decoded as: 2
 */
public class PrimitiveDemo {

    public static void main(String[] args) throws Exception {
        Schema schema = asSchema("basic/primitive.avsc");

        byte[] bytes = primitiveToBytes(schema, 2);

        Utilities.logBytesHex("2 encoded as", bytes);

        int decoded = bytesToPrimitive(schema, bytes);
        Logger.log("decoded as: " + decoded);
    }
}
