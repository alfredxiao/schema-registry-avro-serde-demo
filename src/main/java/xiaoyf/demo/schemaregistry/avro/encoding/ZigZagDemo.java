package xiaoyf.demo.schemaregistry.avro.encoding;

import xiaoyf.demo.schemaregistry.helper.Logger;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesHexString;
import static xiaoyf.demo.schemaregistry.avro.Utilities.zigzagDecode;
import static xiaoyf.demo.schemaregistry.avro.Utilities.zigzagEncode;

public class ZigZagDemo {
    public static void main(String[] args) throws IOException {
        final int end = 10000;
        for (int i=0; i<end; i++) {
            byte[] bytes = zigzagEncode(i);
            int decodedInt = zigzagDecode(bytes);

            if (decodedInt != i) {
                Logger.log("yuk, encode decode fails at " + i);
            } else {
                String bytesInBinary =
                        convertToStream(bytes)
                                .map(ZigZagDemo::toBinaryString)
                                .collect(Collectors.joining());
                Logger.log(String.format("%d -> %s / %s",
                        i,
                        bytesHexString(bytes, " "),
                        bytesInBinary));
            }
        }
    }

    static String toBinaryString(byte b) {
        // Use String.format to convert the byte to an 8-bit binary string
        return String.format("%8s.", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
    }

    private static Stream<Byte> convertToStream(byte[] byteArray) {
        // Use IntStream to convert each byte to a Byte object and then box it to a Stream<Byte>
        return IntStream.range(0, byteArray.length)
                .mapToObj(i -> byteArray[i]);
    }
}

/*
   int -> hex / binary(bits)
 ## 0 -> 00 / 00000000.
 ## 1 -> 02 / 00000010.
 ## 2 -> 04 / 00000100.
 ## 3 -> 06 / 00000110.
 ## 4 -> 08 / 00001000.
 ## 62 -> 7C / 01111100.
 ## 63 -> 7E / 01111110.
 ## 64 -> 80 01 / 10000000.00000001.
 ## 65 -> 82 01 / 10000010.00000001.
 ## 8190 -> FC 7F / 11111100.01111111.
 ## 8191 -> FE 7F / 11111110.01111111.
 ## 8192 -> 80 80 01 / 10000000.10000000.00000001.
 ## 8193 -> 82 80 01 / 10000010.10000000.00000001.

 So, it appears that the first bit (of a byte) has implications:
 1. if it is 0, no need for next byte;
 2. if it is 1, yes, next byte is required.
 */