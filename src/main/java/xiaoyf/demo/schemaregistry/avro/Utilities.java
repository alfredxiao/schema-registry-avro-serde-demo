package xiaoyf.demo.schemaregistry.avro;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class Utilities {

    @SneakyThrows
    public static Schema asSchema(final String filename) {
        return new Schema.Parser().parse(new File("./src/main/avro/" + filename));
    }
    public static Schema stringToSchema(final String SCHEMA) {
        return new Schema.Parser().parse(SCHEMA);
    }

    public static GenericRecord bytesToGenericRecord(Schema schema, byte[] bytes) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        InputStream in = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return reader.read(null, decoder);
    }
    public static byte[] genericRecordToBytes(Schema schema, GenericRecord record) throws Exception {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        return stream.toByteArray();
    }

    public static byte[] arrayToBytes(Schema schema, GenericArray<GenericRecord> array) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);

        writer.write(array, encoder);

        encoder.flush();
        return stream.toByteArray();
    }


    public static GenericArray<GenericRecord> bytesToArray(Schema schema, byte[] bytes) throws IOException {
        GenericDatumReader<GenericArray<GenericRecord>> reader = new GenericDatumReader<>(schema);
        InputStream in = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return reader.read(null, decoder);
    }

    public static byte[] specificRecordToBytes(Schema schema, SpecificRecord record) throws Exception {
        DatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        return stream.toByteArray();
    }

    public static void logBytesHex(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        Logger.log("BYTES: " + sb);
    }
}
