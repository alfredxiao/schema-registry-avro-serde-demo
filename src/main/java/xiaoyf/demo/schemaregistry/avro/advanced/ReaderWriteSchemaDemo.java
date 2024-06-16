package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import xiaoyf.demo.schemaregistry.avro.Utilities;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;

/*
 Purpose: how to enable fast reader
 todo: not yet finished/verified
 */
public class ReaderWriteSchemaDemo {

    public static void main(String[] args) throws Exception {
        Schema writerSchema = Utilities.stringToSchema("""
                {
                    "type": "record",
                    "name": "User",
                    "namespace": "demo.model",
                    "fields": [
                        {
                            "name" : "id",
                            "type" : "int"
                        },
                        {
                            "name" : "name",
                            "type" : "string"
                        }
                    ]
                }
                """);

        GenericRecord writerUser = new GenericData.Record(writerSchema);
        writerUser.put("id", 1);
        writerUser.put("name", "alfred");

        File file = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(writerSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(writerSchema, file);
        dataFileWriter.append(writerUser);
        dataFileWriter.close();

        // file to byte[], Path
        byte[] bytes = Files.readAllBytes(Paths.get("users.avro"));
        logBytesHex("encoded as", bytes);

        Schema readerSchema = Utilities.stringToSchema("""
                {
                    "type": "record",
                    "name": "User",
                    "namespace": "demo.model",
                    "fields": [
                        {
                            "name" : "id",
                            "type" : "int"
                        },
                        {
                            "name" : "name",
                            "type" : "int"
                        }
                    ]
                }
                """);

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(readerSchema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
