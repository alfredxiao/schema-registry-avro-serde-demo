package xiaoyf.demo.schemaregistry.avro.advanced;

import demo.model.Measurement;
import demo.model.MeasurementAggregated;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.util.ArrayList;

import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;

/*
 Purpose: Demonstrates a real use case avro design and how it encodes.

 Output:
  ## Product read:{"subject": "", "winStart": "", "winEnd": "", "grace": 0, "measurements": [{"subject": "A", "measurementId": 0, "measurement": 0, "when": "B"}, {"subject": "A", "measurementId": 0, "measurement": 0, "when": "B"}]}
  ## BYTES: 00 02 00 02 00 02 00 04 02 41 00 00 02 42 02 41 00 00 02 42 00
 More Outputs:
    // empty:  ## BYTES: 00 02 00 02 00 02 00 00
    // size=2            00 02 00 02 00 02 00 04 (02 41 00 00 02 42) (02 41 00 00 02 42) 00
    // below with schema id=6
    //    0, 0, 0, 0, 6, 0, 2, 0, 2, 0, 2, 0, 12, 0, 0, 0, 40, 50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 53, 58, 48, 48, 0, 0, 0, 40, 50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 53, 58, 48, 48, 90, 0, 0, 0, 40, 50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 84, 48, 48, 58, 48, 53, 58, 48, 48, 90, 0, 0, 0, 40, 50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 84, 48, +58 more
    //                                         6           20  2000-01-01T00:10:00Z

 */
public class MeasurementAggregatedDemo {

    private static MeasurementAggregated emptyAggregation() {
        return MeasurementAggregated.newBuilder()
                .setSubject("")
                .setWinStart("")
                .setWinEnd("")
                .setGrace(0L)
                .setMeasurements(new ArrayList<>())
                .build();
    }

    private static Measurement measurement() {
        return Measurement.newBuilder()
                .setSubject("A")
                .setMeasurement(0L)
                .setWhen("B")
                .setMeasurementId(0L)
                .build();
    }

    public static void main(String[] args) throws Exception {
        Schema productSchema = MeasurementAggregated.SCHEMA$;

        MeasurementAggregated empty = emptyAggregation();
        empty.getMeasurements().add(measurement());
//        empty.getMeasurements().add(measurement());

        byte[] bytes = genericRecordToBytes(productSchema, empty);

        GenericRecord read = bytesToGenericRecord(productSchema, bytes);
        Logger.log("Product read:" + read);

        logBytesHex(bytes);
    }
}

