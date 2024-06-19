package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GenericRecordHelper {

    static GenericRecord selectFields(GenericRecord record, Set<String> valueFields) {
        Schema subSchema = selectSchema(record.getSchema(), valueFields);

        GenericRecord subRecord = new GenericData.Record(subSchema);
        for (String fieldName : valueFields) {
            subRecord.put(fieldName, record.get(fieldName));
        }

        return subRecord;
    }

    static Schema selectSchema(Schema schema, Set<String> valueFields) {
        List<Schema.Field> fields = new ArrayList<>();
        for (String fieldName : valueFields) {
            Schema.Field field = new Schema.Field(fieldName, schema.getField(fieldName).schema());
            fields.add(field);
        }

        return Schema.createRecord("temp", "temp-doc", "temp-namespace", false, fields);
    }
}
