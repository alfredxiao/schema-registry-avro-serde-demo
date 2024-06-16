package xiaoyf.demo.schemaregistry.avro.advanced;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;

public class LogicalTypeDemo {
    public static void main(String[] args) throws Exception {
        LogicalTypes.register("string-set", new StringSetLogicalTypeFactory());
        Schema schema = asSchema("advanced/logical-types.avsc");
        GenericRecord logicalRecord = new GenericData.Record(schema);
        logicalRecord.put("myDecimal", new BigDecimal("2.3"));
        logicalRecord.put("myInstant", Instant.ofEpochMilli(1));
        logicalRecord.put("mySet", new StringSet("cat,dog"));

        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new StringSetConversion());

        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());


        byte[] bytes = genericRecordToBytes(schema, genericData, logicalRecord);
        logBytesHex("logical entity encoded", bytes);

        GenericRecord read = bytesToGenericRecord(schema, genericData, bytes);
        Logger.log("then decoded as:" + read);

    }
    
    public static class StringSet {
        private Set<String> data;
        public StringSet(String str) {
           this.data = Arrays.stream(str.split(","))
                   .collect(Collectors.toSet());
        }
        public Set<String> getData() {
            return data;
        }

        public String toString() {
            return String.join(",", data);
        }
    }

    public static class StringSetConversion extends Conversion<StringSet> {
        @Override
        public Class<StringSet> getConvertedType() {
            return StringSet.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "string-set";
        }

        @Override
        public StringSet fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            // Conversion from CharSequence (the base type) to StringSet
            return new StringSet(value.toString());
        }

        @Override
        public CharSequence toCharSequence(StringSet value, Schema schema, LogicalType type) {
            // Conversion from StringSet to CharSequence (the base type)
            return value.toString();
        }
    }

    public static class StringSetLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
        public LogicalType fromSchema(Schema schema) {
            return new StringSetLogicalType("string-set");
        }
    }

    public static class StringSetLogicalType extends LogicalType {

        public StringSetLogicalType(String logicalTypeName) {
            super(logicalTypeName);
        }
    }
}

