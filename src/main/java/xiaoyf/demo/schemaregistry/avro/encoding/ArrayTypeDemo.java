package xiaoyf.demo.schemaregistry.avro.encoding;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;
import xiaoyf.demo.schemaregistry.model.UserCollection;
import xiaoyf.demo.schemaregistry.model.UsersAsTheOnlyField;

import java.util.ArrayList;
import java.util.List;

import static xiaoyf.demo.schemaregistry.avro.Utilities.arrayToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToArray;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;

public class ArrayTypeDemo {
    static Schema USER_SCHEMA = asSchema("user.avsc");
    static Schema USER_ARRAY_SCHEMA = asSchema("user-array.avsc");
    static Schema TOP_SCHEMA_ARRAY_AS_SECOND_FIELD = asSchema("user-array-as-second-field-of-a-record.avsc");
    static Schema TOP_SCHEMA_ARRAY_AS_ONLY_FIELD = asSchema("user-array-as-the-only-field-of-a-record.avsc");
    static Schema TOP_SCHEMA_ARRAY_AS_PART_OF_UNION = asSchema("user-array-as-part-of-union-top-schema.avsc");

    @SneakyThrows
    private static void printBytes(String demo, String function, Schema schema, GenericRecord record) {
        Logger.log(String.format("%s - %s bytes & readback", demo, function));

        byte[] bytes = genericRecordToBytes(schema, record);
        logBytesHex(bytes);

        Logger.log(bytesToGenericRecord(schema, bytes));
    }

    @SneakyThrows
    private static void printBytes(String demo, String function, Schema schema, GenericArray<GenericRecord> array) {
        Logger.log(String.format("%s - %s bytes & readback", demo, function));

        byte[] bytes = arrayToBytes(schema, array);
        logBytesHex(bytes);

        Logger.log(bytesToArray(schema, bytes));
    }

    public static void main(String[] args) throws Exception {
        demoArrayAsSecondFieldOfRecord();

        demoArrayAsTopSchema();

        demoArrayAsTheOnlyFieldOfRecord();

        // we can see the bytes is the same as demoArrayAsTheOnlyFieldOfRecord
        demoArrayAsPartOfUnion();
    }

    private static void demoArrayAsPartOfUnion() {
        Schema topSchema = TOP_SCHEMA_ARRAY_AS_PART_OF_UNION;

        assert topSchema.isUnion();
        assert topSchema.getTypes().get(0).isNullable();
        assert topSchema.getTypes().get(0).getType() == Schema.Type.NULL;
        assert topSchema.getTypes().get(1).getType() == Schema.Type.ARRAY;

        Schema userArraySchema =  topSchema.getTypes().get(1);
        assert USER_ARRAY_SCHEMA.equals(userArraySchema);

        assert userArraySchema.getElementType().isUnion();
        assert userArraySchema.getElementType().getTypes().get(0).isNullable();
        assert userArraySchema.getElementType().getTypes().get(0).getType() == Schema.Type.NULL;
        assert userArraySchema.getElementType().getTypes().get(1).getType() == Schema.Type.RECORD;

        Schema userSchema =  userArraySchema.getElementType().getTypes().get(1);
        assert userSchema.equals(USER_SCHEMA);

        final String demo = "demoArrayAsPartOfUnion";

        // null
        printBytes(
                demo,
                "Null",
                topSchema, (GenericRecord) null);

        // empty array, written to topSchema
        printBytes(
                demo,
                "topSchema, empty users array",
                topSchema,
                users());

        // non-empty array, written to topSchema
        printBytes(
                demo,
                "topSchema, users array of length 2",
                topSchema,
                users("u1", "Alfred", "u2", "Brian"));
    }

    private static void demoArrayAsTopSchema() {
        Schema topSchema = USER_ARRAY_SCHEMA;
        assert USER_SCHEMA.equals(topSchema.getElementType().getTypes().get(1));

        final String demo = "demoArrayAsTopSchema";

        // empty pure array
        printBytes(
                demo,
                "generic UserCollection, with array of length 0",
                topSchema,
                users());

        printBytes(
                demo,
                "generic UserCollection, with array of length 2",
                topSchema,
                users("u1", "Alfred", "u2", "Brian"));
    }

    static void demoArrayAsTheOnlyFieldOfRecord() {
        final String demo = "demoArrayAsTheOnlyFieldOfRecord";

        printBytes(
                demo,
                "null",
                TOP_SCHEMA_ARRAY_AS_ONLY_FIELD,
                UsersAsTheOnlyField.newBuilder()
                        .setUsers(null)
                        .build()
        );

        printBytes(
                demo,
                "empty",
                TOP_SCHEMA_ARRAY_AS_ONLY_FIELD,
                UsersAsTheOnlyField.newBuilder()
                        .setUsers(new ArrayList<>())
                        .build()
        );

        printBytes(
                demo,
                "specific UserCollection2, with array of length 2",
                TOP_SCHEMA_ARRAY_AS_ONLY_FIELD,
                UsersAsTheOnlyField.newBuilder()
                        .setUsers(List.of(
                                user("u1", "Alfred"),
                                user("u2", "Brian")
                        )).build());

    }

    public static void demoArrayAsSecondFieldOfRecord() {
        Schema topSchema = TOP_SCHEMA_ARRAY_AS_SECOND_FIELD;

        assert topSchema.getType() == Schema.Type.RECORD;
        assert topSchema.getField("users").schema().isUnion();
        assert topSchema.getField("users").schema().getTypes().get(0).isNullable();
        assert topSchema.getField("users").schema().getTypes().get(1).getType() == Schema.Type.RECORD;
        assert USER_SCHEMA.equals(
                topSchema.getField("users").schema().getTypes().get(1)
        );


        final String demo = "demoArrayAsSecondFieldOfRecord";

        // empty generic collection
        printBytes(
                demo,
                "empty generic UserCollection",
                topSchema,
                emptyCollection(topSchema, "c1"));

        // generic collection, array of length 1
        GenericRecord userCollection_1 = emptyCollection(topSchema, "c1");
        userCollection_1.put("users", users("u1", "Alfred"));

        printBytes(demo, "generic UserCollection, with array of length 1", topSchema, userCollection_1);

        // specific record, array of length 2
        printBytes(
                demo,
                "specific UserCollection, array of length 2",
                topSchema,
                UserCollection.newBuilder()
                        .setCollectionId("c1")
                        .setUsers(List.of(
                                user("u1", "Alfred"),
                                user("u2", "Brian")
                        )).build());
    }

    static GenericRecord emptyCollection(Schema topSchema, String collectionId) {
        GenericRecord result = new GenericData.Record(topSchema);
        result.put("collectionId", collectionId);

        return result;
    }

    static GenericData.Array<GenericRecord> users(String... attributes) {
        assert attributes.length % 2 == 0;

        final int size = attributes.length / 2;

        GenericData.Array<GenericRecord> result = new GenericData.Array<>(size, USER_ARRAY_SCHEMA);

        for (int i=0; i<size; i++) {
            GenericRecord user = new GenericData.Record(USER_SCHEMA);
            user.put("id", attributes[2*i]);
            user.put("name", attributes[2*i+1]);

            result.add(user);
        }

        return result;
    }

    static User user(String id, String name) {
        return User.newBuilder()
                .setId(id)
                .setName(name)
                .build();
    }
}

/*
 ## empty UserCollection read:{"collectionId": "c0", "users": null}
 ## BYTES: 04 63 30 00
 04: length is 2
 63 30: c0
 00: array length is 0

 ## generic UserCollection, with array of length 1:
          {"collectionId": "c1", "users": [{"id": "u1", "name": "Alfred"}]}
 ## BYTES: 04 63 31 02 02 02 04 75 31 0C 41 6C 66 72 65 64 00
 ## specific UserCollection, array of length 1:
          {"collectionId": "c1", "users": [{"id": "u1", "name": "Alfred"}]}
 ## BYTES: 04 63 31 02 02 02 04 75 31 0C 41 6C 66 72 65 64 00

  04: (string) length is 2
  63 31: c1
  02: (type) index is 1 -> second type of the union, hence array type
  02: array of length 1
  02: (type) index is 1 -> second type of the union, hence type User
  04: string length is 2
  75 31: u1
  0C: string length is 6
  41 6C 66 72 65 64: Alfred
  00: Array close

 ## specific UserCollection, array of length 2:
   {"collectionId": "c1", "users": [{"id": "u1", "name": "Alfred"}, {"id": "u2", "name": "Brian"}]}
 ## BYTES: 04 63 31 02 04 02 04 75 31 0C 41 6C 66 72 65 64 02 04 75 32 0A 42 72 69 61 6E 00
 04: length is 2
 63 31: c1
 02: (type) index is 1 -> second entry in the union type, (first type is null), second type is array
 04: (array) length is 2
 02: 1 -> second entry in the union type, (first tyep is null), second type is User
 04: (string) length is 2
 75 31: u1
 0C: (string) length is 6
 41 6C 66 72 65 64: Alfred
 02: (type) index is 1 -> second entry in the union type, (first type is null), second type is User
 04: (string) length is 2
 75 32: u2
 0A: (string) length is 5
 42 72 69 61 6E: Brian
 00: Close array
 */
