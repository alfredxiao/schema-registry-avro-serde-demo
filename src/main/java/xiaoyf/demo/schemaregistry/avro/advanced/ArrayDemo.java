package xiaoyf.demo.schemaregistry.avro.advanced;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import xiaoyf.demo.schemaregistry.helper.Logger;
import xiaoyf.demo.schemaregistry.model.User;
import xiaoyf.demo.schemaregistry.model.UserCollection;

import java.util.List;

import static xiaoyf.demo.schemaregistry.avro.Utilities.arrayToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.asSchema;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToArray;
import static xiaoyf.demo.schemaregistry.avro.Utilities.bytesToGenericRecord;
import static xiaoyf.demo.schemaregistry.avro.Utilities.genericRecordToBytes;
import static xiaoyf.demo.schemaregistry.avro.Utilities.logBytesHex;


/*
 Purpose: Demonstrates how arrays are encoded, with variations of combinations of union, etc.

 Conclusion:
 - generic record and specific record do not differ in serialized bytes

 Output:

 ## demo: <user array, with array of length 0>'s bytes & decoded
 ## encoded: 00
 ## decoded: []
 ##
 ## demo: <user array, with array of length 2>'s bytes & decoded
 ## encoded: 04 02 0C 41 6C 66 72 65 64 04 0A 42 72 69 61 6E 00
 ## decoded: [{"id": 1, "name": "Alfred"}, {"id": 2, "name": "Brian"}]
 ##
 ## demo: <nullable array - null>'s encoded bytes & decoded
 ## encoded: 00
 ## null
 ##
 ## demo: <nullable array - length 0>'s bytes & decoded
 ## encoded: 02 00
 ## decoded: []
 ##
 ## demo: <nullable array - length 2>'s bytes & decoded
 ## encoded: 02 04 02 0C 41 6C 66 72 65 64 04 0A 42 72 69 61 6E 00
 ## decoded: [{"id": 1, "name": "Alfred"}, {"id": 2, "name": "Brian"}]
 ##
 ## demo: <empty generic UserCollection>'s encoded bytes & decoded
 ## encoded: 04 63 31 00
 ## {"collectionId": "c1", "users": []}
 ##
 ## demo: <generic UserCollection, with array of length 1>'s encoded bytes & decoded
 ## encoded: 04 63 32 02 02 0C 41 6C 66 72 65 64 00
 ## {"collectionId": "c2", "users": [{"id": 1, "name": "Alfred"}]}
 ##
 ## demo: <specific UserCollection, array of length 1>'s encoded bytes & decoded
 ## encoded: 04 63 32 02 02 0C 41 6C 66 72 65 64 00
 ## {"collectionId": "c2", "users": [{"id": 1, "name": "Alfred"}]}
 ##
 */

public class ArrayDemo {
    static Schema USER = asSchema("basic/user.avsc");
    static Schema USER_ARRAY = asSchema("advanced/array/user-array.avsc");
    static Schema USER_ARRAY_INSIDE_A_RECORD = asSchema("advanced/array/user-array-inside-a-record.avsc");
    static Schema NULLABLE_USER_ARRAY = asSchema("advanced/array/nullable-user-array.avsc");

    public static void main(String[] args) throws Exception {
        demoArray();
        demoNullableArray();
        demoArrayInsideARecord();
    }

    private static void demoNullableArray() {
        // null
        printBytes(
                "nullable array - null",
                NULLABLE_USER_ARRAY,
                (GenericRecord) null);

        // empty array, written to topSchema
        printBytes(
                "nullable array - length 0",
                NULLABLE_USER_ARRAY,
                users());

        // non-empty array, written to topSchema
        printBytes(
                "nullable array - length 2",
                NULLABLE_USER_ARRAY,
                users(1, "Alfred", 2, "Brian"));
    }

    private static void demoArray() {
        // empty pure array
        printBytes(
                "user array, with array of length 0",
                USER_ARRAY,
                users());

        printBytes(
                "user array, with array of length 2",
                USER_ARRAY,
                users(1, "Alfred", 2, "Brian"));
    }

    public static void demoArrayInsideARecord() {
        printBytes(
                "empty generic UserCollection",
                USER_ARRAY_INSIDE_A_RECORD,
                emptyGenericUserCollection(USER_ARRAY_INSIDE_A_RECORD, "c1"));

        GenericRecord userCollection = emptyGenericUserCollection(USER_ARRAY_INSIDE_A_RECORD, "c2");
        userCollection.put("users", users(1, "Alfred"));

        printBytes(
                "generic UserCollection, with array of length 1",
                USER_ARRAY_INSIDE_A_RECORD,
                userCollection);

        // specific record, array of length 2
        printBytes(
                "specific UserCollection, array of length 1",
                USER_ARRAY_INSIDE_A_RECORD,
                UserCollection.newBuilder()
                        .setCollectionId("c2")
                        .setUsers(List.of(user(1, "Alfred"))).build());
    }

    static GenericRecord emptyGenericUserCollection(Schema topSchema, String collectionId) {
        GenericRecord result = new GenericData.Record(topSchema);
        result.put("collectionId", collectionId);
        result.put("users", new GenericData.Array<GenericRecord>(0, USER_ARRAY));

        return result;
    }

    static GenericData.Array<GenericRecord> users(Object... attributes) {
        assert attributes.length % 2 == 0;

        final int size = attributes.length / 2;

        GenericData.Array<GenericRecord> result = new GenericData.Array<>(size, USER_ARRAY);

        for (int i=0; i<size; i++) {
            GenericRecord user = new GenericData.Record(USER);
            user.put("id", attributes[2*i]);
            user.put("name", attributes[2*i+1]);

            result.add(user);
        }

        return result;
    }

    static User user(Integer id, String name) {
        return User.newBuilder()
                .setId(id)
                .setName(name)
                .build();
    }


    @SneakyThrows
    private static void printBytes(String function, Schema schema, GenericRecord record) {
        Logger.log(String.format("demo: <%s>'s encoded bytes & decoded", function));

        byte[] bytes = genericRecordToBytes(schema, record);

        logBytesHex("encoded", bytes);
        Logger.log(bytesToGenericRecord(schema, bytes));
        Logger.log("");
    }

    @SneakyThrows
    private static void printBytes(String function, Schema schema, GenericArray<GenericRecord> array) {
        Logger.log(String.format("demo: <%s>'s bytes & decoded", function));

        byte[] bytes = arrayToBytes(schema, array);

        logBytesHex("encoded", bytes);
        Logger.log("decoded: " + bytesToArray(schema, bytes));
        Logger.log("");
    }

}
