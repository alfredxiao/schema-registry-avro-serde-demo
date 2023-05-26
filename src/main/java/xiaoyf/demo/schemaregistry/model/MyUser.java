package xiaoyf.demo.schemaregistry.model;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

public class MyUser extends SpecificRecordBase {

    static final String SCHEMA_STR = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"User\",\n" +
            "    \"namespace\": \"xiaoyf.demo.schemaregistry.model\",\n" +
            "    \"fields\": [\n" +
            "        {\n" +
            "            \"name\" : \"id\",\n" +
            "            \"type\" : \"string\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\" : \"name\",\n" +
            "            \"type\" : \"string\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(SCHEMA_STR);
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    // Used by DatumWriter.  Applications should not call.
    public Object get(int field$) {
        switch (field$) {
            case 0: return id;
            case 1: return name;
            default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    @Override
    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: id = value$ != null ? value$.toString() : null; break;
            case 1: name = value$ != null ? value$.toString() : null; break;
            default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    private String id;
    private String name;

    public MyUser(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
