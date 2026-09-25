package br.com.spring.batch.partitioner.model.document;

public final class ReceivedFileFields {

    public static final String ID = "_id";
    public static final String ROLE = "role";
    public static final String PARENT_FILE_ID = "parent_file_id";
    public static final String FILE_NAME = "file_name";
    public static final String STATUS = "status";
    public static final String ATTEMPTS = "attempts";

    public static final String BLOB = "blob";
    public static final String MOVEMENT = "movement";
    public static final String PARTITIONING = "partitioning";
    public static final String AUDIT = "audit";

    public static final String SOURCE_PATH = "source_path";
    public static final String CURRENT_PATH = "current_path";
    public static final String ETAG = "etag";
    public static final String SIZE_BYTES = "size_bytes";

    public static final String HEADER = "header";
    public static final String TYPE = "type";
    public static final String DATE = "date";

    public static final String INDEX = "index";
    public static final String COUNT = "count";
    public static final String LINE_COUNT = "line_count";
    public static final String BYTE_START = "byte_start";
    public static final String BYTE_END = "byte_end";

    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";
    public static final String PUBLISHED_AT = "published_at";

    private ReceivedFileFields() {
    }

    public static String blob(String field) {
        return BLOB + "." + field;
    }

    public static String movement(String field) {
        return MOVEMENT + "." + field;
    }

    public static String audit(String field) {
        return AUDIT + "." + field;
    }
}
