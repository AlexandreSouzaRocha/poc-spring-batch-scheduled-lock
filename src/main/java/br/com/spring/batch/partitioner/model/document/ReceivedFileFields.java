package br.com.spring.batch.partitioner.model.document;

public final class ReceivedFileFields {

    public static final String ID = "_id";
    public static final String ROLE = "tipo";
    public static final String PARENT_FILE_ID = "id_arquivo_pai";
    public static final String FILE_NAME = "nome_arquivo";
    public static final String STATUS = "status";
    public static final String ATTEMPTS = "tentativas";

    public static final String BLOB = "blob";
    public static final String MOVEMENT = "movimento";
    public static final String TRAILER = "trailer";
    public static final String PARTITIONING = "particionamento";
    public static final String AUDIT = "auditoria";

    public static final String SOURCE_PATH = "caminho_origem";
    public static final String CURRENT_PATH = "caminho_atual";
    public static final String ETAG = "etag";
    public static final String SIZE_BYTES = "tamanho_bytes";

    public static final String HEADER = "header";
    public static final String TYPE = "tipo";
    public static final String DATE = "data";

    public static final String RECORD_COUNT = "quantidade_registros";
    public static final String PROCESSING_DATE = "data_processamento";

    public static final String INDEX = "indice";
    public static final String COUNT = "quantidade";
    public static final String LINE_COUNT = "quantidade_linhas";
    public static final String BYTE_START = "byte_inicio";
    public static final String BYTE_END = "byte_fim";

    public static final String CREATED_AT = "criado_em";
    public static final String UPDATED_AT = "atualizado_em";
    public static final String PUBLISHED_AT = "publicado_em";

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
