const SHEDLOCK_COLLECTION = process.env.SHEDLOCK_COLLECTION || "bloqueio_agendamento";
const SHEDLOCK_NAME_FIELD = process.env.SHEDLOCK_NAME_FIELD || "_id";

const BATCH_COLLECTIONS = ["batch_job_instance", "batch_job_execution", "batch_step_execution", "batch_sequences"];
const BATCH_SEQUENCES = ["batch_job_instance_seq", "batch_job_execution_seq", "batch_step_execution_seq"];
const FILE_COLLECTION = "gerenciamento_arquivo_recebido";

function createCollection(name) {
  if (!db.getCollectionNames().includes(name)) {
    db.createCollection(name);
    print("[collections] criada " + name);
    return;
  }
  print("[collections] ok      " + name);
}

function createIndex(collection, keys, options) {
  db.getCollection(collection).createIndex(keys, options);
  print("[indexes]     " + collection + "." + options.name);
}

BATCH_COLLECTIONS.concat([FILE_COLLECTION, SHEDLOCK_COLLECTION]).forEach(createCollection);

BATCH_SEQUENCES.forEach(function (sequence) {
  db.batch_sequences.updateOne({ _id: sequence }, { $setOnInsert: { count: NumberLong("0") } }, { upsert: true });
});
print("[sequences]   batch_sequences com " + BATCH_SEQUENCES.length + " contadores");

createIndex(FILE_COLLECTION, { tipo: 1, status: 1, "auditoria.criado_em": 1 }, { name: "tipo_status_criado_em" });
createIndex("batch_job_instance", { job_name: 1, job_key: 1 },
  { name: "job_name_job_key_unique", unique: true });
createIndex(FILE_COLLECTION, { id_arquivo_pai: 1, "particionamento.indice": 1 },
  { name: "id_arquivo_pai_particionamento_indice" });

if (SHEDLOCK_NAME_FIELD !== "_id") {
  const keys = {};
  keys[SHEDLOCK_NAME_FIELD] = 1;
  createIndex(SHEDLOCK_COLLECTION, keys, { name: SHEDLOCK_NAME_FIELD + "_unique", unique: true });
}
