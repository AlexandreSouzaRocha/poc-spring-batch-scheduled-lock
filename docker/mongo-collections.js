const SHEDLOCK_COLLECTION = process.env.SHEDLOCK_COLLECTION || "scheduler_locks";
const SHEDLOCK_NAME_FIELD = process.env.SHEDLOCK_NAME_FIELD || "_id";

const BATCH_COLLECTIONS = ["batch_job_instance", "batch_job_execution", "batch_step_execution", "batch_sequences"];
const BATCH_SEQUENCES = ["batch_job_instance_seq", "batch_job_execution_seq", "batch_step_execution_seq"];
const FILE_COLLECTION = "received_file_management";

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

createIndex(FILE_COLLECTION, { role: 1, status: 1, "audit.created_at": 1 }, { name: "role_status_created_at" });
createIndex(FILE_COLLECTION, { parent_file_id: 1, "partitioning.index": 1 },
  { name: "parent_file_id_partition_index" });

if (SHEDLOCK_NAME_FIELD !== "_id") {
  const keys = {};
  keys[SHEDLOCK_NAME_FIELD] = 1;
  createIndex(SHEDLOCK_COLLECTION, keys, { name: SHEDLOCK_NAME_FIELD + "_unique", unique: true });
}
