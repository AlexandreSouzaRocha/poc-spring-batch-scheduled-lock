const RETENTION_DAYS = parseInt(process.env.BATCH_RETENTION_DAYS || "7", 10);

if (!Number.isInteger(RETENTION_DAYS) || RETENTION_DAYS <= 0) {
  throw new Error("BATCH_RETENTION_DAYS invalido: " + process.env.BATCH_RETENTION_DAYS);
}

const RETENTION_SECONDS = RETENTION_DAYS * 86400;

const TTL_SPECS = [
  { collection: "batch_job_instance", field: "create_time" },
  { collection: "batch_job_execution", field: "create_time" },
  { collection: "batch_step_execution", field: "create_time" }
];

print("[ttl] banco=" + db.getName() + " retencao=" + RETENTION_DAYS + " dia(s) (" + RETENTION_SECONDS + "s)");

const backfill = db.batch_job_instance.updateMany(
  { create_time: { $exists: false } },
  [{ $set: { create_time: "$$NOW" } }]
);
print("[ttl] backfill de create_time em batch_job_instance: " + backfill.modifiedCount + " documento(s)");

TTL_SPECS.forEach(function (spec) {
  const name = spec.field + "_ttl";
  const coll = db.getCollection(spec.collection);
  const keys = {};
  keys[spec.field] = 1;

  const currentIndexes = coll.exists() ? coll.getIndexes() : [];

  currentIndexes.forEach(function (index) {
    if (index.expireAfterSeconds !== undefined && index.name !== name) {
      coll.dropIndex(index.name);
      print("[ttl]   removido " + spec.collection + "." + index.name + " (TTL obsoleto)");
    }
  });

  const existing = currentIndexes.filter(function (index) {
    return index.name === name;
  })[0];

  if (!existing) {
    coll.createIndex(keys, { name: name, expireAfterSeconds: RETENTION_SECONDS });
    print("[ttl]   criado   " + spec.collection + "." + name + " (" + RETENTION_SECONDS + "s)");
  } else if (existing.expireAfterSeconds !== RETENTION_SECONDS) {
    db.runCommand({ collMod: spec.collection, index: { name: name, expireAfterSeconds: RETENTION_SECONDS } });
    print("[ttl]   ajustado " + spec.collection + "." + name + " -> " + RETENTION_SECONDS + "s");
  } else {
    print("[ttl]   ok       " + spec.collection + "." + name + " (" + RETENTION_SECONDS + "s)");
  }
});

print("[ttl] concluido (batch_sequences intencionalmente sem TTL)");
