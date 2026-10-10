function retentionDays(variable) {
  const days = parseInt(process.env[variable] || "7", 10);
  if (!Number.isInteger(days) || days <= 0) {
    throw new Error(variable + " invalido: " + process.env[variable]);
  }
  return days;
}

const BATCH_RETENTION_DAYS = retentionDays("BATCH_RETENTION_DAYS");
const LOCK_RETENTION_DAYS = retentionDays("LOCK_RETENTION_DAYS");
const SHEDLOCK_COLLECTION = process.env.SHEDLOCK_COLLECTION || "bloqueio_agendamento";
const SHEDLOCK_LOCK_UNTIL_FIELD = process.env.SHEDLOCK_LOCK_UNTIL_FIELD || "bloqueado_ate";

const TTL_SPECS = [
  { collection: "batch_job_instance", field: "create_time", days: BATCH_RETENTION_DAYS },
  { collection: "batch_job_execution", field: "create_time", days: BATCH_RETENTION_DAYS },
  { collection: "batch_step_execution", field: "create_time", days: BATCH_RETENTION_DAYS },
  { collection: SHEDLOCK_COLLECTION, field: SHEDLOCK_LOCK_UNTIL_FIELD, days: LOCK_RETENTION_DAYS }
];

print("[ttl] banco=" + db.getName() + " retencao batch_*=" + BATCH_RETENTION_DAYS + " dia(s) lock="
  + LOCK_RETENTION_DAYS + " dia(s)");

const backfill = db.batch_job_instance.updateMany(
  { create_time: { $exists: false } },
  [{ $set: { create_time: "$$NOW" } }]
);
print("[ttl] backfill de create_time em batch_job_instance: " + backfill.modifiedCount + " documento(s)");

TTL_SPECS.forEach(function (spec) {
  const seconds = spec.days * 86400;
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
    coll.createIndex(keys, { name: name, expireAfterSeconds: seconds });
    print("[ttl]   criado   " + spec.collection + "." + name + " (" + seconds + "s)");
  } else if (existing.expireAfterSeconds !== seconds) {
    db.runCommand({ collMod: spec.collection, index: { name: name, expireAfterSeconds: seconds } });
    print("[ttl]   ajustado " + spec.collection + "." + name + " -> " + seconds + "s");
  } else {
    print("[ttl]   ok       " + spec.collection + "." + name + " (" + seconds + "s)");
  }
});

print("[ttl] concluido (batch_sequences intencionalmente sem TTL)");
