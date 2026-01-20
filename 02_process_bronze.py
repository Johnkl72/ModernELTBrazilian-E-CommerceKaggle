# Databricks notebook source
from datetime import datetime
from pyspark.sql import functions as F
from utils import entities

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date") or datetime.now().strftime("%Y-%m-%d")

base = "/Volumes/workspace/default/data/olist"
landing_run = f"{base}/landing/{run_date}"
bronze_base = f"{base}/bronze"

dbutils.fs.mkdirs(bronze_base)

print("run_date:", run_date)
print("landing_run:", landing_run)
print("bronze_base:", bronze_base)

# COMMAND ----------

def landing_file_meta(entity: str, filename: str):
    p = f"{landing_run}/{filename}"
    files = dbutils.fs.ls(p)
    if len(files) == 0:
        raise Exception(f"No se encontró el archivo: {p}")

    f = files[0]
    return (entity, f.path, int(f.size), int(f.modificationTime))

incoming_meta = [landing_file_meta(e, fn) for e, fn in entities.items()]

incoming_df = spark.createDataFrame(
    incoming_meta,
    ["entity", "source_file", "file_size", "file_mod_time_ms"]
).select(
    "entity",
    "source_file",
    "file_size",
    (F.col("file_mod_time_ms")/1000).cast("timestamp").alias("file_mod_time"),
    F.lit(run_date).alias("run_date"),
    F.current_timestamp().alias("ingested_at")
)

display(incoming_df)

# COMMAND ----------

manifest_df = spark.table("olist_meta.bronze_manifest")

to_process = (incoming_df.alias("i")
  .join(
      manifest_df.alias("m"),
      on=[
        F.col("i.entity") == F.col("m.entity"),
        F.col("i.source_file") == F.col("m.source_file"),
        F.col("i.file_size") == F.col("m.file_size"),
        F.col("i.file_mod_time") == F.col("m.file_mod_time"),
      ],
      how="left_anti"
  )
)

display(to_process)

# COMMAND ----------

def read_csv_as_raw_strings(path: str):
    df = (spark.read
          .option("header", "true")
          .option("quote", "\"")
          .option("escape", "\"")
          .option("multiLine", "true")
          .csv(path))
    return df.select(*[F.col(c).cast("string").alias(c.strip().lower()) for c in df.columns])

# COMMAND ----------

rows = to_process.select("entity", "source_file", "run_date").collect()

if len(rows) == 0:
    print("[SKIP] No hay archivos nuevos para procesar.")
else:
    for r in rows:
        entity = r["entity"]
        src = r["source_file"]
        rd = r["run_date"]

        df = read_csv_as_raw_strings(src).select(
            "*",
            F.lit(rd).alias("run_date"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(src).alias("source_file")
        )

        out_path = f"{bronze_base}/{entity}/run_date={rd}"
        df.write.mode("append").parquet(out_path)

        print(f"[WROTE] {entity} -> {out_path}")

# COMMAND ----------

if len(rows) > 0:
    (to_process
      .select("entity","source_file","file_size","file_mod_time","run_date","ingested_at")
      .write
      .format("delta")
      .mode("append")
      .saveAsTable("olist_meta.bronze_manifest"))
    print("[OK] Manifest actualizado.")
else:
    print("[SKIP] Nada nuevo que registrar en manifest.")
