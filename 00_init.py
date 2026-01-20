# Databricks notebook source
dbutils.widgets.text("path","")
base_path = dbutils.widgets.get("path")

# COMMAND ----------

paths = {
  "landing": f"{base_path}/landing",
  "bronze": f"{base_path}/bronze",
  "silver": f"{base_path}/silver",
  "gold":   f"{base_path}/gold",
  "chk":    f"{base_path}/_checkpoints",
  "meta":   f"{base_path}/_meta"
}

for k,v in paths.items():
    dbutils.fs.mkdirs(v)

paths
