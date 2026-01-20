# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Configuración de Rutas
BASE_PATH = "/Volumes/workspace/default/data/olist"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_DB = "olist_silver"

# 1. Crear Base de Datos
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")

spark.conf.set("spark.sql.shuffle.partitions", "8") 

print("✅ Entorno configurado y optimizado.")

# COMMAND ----------

def read_bronze(entity: str):
    """Lectura recursiva de Bronze."""
    path = f"{BRONZE_PATH}/{entity}"
    try:
        return spark.read.format("parquet").option("recursiveFileLookup", "true").load(path)
    except Exception:
        print(f"⚠️ Datos no encontrados para: {entity}")
        return None

def upsert_to_silver(df_source, entity_name, merge_keys):
    """Escritura Idempotente con Delta Merge."""
    if df_source is None: return

    table_name = f"{SILVER_DB}.{entity_name}"
    
    df_dedup = df_source.dropDuplicates(merge_keys)

    if not spark.catalog.tableExists(table_name):
        print(f"🔨 Creando tabla {table_name}...")
        df_dedup.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        print(f"🔄 Merge en {table_name}...")
        delta_table = DeltaTable.forName(spark, table_name)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        (delta_table.alias("target")
            .merge(df_dedup.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

# COMMAND ----------

def transform_orders(df):
    # Definimos columnas base casteadas para usar en expresiones subsiguientes
    # Nota: Para evitar repetir F.to_timestamp multiples veces, podemos usar un select intermedio
    # SOLO SI mejora la legibilidad, pero aquí haremos todo en una pasada limpia.
    
    return df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias("purchase_ts"),
        F.to_timestamp("order_approved_at").alias("approved_ts"),
        F.to_timestamp("order_delivered_carrier_date").alias("shipped_ts"),
        F.to_timestamp("order_delivered_customer_date").alias("delivered_ts"),
        F.to_timestamp("order_estimated_delivery_date").alias("estimated_ts"),
        
        # --- Lógica de Negocio (KPIs) ---
        
        # Lead Time: Diferencia entre entrega y compra
        F.round(
            F.datediff(
                F.to_timestamp("order_delivered_customer_date"), 
                F.to_timestamp("order_purchase_timestamp")
            ), 2
        ).alias("lead_time_days"),
        
        # Delay: Diferencia entre entrega y estimado
        F.datediff(
            F.to_timestamp("order_delivered_customer_date"), 
            F.to_timestamp("order_estimated_delivery_date")
        ).alias("delay_days"),
        
        # Flag: On Time Delivery
        F.when(
            F.to_timestamp("order_delivered_customer_date") <= F.to_timestamp("order_estimated_delivery_date"), 
            True
        ).otherwise(False).alias("is_on_time_delivery"),
        
        # Fecha particionable
        F.to_date("order_purchase_timestamp").alias("order_purchase_date")
    )

# COMMAND ----------

def transform_products(df):
    return df.select(
        F.col("product_id"),
        F.col("product_category_name"),
        F.col("product_name_lenght").cast("int"),
        F.col("product_description_lenght").cast("int"),
        F.col("product_photos_qty").cast("int"),
        
        # Limpieza de nulos + Casting
        F.coalesce(F.col("product_weight_g").cast("int"), F.lit(0)).alias("weight_g"),
        F.coalesce(F.col("product_length_cm").cast("int"), F.lit(0)).alias("length_cm"),
        F.coalesce(F.col("product_height_cm").cast("int"), F.lit(0)).alias("height_cm"),
        F.coalesce(F.col("product_width_cm").cast("int"), F.lit(0)).alias("width_cm"),
        
        # Cálculo de Volumen (Lógica de Negocio)
        (
            F.coalesce(F.col("product_length_cm").cast("int"), F.lit(0)) * F.coalesce(F.col("product_height_cm").cast("int"), F.lit(0)) * F.coalesce(F.col("product_width_cm").cast("int"), F.lit(0))
        ).cast("long").alias("volume_cm3"),
        
        # Clasificación (Case When)
        F.when(F.col("product_weight_g").cast("int") < 2000, "Light")
         .when(F.col("product_weight_g").cast("int") < 10000, "Medium")
         .otherwise("Heavy").alias("weight_class")
    )

# COMMAND ----------

def transform_reviews(df):
    return df.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int"),
        F.col("review_comment_title"),
        F.col("review_comment_message"),
        F.to_timestamp("review_creation_date").alias("created_at"),
        F.to_timestamp("review_answer_timestamp").alias("answered_at"),
        
        # Sentiment Flag
        F.when(F.col("review_score").cast("int") >= 4, "Positive")
         .when(F.col("review_score").cast("int") == 3, "Neutral")
         .otherwise("Negative").alias("sentiment"),
         
        # Binary Flag (mejor que checkear nulls luego)
        (F.length(F.col("review_comment_message")) > 1).alias("has_comment")
    )

def transform_payments(df):
    return df.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        F.col("payment_value").cast("double"),
        
        # Flag analítico
        (F.col("payment_installments") > 1).alias("is_installment")
    )
    
def transform_items(df):
    return df.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_ts"),
        F.col("price").cast("double"),
        F.col("freight_value").cast("double")
    )

# COMMAND ----------

def transform_geolocation(df):
    return df.groupBy("geolocation_zip_code_prefix").agg(
        F.first("geolocation_lat").alias("lat"),
        F.first("geolocation_lng").alias("lng"),
        F.first("geolocation_city").alias("city"),
        F.first("geolocation_state").alias("state")
    )

# COMMAND ----------

entities = {
    "geolocation":  (["geolocation_zip_code_prefix"], transform_geolocation),
    "orders":       (["order_id"], transform_orders),
    "order_items":  (["order_id", "order_item_id"], transform_items),
    "products":     (["product_id"], transform_products),
    "order_reviews":(["review_id"], transform_reviews),
    "order_payments":(["order_id", "payment_sequential"], transform_payments),
    "customers":    (["customer_id"], lambda df: df), # Passthrough
    "sellers":      (["seller_id"], lambda df: df)
}

print("🚀 Iniciando Pipeline Silver (Modo Optimizado)...\n")

for entity, (pks, transform_fn) in entities.items():
    print(f"▶️ Procesando: {entity}")
    
    df_bronze = read_bronze(entity)
    if df_bronze:
        df_silver = transform_fn(df_bronze)
        upsert_to_silver(df_silver, entity, pks)

print("\n✨ Carga Silver Completada.")
