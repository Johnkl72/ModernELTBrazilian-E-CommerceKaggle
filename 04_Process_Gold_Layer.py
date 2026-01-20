# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Configuración
SILVER_DB = "olist_silver"
GOLD_DB = "olist_gold"

# Setup Inicial
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")

print("✅ Función upsert_to_gold lista para producción.")

# COMMAND ----------

def upsert_to_gold(df_source, table_name, merge_keys, partition_col=None):
    """
    Realiza un MERGE (SCD Tipo 1) de manera genérica.
    - Si la tabla no existe -> La crea.
    - Si la tabla existe -> Actualiza registros existentes e inserta nuevos.
    """
    full_table_name = f"{GOLD_DB}.{table_name}"
    
    df_dedup = df_source.dropDuplicates(merge_keys)

    # 2. Estrategia de Escritura
    if not spark.catalog.tableExists(full_table_name):
        print(f"🆕 Creando tabla (First Load): {full_table_name}")
        writer = df_dedup.write.format("delta").mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(full_table_name)
    
    else:
        print(f"🔄 Ejecutando Upsert (Merge) en: {full_table_name}")
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        # Construcción dinámica de la condición ON (t.id = s.id AND ...)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        (delta_table.alias("target")
            .merge(df_dedup.alias("source"), condition)
            .whenMatchedUpdateAll()   # Actualiza si cambia algún atributo
            .whenNotMatchedInsertAll() # Inserta si es nuevo
            .execute())

# COMMAND ----------

def build_dim_date(start='2016-01-01', end='2025-12-31'):
    print("📅 Generando Dimensión Tiempo...")
    
    # Generador de secuencia de días
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) as date_key")
    
    df_final = df.select(
        F.col("date_key"),
        F.year("date_key").alias("year"),
        F.month("date_key").alias("month"),
        F.dayofmonth("date_key").alias("day"),
        F.quarter("date_key").alias("quarter"),
        F.dayofweek("date_key").alias("day_of_week"),
        F.date_format("date_key", "EEEE").alias("day_name"),
        # KPIs Temporales
        F.when(F.dayofweek("date_key").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        F.when(F.month("date_key").isin(11, 12), True).otherwise(False).alias("is_holiday_season")
    )
    
    # Dim Date suele ser pequeña, overwrite es seguro y rápido
    df_final.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.dim_date")
    print("✅ dim_date actualizada.")

build_dim_date()

# COMMAND ----------

def build_master_dims():
    print("🏗️ Construyendo Dimensiones Maestras...")

    # --- DIM PRODUCT ---
    # Enriquecemos productos con reglas de negocio (volumen, clase)
    df_prod = spark.read.table(f"{SILVER_DB}.products").select(
        F.col("product_id").alias("product_key"), # PK
        F.col("product_category_name").alias("category"),
        "weight_g", "length_cm", "height_cm", "width_cm", "volume_cm3",
        "weight_class" # Columna creada en Silver
    )
    upsert_to_gold(df_prod, "dim_product", ["product_key"])

    customers = spark.read.table(f"{SILVER_DB}.customers")
    geo = spark.read.table(f"{SILVER_DB}.geolocation")
    
    # Join con Geo para obtener lat/lng del cliente
    df_cust = customers.join(
        geo, 
        customers.customer_zip_code_prefix == geo.geolocation_zip_code_prefix, 
        "left"
    ).select(
        F.col("customer_id").alias("customer_key"), # PK
        "customer_unique_id",
        "customer_city",
        "customer_state",
        F.col("lat").alias("customer_lat"),
        F.col("lng").alias("customer_lng")
    )
    upsert_to_gold(df_cust, "dim_customer", ["customer_key"])

    # --- DIM SELLER (Enriquecida con Geo) ---
    sellers = spark.read.table(f"{SILVER_DB}.sellers")
    
    df_seller = sellers.join(
        geo, 
        sellers.seller_zip_code_prefix == geo.geolocation_zip_code_prefix, 
        "left"
    ).select(
        F.col("seller_id").alias("seller_key"), # PK
        "seller_city", 
        "seller_state",
        F.col("lat").alias("seller_lat"),
        F.col("lng").alias("seller_lng")
    )
    upsert_to_gold(df_seller, "dim_seller", ["seller_key"])

build_master_dims()

# COMMAND ----------

def build_fact_sales():
    print("Construyendo Fact Sales (Incremental)...")
    
    orders = spark.read.table(f"{SILVER_DB}.orders")
    items = spark.read.table(f"{SILVER_DB}.order_items")
    
    df_fact = items.alias("i").join(orders.alias("o"), "order_id", "inner").select(
        F.col("o.order_id"),
        F.col("i.order_item_id"),
        F.col("o.customer_id").alias("customer_key"),
        F.col("i.product_id").alias("product_key"),
        F.col("i.seller_id").alias("seller_key"),
        F.col("o.order_purchase_date").alias("date_key"), 
        
        F.col("i.price").alias("sales_amount"),
        F.col("i.freight_value").alias("freight_amount"),
        (F.col("i.price") + F.col("i.freight_value")).alias("total_amount"),
        
        F.col("o.order_status"),
        
        F.when(F.col("i.price") > 500, True).otherwise(False).alias("is_high_ticket")
    )
    
    # Upsert usando PK compuesta
    upsert_to_gold(
        df_source=df_fact,
        table_name="fact_sales",
        merge_keys=["order_id", "order_item_id"],
        partition_col="date_key"
    )

build_fact_sales()

# COMMAND ----------

def build_fact_logistics():
    print("🚚 Construyendo Fact Logistics (Incremental)...")
    
    orders = spark.read.table(f"{SILVER_DB}.orders")
    
    df_logistics = orders.select(
        F.col("order_id"), # PK
        F.col("customer_id").alias("customer_key"),
        F.col("order_purchase_date").alias("date_key"),
        
        "purchase_ts", "approved_ts", "delivered_ts", "estimated_ts",
        "lead_time_days", "delay_days",
        
        F.when(F.col("order_status") == "canceled", "Canceled")
         .when(F.col("order_status") == "delivered", 
               F.when(F.col("delay_days") > 0, "Delivered Late").otherwise("Delivered OnTime"))
         .when(F.col("estimated_ts") < F.current_timestamp(), "Delayed In Transit")
         .otherwise("In Transit OnTime").alias("delivery_status_detail"),
         
        F.when(F.col("lead_time_days") <= 2, "Flash (<48h)")
         .when(F.col("lead_time_days") <= 7, "Standard (1w)")
         .otherwise("Slow (>1w)").alias("speed_class")
    )
    
    upsert_to_gold(
        df_source=df_logistics,
        table_name="fact_logistics",
        merge_keys=["order_id"],
        partition_col="date_key"
    )

build_fact_logistics()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.quarter,
# MAGIC     -- Formato de moneda para reporte
# MAGIC     FORMAT_NUMBER(SUM(f.total_amount), 2) as total_revenue_brl,
# MAGIC     COUNT(DISTINCT f.order_id) as total_orders
# MAGIC FROM olist_gold.fact_sales f
# MAGIC JOIN olist_gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1 DESC, 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     delivery_status_detail,
# MAGIC     speed_class,
# MAGIC     COUNT(order_id) as order_count,
# MAGIC     -- KPI: Promedio de días de retraso solo para los que llegaron tarde
# MAGIC     ROUND(AVG(CASE WHEN delay_days > 0 THEN delay_days ELSE NULL END), 1) as avg_delay_days
# MAGIC FROM olist_gold.fact_logistics
# MAGIC WHERE date_key >= '2017-01-01' -- Filtramos usando partición (rápido)
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     SUM(f.sales_amount) as gmv,
# MAGIC     ROUND(AVG(f.sales_amount), 2) as avg_ticket
# MAGIC FROM olist_gold.fact_sales f
# MAGIC JOIN olist_gold.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.customer_state,
# MAGIC     COUNT(DISTINCT f.order_id) as num_orders,
# MAGIC     FORMAT_NUMBER(SUM(f.total_amount), 2) as total_revenue
# MAGIC FROM olist_gold.fact_sales f
# MAGIC JOIN olist_gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC GROUP BY 1
# MAGIC ORDER BY 3 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.customer_city,
# MAGIC     f.total_amount,
# MAGIC     d.day_name
# MAGIC FROM olist_gold.fact_sales f
# MAGIC JOIN olist_gold.dim_date d ON f.date_key = d.date_key
# MAGIC JOIN olist_gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC JOIN olist_silver.orders o ON f.order_id = o.order_id
# MAGIC WHERE 
# MAGIC     f.is_high_ticket = true 
# MAGIC     AND d.is_weekend = true
# MAGIC ORDER BY f.total_amount DESC
# MAGIC LIMIT 20;
