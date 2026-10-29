"""
Tarea 3 - Procesamiento batch con Apache Spark
Dataset: NYC 311 Service Requests (muestra 2024) almacenado en HDFS
"""
import os
from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = os.path.expanduser("~/tarea3/resultados")
os.makedirs(OUT_DIR, exist_ok=True)

spark = SparkSession.builder.appName("Tarea3_311_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

FILE_PATH = "hdfs://localhost:9000/Tarea3/311_2024.csv"

# 1. CARGA
raw = (spark.read
       .option("header", "true")
       .option("multiLine", "true")
       .option("escape", '"')
       .csv(FILE_PATH))

print("\n=== ESQUEMA ORIGINAL ===")
raw.printSchema()
print("Registros cargados:", raw.count())
raw.show(5, truncate=False)

# 2. LIMPIEZA Y TRANSFORMACION
BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

df = (raw
      .dropDuplicates(["unique_key"])
      .withColumn("created_date", F.to_timestamp("created_date"))
      .withColumn("closed_date", F.to_timestamp("closed_date"))
      .filter(F.col("created_date").isNotNull() & F.col("complaint_type").isNotNull())
      .withColumn("complaint_type", F.initcap(F.trim("complaint_type")))
      .withColumn("borough", F.upper(F.trim("borough")))
      .withColumn("borough", F.when(F.col("borough").isin(BOROUGHS), F.col("borough"))
                              .otherwise("NO ESPECIFICADO"))
      .withColumn("mes", F.date_format("created_date", "yyyy-MM"))
      .withColumn("horas_resolucion",
                  (F.unix_timestamp("closed_date") - F.unix_timestamp("created_date")) / 3600)
      )

print("\nRegistros tras limpieza:", df.count())

print("\n=== NULOS POR COLUMNA ===")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(truncate=False)

cerrados = df.filter(F.col("horas_resolucion").isNotNull() & (F.col("horas_resolucion") >= 0))
print("Registros con fecha de cierre valida:", cerrados.count())

# 3. EDA
print("\n=== ESTADISTICAS DEL TIEMPO DE RESOLUCION (horas) ===")
cerrados.select("horas_resolucion").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

top_tipos = (df.groupBy("complaint_type").count()
               .orderBy(F.desc("count")).limit(10))
print("=== TOP 10 TIPOS DE QUEJA ===")
top_tipos.show(truncate=False)

por_mes = df.groupBy("mes").count().orderBy("mes")
print("=== SOLICITUDES POR MES ===")
por_mes.show(12)

por_borough = df.groupBy("borough").count().orderBy(F.desc("count"))
print("=== SOLICITUDES POR BOROUGH ===")
por_borough.show()

por_agencia = (cerrados.groupBy("agency")
               .agg(F.count("*").alias("casos"),
                    F.round(F.avg("horas_resolucion"), 1).alias("horas_promedio"),
                    F.round(F.percentile_approx("horas_resolucion", 0.5), 1).alias("horas_mediana"))
               .filter(F.col("casos") >= 500)
               .orderBy(F.desc("horas_promedio")))
print("=== TIEMPO DE RESOLUCION POR ENTIDAD ===")
por_agencia.show(15, truncate=False)

print("=== EJEMPLO RDD: solicitudes por estado (map + reduceByKey) ===")
rdd_estado = (df.select("status").rdd
              .map(lambda fila: (fila["status"], 1))
              .reduceByKey(lambda a, b: a + b)
              .sortBy(lambda kv: -kv[1]))
for estado, n in rdd_estado.take(10):
    print(f"{estado}: {n}")

# 4. VISUALIZACION
pdf_top = top_tipos.toPandas().sort_values("count")
plt.figure(figsize=(10, 5))
plt.barh(pdf_top["complaint_type"], pdf_top["count"], color="#1f77b4")
plt.title("Top 10 tipos de solicitud 311 (muestra 2024)")
plt.xlabel("Numero de solicitudes")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/top_tipos.png", dpi=120)
plt.close()

pdf_mes = por_mes.toPandas()
plt.figure(figsize=(10, 4))
plt.plot(pdf_mes["mes"], pdf_mes["count"], marker="o")
plt.title("Solicitudes por mes (muestra 2024)")
plt.xticks(rotation=45)
plt.ylabel("Solicitudes")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/solicitudes_por_mes.png", dpi=120)
plt.close()

pdf_b = por_borough.toPandas()
plt.figure(figsize=(8, 4))
plt.bar(pdf_b["borough"], pdf_b["count"], color="#2ca02c")
plt.title("Solicitudes por borough")
plt.xticks(rotation=20)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/por_borough.png", dpi=120)
plt.close()

pdf_a = por_agencia.limit(10).toPandas().sort_values("horas_promedio")
plt.figure(figsize=(9, 5))
plt.barh(pdf_a["agency"], pdf_a["horas_promedio"], color="#d62728")
plt.title("Horas promedio de resolucion por entidad")
plt.xlabel("Horas")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/resolucion_por_entidad.png", dpi=120)
plt.close()

pdf_top.to_csv(f"{OUT_DIR}/top_tipos.csv", index=False)
pdf_mes.to_csv(f"{OUT_DIR}/solicitudes_por_mes.csv", index=False)
pdf_b.to_csv(f"{OUT_DIR}/por_borough.csv", index=False)
pdf_a.to_csv(f"{OUT_DIR}/resolucion_por_entidad.csv", index=False)

print(f"\nGraficos y tablas guardados en: {OUT_DIR}")
spark.stop()
