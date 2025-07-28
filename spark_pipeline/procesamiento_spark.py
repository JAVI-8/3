from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, round as spark_round, when

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("scouting_pipeline") \
    .getOrCreate()

# Cargar CSV unificados
base_path = "data/limpios"

fbref = spark.read.option("header", True).csv(f"{base_path}/fbref/fbref.csv")
understat = spark.read.option("header", True).csv(f"{base_path}/understat/understat.csv")
defense = spark.read.option("header", True).csv(f"{base_path}/fbref_defense/fbref_defense.csv")
mercado = spark.read.option("header", True).csv(f"{base_path}/mercado/mercado.csv")

# Normalizar claves para join
for df_name in ["fbref", "understat", "defense", "mercado"]:
    df = locals()[df_name]
    df = df.withColumn("nombre", lower(trim(col("nombre")))) \
           .withColumn("club_actual", lower(trim(col("club_actual")))) \
               .withColumn("liga", lower(trim(col("liga"))))
    locals()[df_name] = df

# Join de todas las fuentes
unido = fbref \
    .join(understat, ["nombre", "club_actual", "temporada", "liga"], "inner") \
    .join(defense, ["nombre", "club_actual","temporada", "liga"], "inner") \
    .join(mercado, ["nombre", "club_actual","temporada", "liga"], "inner")

# Convertir columnas necesarias a float
columnas_num = ["goles", "asistencias", "minutos_jugados", "goles_esperados", "asistencias_esperadas", "goles_esperados_noPen", "valor_mercado_eur"]
for c in columnas_num:
    if c in unido.columns:
        unido = unido.withColumn(c, col(c).cast("float"))

# Features derivados
unido = unido.withColumn("xG_xA_por_90", spark_round((col("goles_esperados") + col("asistencias_esperadas")) / (col("minutos_jugados") / 90), 2))

unido = unido.withColumn(
    "eficiencia_gol",
    when((col("goles_esperados") > 0), col("goles") / col("goles_esperados"))
    .otherwise(None)
)
unido = unido.withColumn("asistencias_esperadas_por_90", spark_round(col("asistencias_esperadas") / (col("minutos_jugados") / 90), 2))

# Exportar como Parquet
unido.write.mode("overwrite").parquet("data/final2/merge_jugadores2.parquet")

print("✅ Datos procesados y exportados en formato Parquet.")
