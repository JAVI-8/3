from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, round as spark_round, when

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("scouting_pipeline") \
    .getOrCreate()

# Cargar CSV unificados
base_path = "data2/limpios"

stats = spark.read.option("header", True).csv(f"{base_path}/stats/stats.csv")
misc = spark.read.option("header", True).csv(f"{base_path}/misc/misc.csv")
defense = spark.read.option("header", True).csv(f"{base_path}/defense/defense.csv")
passing = spark.read.option("header", True).csv(f"{base_path}/passing/passing.csv")
possession = spark.read.option("header", True).csv(f"{base_path}/possession/possession.csv")
shooting = spark.read.option("header", True).csv(f"{base_path}/shooting/shooting.csv")
mercado = spark.read.option("header", True).csv(f"{base_path}/mercado/mercado.csv")

# Normalizar claves para join
for df_name in ["stats", "misc", "defense", "passing", "possession", "shooting", "mercado"]:
    df = locals()[df_name]
    df = df.withColumn("Player", lower(trim(col("Player")))) \
           .withColumn("Squad", lower(trim(col("Squad")))) \
               .withColumn("Competition", lower(trim(col("Competition"))))
    locals()[df_name] = df

# Join de todas las fuentes
unido = stats \
    .join(misc, ["Player", "Squad", "Season", "Competition"], "inner") \
        .join(defense, ["Player", "Squad", "Season", "Competition"], "inner") \
            .join(passing, ["Player", "Squad", "Season", "Competition"], "inner") \
                .join(possession, ["Player", "Squad", "Season", "Competition"], "inner") \
                    .join(shooting, ["Player", "Squad", "Season", "Competition"], "inner") \
                        .join(mercado, ["Player", "Squad", "Season", "Competition"], "inner")

# Convertir columnas necesarias a float
#columnas_num = ["Born", "asistencias", "minutos_jugados", "goles_esperados", "asistencias_esperadas", "goles_esperados_noPen", "valor_mercado_eur"]
#for c in columnas_num:
    #if c in unido.columns:
        #unido = unido.withColumn(c, col(c).cast("float"))

# Features derivados
#unido = unido.withColumn("xG_xA_por_90", spark_round((col("goles_esperados") + col("asistencias_esperadas")) / (col("minutos_jugados") / 90), 2))

#unido = unido.withColumn(
    #"eficiencia_gol",
    #when((col("goles_esperados") > 0), col("goles") / col("goles_esperados"))
    #.otherwise(None)
#)
#unido = unido.withColumn("asistencias_esperadas_por_90", spark_round(col("asistencias_esperadas") / (col("minutos_jugados") / 90), 2))

# Exportar como Parquet
unido.write.mode("overwrite").parquet("data2/final/merge_jugadores.parquet")

print("✅ Datos procesados y exportados en formato Parquet.")
