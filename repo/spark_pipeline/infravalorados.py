from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf
# Cargar CSV unificados
from pathlib import Path
from pyspark.sql import functions as F
import os

from pathlib import Path
import os


def leer():
    spark = SparkSession.builder.appName("LeerParquet").getOrCreate()
    df = spark.read.parquet(r"data/performance.parquet")  
    comparar_jugadores(df)
#lista de jugadores por posicion y temporada con mas performance_score que no estan dentro del percentil 50 mas caros
def comparar_jugadores(df, output):
    # 1. Calcular percentiles por temporada y posición
    percentiles_df = df.groupBy("Season", "Pos").agg(
        F.expr("percentile_approx(adjusted_score, 0.75)").alias("p75_perf"),
        F.expr("percentile_approx(value, 0.5)").alias("p50_value")
    )

    # 2. Unir percentiles al DataFrame original
    df_with_thresholds = df.join(percentiles_df, on=["Season", "Pos"], how="left")

    # 3. Filtrar jugadores infravalorados (alto rendimiento y bajo valor de mercado)
    undervalued_players = df_with_thresholds.filter(
        (F.col("adjusted_score") >= F.col("p75_perf")) &
        (F.col("value") < F.col("p50_value"))
    ).select("player", "Season", "Pos", "adjusted_score", "value")

    # 4. (Opcional) Ordenar los resultados
    undervalued_players = undervalued_players.orderBy("Season", "Pos", F.col("performance_score").desc())
    
    guardar_top(undervalued_players, output)

def guardar_top(df, output):
    base = Path(r"work/parquets/latest")
    out_dir = base
    dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    
    df.write.mode("overwrite").parquetstr(dir / "top.parquet")
if __name__ == "__main__":
    leer()