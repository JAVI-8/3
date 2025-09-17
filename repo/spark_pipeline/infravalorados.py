from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf
# Cargar CSV unificados
from pathlib import Path
from pyspark.sql import functions as F
import os

from pathlib import Path
import os

#lista de jugadores por posicion y temporada con mas adjusted_score que no estan dentro del percentil 50 mas caros
def comparar_jugadores(df, output):
    
    percentiles_df = df.groupBy("Season", "Pos").agg(
        F.expr("percentile_approx(adjusted_score, 0.75)").alias("p75_perf"),
        F.expr("percentile_approx(value, 0.5)").alias("p50_value")
    )

   
    df_with_thresholds = df.join(percentiles_df, on=["Season", "Pos"], how="left")

    
    undervalued_players = df_with_thresholds.filter(
        (F.col("adjusted_score") >= F.col("p75_perf")) &
        (F.col("value") < F.col("p50_value"))
    ).select("player", "Season", "Pos", "adjusted_score", "value")

    
    undervalued_players = undervalued_players.orderBy("Season", "Pos", F.col("adjusted_score").desc())
    
    guardar_top(undervalued_players, output)

#guarda el top
def guardar_top(df, output):
    
    dir = Path(output)
    dir.mkdir(parents=True, exist_ok=True)
    
    df.write.mode("overwrite").parquet(str(dir / "top.parquet"))
