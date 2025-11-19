from pyspark.sql import SparkSession, functions as F, types as T
import os
import re
from functools import reduce
import unicodedata
import re
from pyspark.sql.window import Window
# ------------------- PATHS -------------------

SOFA_SILVER_PATH = r"C:/Users/jahoy/Documents/scouting/lake/silver/sofascore"
MERC_SILVER_PATH = r"C:/Users/jahoy/Documents/scouting/lake/silver/mercado"
GOLD_PATH        = r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market"

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# --- normalización fuerte para nombres/equipos ---

def _normalize_name(s: str) -> str:
    """
    Normaliza nombres de jugadores y equipos:
      - minúsculas
      - quita acentos
      - quita caracteres raros
      - elimina stopwords típicas de nombres de club (fc, cf, club, etc.)
      - colapsa espacios
    """
    if s is None:
        return None

    s = str(s).lower().strip()

    # quitar acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # dejar solo letras/números como tokens
    s = re.sub(r"[^a-z0-9]+", " ", s)

    stopwords = {
        "fc", "cf", "club", "sad", "ac", "sc", "ud", "cd",
        "de", "the", "fk", "sv", "afc", "cfc", "bc"
    }
    tokens = [t for t in s.split() if t not in stopwords]

    s = " ".join(tokens)
    s = re.sub(r"\s+", " ", s).strip()
    return s

normalize_name_udf = F.udf(_normalize_name, T.StringType())


# --- similitud en % usando levenshtein ---

def similarity_percent(col1, col2):
    """
    Devuelve un Column con el % de similitud (0-100) entre dos columnas string
    usando levenshtein / longitud máxima.
    """
    dist = F.levenshtein(col1, col2)
    max_len = F.greatest(F.length(col1), F.length(col2))
    sim = (1 - dist / max_len) * 100.0
    # evitar división por cero
    return F.when(max_len == 0, F.lit(0.0)).otherwise(sim)
#funcion auxiliar--------------------------------------------------------------------------------

def igualar_sofascore_con_mercado(sofa_df,merc_df,umbral_similitud=80.0):
    """
    Cambia los nombres de Sofascore (player_name, team_name) por los de Mercado
    cuando:
      - Liga y Season coinciden EXACTAMENTE
      - la similitud (score combinado jugador+equipo) es >= umbral_similitud

    NO añade valores de mercado, solo corrige nombres de Sofascore.

    Parámetros:
      sofa_df: DataFrame Spark con columnas al menos:
               player_name, team_name, Liga, Season, ...
      merc_df: DataFrame Spark con columnas al menos:
               player_name, team_name, Liga, Season
      umbral_similitud: porcentaje mínimo de similitud (0-100) para aceptar el cambio.

    Devuelve:
      sofa_corrected: mismo esquema que sofa_df, pero con player_name y team_name
                      reemplazados por los de Mercado cuando hay match.
    """

    # 1) Normalizar nombres en ambos DF
    sofa_norm = (
        sofa_df
        .withColumn("player_key", normalize_name_udf("player_name"))
        .withColumn("team_key",   normalize_name_udf("team_name"))
    )

    merc_norm = (
        merc_df
        .withColumn("player_key", normalize_name_udf("player_name"))
        .withColumn("team_key",   normalize_name_udf("team_name"))
    )

    # Nos quedamos con lo necesario de Mercado (unique-ish)
    merc_small = (
        merc_norm
        .select(
            "Liga",
            "Season",
            "player_name",
            "team_name",
            "player_key",
            "team_key"
        )
        .dropDuplicates()
    )

    # 2) Generar candidatos solo dentro de misma Liga + Season (match exacto)
    candidates = (
        sofa_norm.alias("s")
        .join(
            merc_small.alias("m"),
            (F.col("s.Liga")   == F.col("m.Liga")) &
            (F.col("s.Season") == F.col("m.Season")),
            "inner"
        )
        # similitud en % para jugador y equipo
        .withColumn("player_sim", similarity_percent(F.col("s.player_key"), F.col("m.player_key")))
        .withColumn("team_sim",   similarity_percent(F.col("s.team_key"),   F.col("m.team_key")))
        # score combinado (ajustable)
        .withColumn("score", 0.7 * F.col("player_sim") + 0.3 * F.col("team_sim"))
        .filter(F.col("score") >= umbral_similitud)
    )

    # 3) Elegir el mejor candidato de Mercado para cada fila de Sofascore
    w = Window.partitionBy(
        "s.player_name", "s.team_name", "s.Liga", "s.Season"
    ).orderBy(F.col("score").desc())

    best_candidates = (
        candidates
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 4) Construir tabla de mapping (Sofascore -> Mercado)
    mapping = (
        best_candidates
        .select(
            F.col("s.Liga").alias("Liga"),
            F.col("s.Season").alias("Season"),
            F.col("s.player_name").alias("sofa_player_name"),
            F.col("s.team_name").alias("sofa_team_name"),
            F.col("m.player_name").alias("mercado_player_name"),
            F.col("m.team_name").alias("mercado_team_name"),
            F.col("score")
        )
        .dropDuplicates()
    )

    # 5) Aplicar mapping sobre el DF original de Sofascore
    #    → reemplazar player_name y team_name cuando hay match
    sofa_corrected = (
        sofa_df.alias("s")
        .join(
            mapping.alias("map"),
            (F.col("s.Liga")        == F.col("map.Liga")) &
            (F.col("s.Season")      == F.col("map.Season")) &
            (F.col("s.player_name") == F.col("map.sofa_player_name")) &
            (F.col("s.team_name")   == F.col("map.sofa_team_name")),
            "left"
        )
        .select(
            # todas las columnas originales salvo player_name/team_name...
            *[
                F.col("s." + c)
                for c in sofa_df.columns
                if c not in ("player_name", "team_name")
            ],
            # ...y estas 2 ya corregidas
            F.coalesce(F.col("map.mercado_player_name"), F.col("s.player_name")).alias("player_name"),
            F.coalesce(F.col("map.mercado_team_name"),   F.col("s.team_name")).alias("team_name"),
        )
    )

    return sofa_corrected
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def join_stats_market_and_write(sofa_df, merc_df):
    """
    LEFT JOIN desde Mercado hacia Sofascore:
      claves: player_name, team_name, Season, Liga

    ➜ Solo jugadores con valor de mercado (Mercado) aparecen en GOLD.
       Stats de Sofascore se añaden cuando existen; si no, quedan a NULL.

    Escribe un parquet GOLD sobrescribiendo en GOLD_PATH.
    """

    # 1) RENOMBRAR columnas 'source' de silver para que no choquen
    if "source" in merc_df.columns:
        merc_df = merc_df.withColumnRenamed("source", "source_market")
    if "source" in sofa_df.columns:
        sofa_df = sofa_df.withColumnRenamed("source", "source_stats")

    # 2) Aseguramos una fila por jugador+equipo+Liga+Season en Mercado
    merc_small = (
        merc_df
        .dropDuplicates(["player_name", "team_name", "Season", "Liga"])
    )
    print("➡️  Filas en Mercado:", merc_df.count())
    merc_df.select("player_name","team_name","Season","Liga").show(20, truncate=False)

    print("➡️  Filas únicas para join:", merc_small.count())

    # 3) LEFT JOIN: el lado izquierdo es Mercado
    gold_df = (
        merc_small
        .join(
            sofa_df,
            on=["player_name", "team_name", "Season", "Liga"],
            how="left"
        )
        .withColumn("created_at", F.current_timestamp())
    )

    # 4) Por si algún paso previo dejó otra 'source' colada, la eliminamos
    if "source" in gold_df.columns:
        gold_df = gold_df.drop("source")



    # 5) Escribir GOLD particionado
    (
        gold_df
        .write
        .mode("overwrite")
        .partitionBy("Season", "Liga")
        .parquet(GOLD_PATH)
    )
    print("➡️  Filas en GOLD antes de escribir:", gold_df.count())

    print(f"✅ GOLD creado (overwrite) en: {GOLD_PATH}")
    return gold_df


# ---------------------------------------------
#  INICIAR SPARK
# ---------------------------------------------

def iniciar_spark():
    return (
        SparkSession.builder
        .appName("build_gold_players_stats_market")
        .getOrCreate()
    )




# ---------------------------------------------
#  COMPROBACIÓN DE SCHEMAS
# ---------------------------------------------

def comprobacion(df_mercado, df_sofascore):
    required = ["player_name", "team_name", "Season", "Liga"]

    for col in required:
        if col not in df_sofascore.columns:
            raise ValueError(f"❌ Falta columna '{col}' en Sofascore Silver")
        if col not in df_mercado.columns:
            raise ValueError(f"❌ Falta columna '{col}' en Mercado Silver")

    if "player_market_value_euro" not in df_mercado.columns:
        raise ValueError("❌ Mercado Silver debe tener 'player_market_value_euro'")

# ---------------------------------------------
#  CARGA DE PARQUETS
# ---------------------------------------------

def cargar_mercado(spark):
    return spark.read.parquet(MERC_SILVER_PATH)

def cargar_sofascore(spark):
    return spark.read.parquet(SOFA_SILVER_PATH)

# ---------------------------------------------
#  MAIN PIPELINE
# ---------------------------------------------

def main():
    spark = iniciar_spark()

    df_mercado = cargar_mercado(spark)
    df_sofascore = cargar_sofascore(spark)

    comprobacion(df_mercado, df_sofascore)
    
    df_sofascore = igualar_sofascore_con_mercado(df_sofascore, df_mercado)
    


    df_gold = join_stats_market_and_write(df_sofascore, df_mercado)
    spark.stop()


if __name__ == "__main__":
    main()
