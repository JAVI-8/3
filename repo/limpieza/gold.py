from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
import os, unicodedata, re

# ------------------- PATHS -------------------
SOFA_SILVER_PATH = r"C:/Users/jahoy/Documents/scouting/lake/silver/sofascore"
MERC_SILVER_PATH = r"C:/Users/jahoy/Documents/scouting/lake/silver/info"
UN_SILVER_PATH   = r"C:/Users/jahoy/Documents/scouting/lake/silver/understat"
STATS_PATH       = r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market"

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# ------------- Normalización robusta (UDF) -------------
def _normalize_name(s: str) -> str:
    if s is None:
        return None
    s = str(s).lower().strip()
    # quitar acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # dejar solo letras/numeros como tokens
    s = re.sub(r"[^a-z0-9]+", " ", s)
    # quitar stopwords típicas de nombres de club
    stop = {"fc","cf","club","sad","ac","sc","ud","cd","de","the","fk","sv","afc","cfc","bc"}
    s = " ".join(t for t in s.split() if t not in stop)
    s = re.sub(r"\s+", " ", s).strip()
    return s

normalize_name_udf = F.udf(_normalize_name, T.StringType())

def similarity_percent(col1, col2):
    dist = F.levenshtein(col1, col2)
    max_len = F.greatest(F.length(col1), F.length(col2))
    return F.when(max_len == 0, F.lit(0.0)).otherwise((1 - dist / max_len) * 100.0)

# ------------- MAPPING genérico: src -> tgt -------------
def build_mapping_src_to_tgt(df_src, df_tgt, umbral=85.0, w_player=0.7, w_team=0.3):
    """
    Crea un mapping para que los nombres del DF de destino (tgt) se reetiqueten
    a los nombres del DF fuente (src), emparejando por (liga, season) y similitud.
    Devuelve columnas: liga, season, src_player_name, src_team_name, tgt_player_name, tgt_team_name, score...
    """
    s = (df_src
         .withColumn("player_key", normalize_name_udf("player_name"))
         .withColumn("team_key",   normalize_name_udf("team_name"))
         .withColumn("liga_key",   normalize_name_udf("liga"))
         .withColumn("season_key", F.col("season"))
         .select("player_name","team_name","liga","season",
                 "player_key","team_key","liga_key","season_key")
         .dropDuplicates(["player_name","team_name","liga","season"])
    )

    t = (df_tgt
         .withColumn("player_key", normalize_name_udf("player_name"))
         .withColumn("team_key",   normalize_name_udf("team_name"))
         .withColumn("liga_key",   normalize_name_udf("liga"))
         .withColumn("season_key", F.col("season"))
         .select("player_name","team_name","liga","season",
                 "player_key","team_key","liga_key","season_key")
         .dropDuplicates(["player_name","team_name","liga","season"])
    )

    cand = (
        s.alias("s")
        .join(
            t.alias("t"),
            (F.col("s.liga_key") == F.col("t.liga_key")) &
            (F.col("s.season_key") == F.col("t.season_key")),
            "inner"
        )
        .withColumn("player_sim", similarity_percent(F.col("s.player_key"), F.col("t.player_key")))
        .withColumn("team_sim",   similarity_percent(F.col("s.team_key"),   F.col("t.team_key")))
        .withColumn("score", w_player*F.col("player_sim") + w_team*F.col("team_sim"))
        .filter(F.col("score") >= F.lit(umbral))
    )

    w = Window.partitionBy(
        "t.player_name","t.team_name","t.liga","t.season"
    ).orderBy(F.col("score").desc())

    best = (
        cand.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn")==1)
            .select(
                F.col("s.liga").alias("liga"),
                F.col("s.season").alias("season"),
                F.col("s.player_name").alias("src_player_name"),
                F.col("s.team_name").alias("src_team_name"),
                F.col("t.player_name").alias("tgt_player_name"),
                F.col("t.team_name").alias("tgt_team_name"),
                F.col("score"), F.col("player_sim"), F.col("team_sim")
            )
            .dropDuplicates()
    )
    return best

def apply_mapping_to_target(df_tgt, mapping):
    """
    Aplica el mapping (src->tgt) al DF destino para reetiquetar player_name y team_name
    a los nombres del DF fuente (src).
    """
    m = mapping.select(
        "liga","season",
        "src_player_name","src_team_name",
        "tgt_player_name","tgt_team_name"
    )

    out = (
        df_tgt.alias("t")
        .join(
            m.alias("m"),
            (F.col("t.liga") == F.col("m.liga")) &
            (F.col("t.season") == F.col("m.season")) &
            (F.col("t.player_name") == F.col("m.tgt_player_name")) &
            (F.col("t.team_name") == F.col("m.tgt_team_name")),
            "left"
        )
        .select(
            *[F.col("t."+c) for c in df_tgt.columns if c not in ("player_name","team_name")],
            F.coalesce(F.col("m.src_player_name"), F.col("t.player_name")).alias("player_name"),
            F.coalesce(F.col("m.src_team_name"),   F.col("t.team_name")).alias("team_name"),
        )
    )
    return out

# ------------- utilidades -------------
def iniciar_spark():
    return (
        SparkSession.builder
        .appName("build_gold_players_stats_market")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )

def comprobacion(df_mercado, df_sofascore, df_understat):
    required = ["player_name", "team_name", "season", "liga"]
    for col in required:
        if col not in df_sofascore.columns:
            raise ValueError(f"❌ Falta columna '{col}' en Sofascore Silver")
        if col not in df_mercado.columns:
            raise ValueError(f"❌ Falta columna '{col}' en Mercado Silver")
        if col not in df_understat.columns:
            raise ValueError(f"❌ Falta columna '{col}' en Understat Silver")
    if "market_value" not in df_mercado.columns:
        raise ValueError("❌ Mercado Silver debe tener 'market_value'")

def cargar_mercado(spark):   return spark.read.parquet(MERC_SILVER_PATH)
def cargar_sofascore(spark): return spark.read.parquet(SOFA_SILVER_PATH)
def cargar_understat(spark): return spark.read.parquet(UN_SILVER_PATH)

# ------------- MAIN -------------
def main():
    spark = iniciar_spark()

    df_mercado   = cargar_mercado(spark)
    df_sofascore = cargar_sofascore(spark)
    df_understat = cargar_understat(spark)

    print("▶️ Conteos iniciales")
    print("Mercado  :", df_mercado.count(),   "filas —", df_mercado.columns)
    print("Sofascore:", df_sofascore.count(), "filas —", df_sofascore.columns)
    print("Understat:", df_understat.count(), "filas —", df_understat.columns)

    comprobacion(df_mercado, df_sofascore, df_understat)

    # 1) Mapear Understat -> Sofascore (usamos Sofascore como canon)
    map_sofa_under = build_mapping_src_to_tgt(df_sofascore, df_understat, umbral=70.0)
    print("Pairs mapeados (Understat→Sofa):", map_sofa_under.count())
    # map_sofa_under.orderBy(F.col("score").desc()).show(20, truncate=False)

    df_under_unificado = apply_mapping_to_target(df_understat, map_sofa_under)

    # 2) Mapear Mercado -> Sofascore (mismo criterio)
    map_sofa_merc = build_mapping_src_to_tgt(df_sofascore, df_mercado, umbral=70.0)
    print("Pairs mapeados (Mercado→Sofa):", map_sofa_merc.count())

    df_mercado_unificado = apply_mapping_to_target(df_mercado, map_sofa_merc)

    # 3) Renombrar 'source' para evitar colisiones
    if "source" in df_sofascore.columns:
        df_sofascore = df_sofascore.withColumnRenamed("source", "source_sofascore")
    if "source" in df_under_unificado.columns:
        df_under_unificado = df_under_unificado.withColumnRenamed("source", "source_understat")
    if "source" in df_mercado_unificado.columns:
        df_mercado_unificado = df_mercado_unificado.withColumnRenamed("source", "source_info")

    # 4) JOINS exactos en las 4 claves
    keys = ["player_name","team_name","season","liga"]

    # deduplicar por seguridad
    df_sofascore = df_sofascore.dropDuplicates(keys)
    df_under_unificado = df_under_unificado.dropDuplicates(keys)
    df_mercado_unificado = df_mercado_unificado.dropDuplicates(keys)

    df_join1 = df_sofascore.join(df_under_unificado, on=keys, how="inner")
    print("Union sofascore+understat:", df_join1.count())
    df_final = df_join1.join(df_mercado_unificado, on=keys, how="inner")
    print("Union sofascore+understat+mercado:", df_final.count())
    # 5) Particiones y escritura
    df_final = df_final.withColumn("season_part", F.col("season")) \
                       .withColumn("liga_part",   F.col("liga"))

    print("➡️ Filas finales (match exacto 4 claves):", df_final.count())

    (df_final.write
        .mode("overwrite")
        .partitionBy("season_part", "liga_part")
        .parquet(STATS_PATH))

    print(f"✅ GOLD escrito en: {STATS_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
