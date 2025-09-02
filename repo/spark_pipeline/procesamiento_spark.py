from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when
from pyspark import SparkConf
# Cargar CSV unificados
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Rutas dentro del contenedor ---
DATA= Path("/opt/airflow/data/v1")
WORK_DIR = Path("/opt/airflow/v1")
WORK_DIR.mkdir(parents=True, exist_ok=True)

#varibles a normalizar
variables_por_90 = [
    "Gls", "Ast", "PrgC", "PrgP", "PrgR", "Fls", "Off", "Crosses", "Recov",
    "Tack_Def_3rd", "Tack_Mid_3rd", "Tack_Att_3rd", "Tkl", "TklW", "Int", "Blocks",
    "Block_Shots", "Block_Pass", "Clearences", "Err", "Pass_cmp", "Pass_Short",
    "Pass_Medium", "Pass_Long", "xAG", "xA", "A-xAG", "Pass_cmp_Att_3rd", "PPA",
    "CrsPA", "Touch_Def_3rd", "Touch_Mid_3rd", "Touch_Att_3rd", "Touch_Att_Pen",
    "Touch_Live", "drib_Att", "Tckl_Drib", "PrgDist", "Carries_Att_3rd",
    "Carries_Att_Pen", "fail_To_Gain_Control", "Loss_Control_Tackle",
    "Sh", "SoT", "xG", "npxG", "GA", "SoTA", "PKatt", "PSxG",
    "PSxG/SoT", "PSxG+/-", "Crosses_Opp", "#OPA", "YellowC", "RedC"
]
#volumen * porcentaje de exito
def combinacion_variables(df):
    df = df.withColumn("Effective_Pass_Short", col("Pass_Short") * col("Pass_cmp_Short%"))
    df = df.withColumn("Effective_Pass_Medium", col("Pass_Medium") * col("Pass_cmp_Medium%"))
    df = df.withColumn("Effective_Pass_Long", col("Pass_Long") * col("Pass_cmp_Long%"))
    df = df.withColumn("Effective_Tkl", col("TKL%") * col("TKL"))
    df = df.withColumn("Effective_Drib", col("drib_Att") * col("drib_Succ%"))
    df = df.withColumn("Effective_Tackles_vsDrib", col("Tckl_Drib") * col("Tckl_Drib%"))
    df = df.withColumn("Effective_Penalty_Saves", col("PKatt") * col("P_Save%"))
    df = df.withColumn("Effective_Cross_stop", col("Crosses_Opp") * col("Crosses_stp%"))
    return df


def crear_sesion():
    conf = (
        SparkConf()
        .setAppName("scouting_pipeline")
        .set("spark.driver.memory", "6g")
    )
    spark = (
        SparkSession.builder
        .config(conf=conf)
        .master("local[*]")
        .getOrCreate()
    )
    return spark

#iguala los nombres de los equipos de transfermarht a las de fbref ya que son distintos
from pyspark.sql import functions as F, Window

def map_squads_by_league(df_tm, df_fb, max_frac=0.5, max_abs=2):
    #unicos por liga y temporada
    tm = (df_tm.select("Liga", "Season", "Squad").distinct()
              .withColumnRenamed("Squad", "Squad_tm")
              .withColumnRenamed("Season", "Season_tm"))

    fb = (df_fb.select("Liga", "Season", "Squad").distinct()
              .withColumnRenamed("Squad", "Squad_fb")
              .withColumnRenamed("Season", "Season_fb"))

    #misma liga Y temporada
    cand = (tm.join(
                fb,
                (tm["Liga"] == fb["Liga"]) & (tm["Season_tm"] == fb["Season_fb"]),
                how="inner"
            )
            .withColumn("dist", F.levenshtein(F.col("Squad_tm"), F.col("Squad_fb")))
            .withColumn("len_tm", F.length("Squad_tm"))
            .withColumn(
                "max_dist",
                F.greatest(F.lit(max_abs),
                           (F.col("len_tm") * F.lit(max_frac)).cast("int"))
            )
            .filter(F.col("dist") <= F.col("max_dist"))
    )

    # Mejor match por (Liga, Season_tm, Squad_tm)
    w = Window.partitionBy("Liga", "Season_tm", "Squad_tm") \
              .orderBy(F.col("dist").asc(),
                       F.length("Squad_fb").asc(),
                       F.col("Squad_fb").asc())

    best = (cand.withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(
                    F.col("Liga"),
                    F.col("Season_tm").alias("Season"),
                    F.col("Squad_tm"),
                    F.col("Squad_fb"),
                    F.col("dist"))
    )

    # Join de vuelta y sobrescribe Squad (manteniendo el resto de columnas)
    m = df_tm.alias("m")
    b = best.alias("b")
    cond = ((F.col("m.Liga")   == F.col("b.Liga")) &
            (F.col("m.Season") == F.col("b.Season")) &
            (F.col("m.Squad")  == F.col("b.Squad_tm")))

    df_tm_mapeado = (m.join(b, cond, "left")
                       .select(
                           F.col("m.*"),
                           F.coalesce(F.col("b.Squad_fb"), F.col("m.Squad")).alias("Squad_new")
                       )
                       .drop("Squad")
                       .withColumnRenamed("Squad_new", "Squad"))

    return df_tm_mapeado

#une los diferentes df
def union_datasets(spark):
    #cargar los diferentes df
    sources = {
        "stats": spark.read.option("header", True).csv(str(DATA/ "stats.csv")),
        "misc": spark.read.option("header", True).csv(str(DATA/ "misc.csv")),
        "defense": spark.read.option("header", True).csv(str(DATA/ "defense.csv")),
        "passing": spark.read.option("header", True).csv(str(DATA/ "passing.csv")),
        "possession": spark.read.option("header", True).csv(str(DATA/ "possession.csv")),
        "shooting":spark.read.option("header", True).csv(str(DATA/ "shooting.csv")),
        "mercado": spark.read.option("header", True).csv(DATA / "mercado.csv"),
        "keepers": spark.read.option("header", True).csv(str(DATA/ "keepers.csv")),
        "keepersadv": spark.read.option("header", True).csv(str(DATA/ "keepersadv.csv"))
    }
    #unir
    unido = sources["stats"] \
            .join(sources["misc"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["defense"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["passing"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["possession"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["mercado"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["shooting"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["keepers"], ["Player", "Squad", "Season", "Liga"], "left") \
            .join(sources["keepersadv"], ["Player", "Squad", "Season", "Liga"], "left")
            
    return unido

#guarda todo el df de los datos unidos
def guardar_en_parquet(df):
    output = str(WORK_DIR / "players_clean.parquet")
    
    df.write.mode("overwrite").parquet(output)
    print("Datos procesados y exportados en formato Parquet.")
    return output
#normalizar por 90 min 
def normalizar_por_90_min(df):
    columnas_validas = [c for c in variables_por_90 if c in df.columns]
    for col_name in columnas_validas:
        df = df.withColumn(
            f"{col_name}_per90",
             round(
                when(col("Min") > 0, (col(col_name) / col("Min")) * 90).otherwise(0.0), 2
            )
        )
    return df

from pyspark.sql import functions as F

def cast_all_numeric_to_double(df):
    exclude=("Player","Squad","Season","Liga","Pos")
    
    for c in df.columns:
        if c not in exclude:
            df = df.withColumn(c, F.col(c).cast("double"))
        else:
            df = df.withColumn(c, F.col(c).cast("string"))
    return df


def procesar():
    spark = crear_sesion()
    try:
        
        
        print("Uniendo los datasets...")
        df = union_datasets(spark)
        
        print("casteando a double...")
        df = cast_all_numeric_to_double(df)

        print("normalizando las variables x 90min...")
        df = normalizar_por_90_min(df)

        df = combinacion_variables(df)
        print("Guardando...")
        return guardar_en_parquet(df)

        
    finally:
        spark.stop()
        

            
        