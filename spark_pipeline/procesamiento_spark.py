from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, round, when
from pyspark.sql.types import FloatType
from pyspark import SparkConf
# Cargar CSV unificados
base_path = "data/limpios"
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
    # Crear sesión Spark
    conf = SparkConf().set("spark.driver.memory", "6g")
    spark = SparkSession.builder \
    .appName("scouting_pipeline") \
    .getOrCreate()
    return spark

def union_datasets(spark, dfs):
    sources = {
        "stats": spark.read.option("header", True).csv("data/v1/stats.csv"),
        "misc": spark.read.option("header", True).csv("data/v1/misc.csv"),
        "defense": spark.read.option("header", True).csv("data/v1/defense.csv"),
        "passing": spark.read.option("header", True).csv("data/v1/passing.csv"),
        "possession": spark.read.option("header", True).csv("data/v1/possession.csv"),
        "shooting": spark.read.option("header", True).csv("data/v1/shooting.csv"),
        "mercado": spark.read.option("header", True).csv("data/v1/mercado.csv"),
        "keepers": spark.read.option("header", True).csv("data/v1/keepers.csv"),
        "keepersadv": spark.read.option("header", True).csv("data/v1/keepersadv.csv")
    }
    unido = sources["stats"] \
            .join(sources["misc"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["defense"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["passing"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["possession"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["shooting"], ["Player", "Squad", "Season", "Liga"]) \
            .join(sources["keepers"], ["Player", "Squad", "Season", "Liga"], "left") \
            .join(sources["keepersadv"], ["Player", "Squad", "Season", "Liga"], "left")
            
    return unido

def guardar_en_parquet(df):
    output = "data/unidos/merge_jugadores.parquet"
    df.write.mode("overwrite").parquet(output)
    print("Datos procesados y exportados en formato Parquet.")
    return output

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


def procesar(dfs):
    spark = crear_sesion()
    try:
        print("Uniendo los datasets...")
        df = union_datasets(spark, dfs)
        print("normalizando las variables x 90min...")
        df = normalizar_por_90_min(df)
        #porcentajes de aierto y variable que indiquen volumen
        df = combinacion_variables(df)
        print("Guardando...")
        return guardar_en_parquet(df)

        
    finally:
        spark.stop()