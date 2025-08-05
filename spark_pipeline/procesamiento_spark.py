from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, round, when
from pyspark.sql.types import FloatType
from pyspark import SparkConf
# Cargar CSV unificados
base_path = "data/limpios"

float_columns = [
        "Born", "MP", "Starts", "Min", "Gls", "Ast", "YellowC", "RedC", "PrgC", "PrgP", "PrgR",
        "Fls", "Off", "Crosses", "Recov", "Aerialwon%", "Tack_Def_3rd", "Tack_Mid_3rd",
        "Tack_Att_3rd", "Tkl%", "Tkl", "TklW", "Int", "Blocks", "Block_Shots", "Block_Pass",
        "Clearences", "Err", "Pass_cmp", "Pass_cmp%", "Pass_cmp_Short%", "Pass_cmp_Medium%",
        "Pass_cmp_Long%", "xAG", "xA", "A-xAG", "Pass_cmp_Att_3rd", "PPA", "CrsPA",
        "Touch_Def_3rd", "Touch_Mid_3rd", "Touch_Att_3rd", "Touch_Att_Pen", "drib_Att",
        "drib_Succ%", "PrgDist", "Carries_Att_3rd", "Carries_Att_Pen", "fail_To_Gain_Control",
        "Loss_Control_Tackle", "Sh", "SoT", "G/Sh", "xG", "npxG", "npxG/Sh", "G-xG", "Height", "Value",
        "Pass_Medium", "Pass_Long", "Pass_Short", "Touch_Live", "Tckl_Drib%", "Tckl_Drib", "GA", 
        "SoTA", "Save%", "CS%", "PKatt", "P_Save%", "PSxG","PSxG/SoT", "PSxG+/-", "Launch%", 
        "Crosses_Opp", "Crosses_stp%", "#OPA"
]
penalty_weights = {
        'YellowC': 1.0,
        'RedC': 2.5,
        'Fls': 0.2,
        'Err': 1.0,
        'Loss_Control_Tackle': 0.5,
        'fail_To_Gain_Control': 0.3,
        'Off': 0.5  # solo para ofensivos
    }
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

def union_datasets(spark):
    sources = {
        "stats": spark.read.option("header", True).csv("data/limpios/stats/stats.csv"),
        "misc": spark.read.option("header", True).csv("data/limpios/misc/misc.csv"),
        "defense": spark.read.option("header", True).csv("data/limpios/defense/defense.csv"),
        "passing": spark.read.option("header", True).csv("data/limpios/passing/passing.csv"),
        "possession": spark.read.option("header", True).csv("data/limpios/possession/possession.csv"),
        "shooting": spark.read.option("header", True).csv("data/limpios/shooting/shooting.csv"),
        "mercado": spark.read.option("header", True).csv("data/limpios/mercado/mercado.csv"),
        "keepers": spark.read.option("header", True).csv("data/limpios/keepers/keepers.csv"),
        "keepersadv": spark.read.option("header", True).csv("data/limpios/keepersadv/keepersadv.csv")
    }

    for name in sources:
        sources[name] = sources[name].withColumn("Player", lower(trim(col("Player")))) \
                                     .withColumn("Squad", lower(trim(col("Squad")))) \
                                     .withColumn("Competition", lower(trim(col("Competition"))))

    unido = sources["stats"] \
        .join(sources["misc"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["defense"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["passing"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["possession"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["shooting"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["mercado"], ["Player", "Squad", "Season", "Competition"]) \
        .join(sources["keepers"], ["Player", "Squad", "Season", "Competition"], "left") \
        .join(sources["keepersadv"], ["Player", "Squad", "Season", "Competition"], "left")

    return unido


def conversion_float(df):
    for col_name in float_columns:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))
    return df

#para que este en la misma escala base que el resto
def columnas_con_porcentaje(df):
    percent_columns = [col_name for col_name in float_columns if col_name.endswith("%")]

    for col_name in percent_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, (col(col_name) / 100).cast(FloatType()))

    return df

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

def guardar_en_parquet(df):
    df.write.mode("overwrite").parquet("data/unidos/merge_jugadores.parquet")
    print("Datos procesados y exportados en formato Parquet.")


def procesar():
    spark = crear_sesion()
    try:
        print("Uniendo los datasets...")
        df = union_datasets(spark)
        
        print("convertir a float...")
        df = conversion_float(df)
        
        print("Uniendo los datasets...")
        df = columnas_con_porcentaje(df)
        
        print("normalizando las variables x 90min...")
        df = normalizar_por_90_min(df)
        
        df = combinacion_variables(df)
        
        print("guardando unidos...")
        guardar_en_parquet(df)
    finally:
        spark.stop()


procesar()