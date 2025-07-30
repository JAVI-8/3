from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, round as spark_round, when
from pyspark.sql.types import FloatType
from scoring_utils import calcular_penalty_score, calcular_performance_score

# Cargar CSV unificados
base_path = "data/limpios"

float_columns = [
        'Born', 'MP', 'Starts', 'Min', 'Gls', 'Ast', 'YellowC', 'RedC', 'PrgC', 'PrgP', 'PrgR',
        'Fls', 'Off', 'Crosses', 'Recov', 'Aerialwon%', 'Tack_Def_3rd', 'Tack_Mid_3rd',
        'Tack_Att_3rd', 'Tkl%', 'Tkl', 'TklW', 'Int', 'Blocks', 'Block_Shots', 'Bolck_Pass',
        'Clearences', 'Err', 'Pass_cmp', 'Pass_cmp%', 'Pass_cmp_Short%', 'Pass_cmp_Medium%',
        'Pass_cmp_Long%', 'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA', 'CrsPA',
        'Touch_Def_3rd', 'Touch_Mid_3rd', 'Touch_Att_3rd', 'Touch_Att_Pen', 'drib_Att',
        'drib_Succ%', 'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen', 'fail_To_Gain_Control',
        'Loss_Control_Tackle', 'Sh', 'SoT', 'G/Sh', 'xG', 'npxG', 'npxG/Sh', 'G-xG', 'Height', 'Value',
        'Pass_Medium', 'Pass_Long', 'Pass_Short', 'Touch_Live', 'Tckl_Drib%', 'Tckl_Drib'
]

position_metrics = {
    'Goalkeeper': [],
    'Centre-Back': [
        'Tkl', 'Int', 'Blocks', 'Clearences', 'Aerialwon%',
        'Touch_Def_3rd', 'Tack_Def_3rd', 'Pass_cmp%', 'PrgC', 'PrgP',
        'Tkl%', 'Block_Shots', 'Pass_cmp_Short%', 'Pass_Short',
        'Pass_cmp_Medium%', 'Pass_Medium', 'Pass_cmp_Long%', 'Pass_Long',
        'PrgDist'
    ],
    'Left-Back': [
        'PrgC', 'PrgP', 'Crosses', 'Recov', 'Tack_Def_3rd', 'Tkl%', 'Tkl', 'Int',
        'Block_Shots', 'Bolck_Pass', 'Clearences', 'Pass_cmp', 'Pass_cmp%',
        'Pass_cmp_Short%', 'Pass_Short', 'Pass_cmp_Medium%', 'Pass_Medium', 'xA',
        'A-xAG', 'CrsPA', 'Touch_Def_3rd', 'Touch_Att_3rd', 'drib_Att', 'drib_Succ%',
        'PrgDist', 'Carries_Att_3rd', 'Touch_Live'
    ],
    'Right-Back': [
        'PrgC', 'PrgP', 'Crosses', 'Recov', 'Tack_Def_3rd', 'Tkl%', 'Tkl', 'Int',
        'Block_Shots', 'Bolck_Pass', 'Clearences', 'Pass_cmp', 'Pass_cmp%',
        'Pass_cmp_Short%', 'Pass_Short', 'Pass_cmp_Medium%', 'Pass_Medium', 'xA',
        'A-xAG', 'CrsPA', 'Touch_Def_3rd', 'Touch_Att_3rd', 'drib_Att', 'drib_Succ%',
        'PrgDist', 'Carries_Att_3rd', 'Touch_Live'
    ],
    'Defensive Midfield': [
        'Tkl', 'Int', 'Blocks', 'Clearences', 'Aerialwon%', 'Touch_Def_3rd',
        'Touch_Mid_3rd', 'Tack_Def_3rd', 'Pass_cmp', 'Pass_cmp%', 'PrgC', 'PrgP',
        'Tkl%', 'Block_Shots', 'Pass_cmp_Short%', 'Pass_cmp_Long%',
        'Pass_cmp_Medium%', 'PrgDist', 'Recov',
        'Tack_Mid_3rd', 'Bolck_Pass', 'Touch_Live'
    ],
    'Central Midfield': [
        'Ast', 'PrgC', 'PrgP', 'Recov', 'Aerialwon%', 'Tack_Def_3rd', 'Tack_Mid_3rd',
        'Tack_Att_3rd', 'Tkl%', 'Tkl', 'Int', 'Bolck_Pass', 'Pass_cmp',
        'Pass_cmp%', 'Pass_cmp_Short%', 'Pass_Short', 'Pass_cmp_Medium%', 'Pass_Medium',
        'Pass_Long', 'Pass_cmp_Long%', 'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA',
        'Touch_Def_3rd', 'Touch_Mid_3rd', 'Touch_Att_3rd', 'Tckl_Drib', 'Tckl_Drib%',
        'PrgDist', 'Carries_Att_3rd', 'Touch_Live'
    ],
    'Attacking Midfield': [
        'Ast', 'PrgC', 'PrgP', 'PrgR', 'Tack_Att_3rd', 'Tack_Mid_3rd', 'Bolck_Pass',
        'Pass_cmp', 'Pass_cmp%', 'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium',
        'Pass_cmp_Medium%', 'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA',
        'Touch_Mid_3rd', 'Touch_Att_3rd', 'Touch_Att_Pen', 'drib_Att', 'drib_Succ%',
        'Tckl_Drib', 'Tckl_Drib%', 'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen',
        'Sh', 'SoT', 'G/Sh', 'Touch_Live'
    ],
    'Right Midfield': [
        'Gls', 'Ast', 'PrgC', 'PrgP', 'PrgR', 'Crosses', 'Tack_Mid_3rd', 'Tack_Att_3rd',
        'Tkl%', 'Tkl', 'Bolck_Pass', 'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium',
        'Pass_cmp_Medium%', 'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA', 'CrsPA',
        'Touch_Mid_3rd', 'Touch_Att_3rd', 'drib_Att', 'drib_Succ%', 'Tckl_Drib',
        'Tckl_Drib%', 'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen',
        'Sh', 'SoT', 'G/Sh'
    ],
    'Left Midfield': [
        'Gls', 'Ast', 'PrgC', 'PrgP', 'PrgR', 'Crosses', 'Tack_Mid_3rd', 'Tack_Att_3rd',
        'Tkl%', 'Tkl', 'Bolck_Pass', 'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium',
        'Pass_cmp_Medium%', 'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA', 'CrsPA',
        'Touch_Mid_3rd', 'Touch_Att_3rd', 'drib_Att', 'drib_Succ%', 'Tckl_Drib',
        'Tckl_Drib%', 'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen',
        'Sh', 'SoT', 'G/Sh'
    ],
    'Left Winger': [
        'Gls', 'Ast', 'PrgC', 'PrgP', 'PrgR', 'Crosses', 'Tack_Att_3rd', 'Tkl%', 'Tkl',
        'Bolck_Pass', 'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium', 'Pass_cmp_Medium%',
        'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA', 'CrsPA', 'Touch_Att_3rd',
        'Touch_Att_Pen', 'drib_Att', 'drib_Succ%', 'Tckl_Drib', 'Tckl_Drib%',
        'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen',
        'Sh', 'SoT', 'G/Sh', 'xG', 'npxG', 'npxG/Sh', 'G-xG'
    ],
    'Right Winger': [
        'Gls', 'Ast', 'PrgC', 'PrgP', 'PrgR', 'Crosses', 'Tack_Att_3rd', 'Tkl%', 'Tkl',
        'Bolck_Pass', 'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium', 'Pass_cmp_Medium%',
        'xAG', 'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'PPA', 'CrsPA', 'Touch_Att_3rd',
        'Touch_Att_Pen', 'drib_Att', 'drib_Succ%', 'Tckl_Drib', 'Tckl_Drib%',
        'PrgDist', 'Carries_Att_3rd', 'Carries_Att_Pen', 
        'Sh', 'SoT', 'G/Sh', 'xG', 'npxG', 'npxG/Sh', 'G-xG'
    ],
    'Second Striker': [
        'Gls', 'Ast', 'PrgC', 'PrgP', 'PrgR', 'Tack_Att_3rd', 'Bolck_Pass',
        'Pass_cmp_Short%', 'Pass_Medium', 'Pass_cmp_Medium%', 'xAG', 'xA', 'A-xAG',
        'Pass_cmp_Att_3rd', 'PPA', 'Touch_Mid_3rd', 'Touch_Att_3rd', 'Touch_Att_Pen',
        'drib_Att', 'drib_Succ%', 'Tckl_Drib', 'Tckl_Drib%', 'PrgDist',
        'Carries_Att_3rd', 'Carries_Att_Pen', 
        'Sh', 'SoT', 'G/Sh', 'xG', 'npxG', 'npxG/Sh', 'G-xG', 'Touch_Live'
    ],
    'Centre-Forward': [
        'Gls', 'Ast', 'PrgR', 'Aerialwon%', 'Tack_Att_3rd', 'Tkl%', 'Tkl', 'Bolck_Pass',
        'Pass_Short', 'Pass_cmp_Short%', 'Pass_Medium', 'Pass_cmp_Medium%', 'xAG',
        'xA', 'A-xAG', 'Pass_cmp_Att_3rd', 'Touch_Att_3rd', 'Touch_Att_Pen',
        'Carries_Att_Pen', 'drib_Att', 'drib_Succ%', 
        'Sh', 'SoT', 'G/Sh', 'xG', 'npxG', 'npxG/Sh', 'G-xG'
    ]
}
penalty_metrics = {
    'YellowC': 2.0,
    'RedC': 5.0,
    'Fls': 1.0,
    'Off': 1.5,  # solo si es atacante
    'Err': 2.0,
    'Loss_Control_Tackle': 1.0,
    'fail_To_Gain_Control': 0.5
}
def crear_sesion():
    # Crear sesión Spark
    spark = SparkSession.builder \
    .appName("scouting_pipeline") \
    .getOrCreate()
    return spark

def union_datasets(spark):
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
    return unido

def conversion_float(df):
    for col_name in float_columns:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))
    return df

#para que este en la misma escala base que el resto
def columnas_con_porcentaje(df):
    percent_columns = [col_name for col_name in float_columns if col_name.endswith('%')]

    for col_name in percent_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, (col(col_name) / 100).cast(FloatType()))

    return df

def guardar_en_parquet(df):
    df.write.mode("overwrite").parquet("data/final/merge_jugadores.parquet")
    print("Datos procesados y exportados en formato Parquet.")
    
def procesar():
    spark = crear_sesion()
    df = union_datasets(spark)
    df = conversion_float(df)
    df = columnas_con_porcentaje(df)
    df = calcular_penalty_score(df)
    df = calcular_performance_score(df, position_metrics)
    guardar_en_parquet(df)


procesar()