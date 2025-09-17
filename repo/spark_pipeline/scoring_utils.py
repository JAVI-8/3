from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.types import  FloatType
from pyspark.ml.functions import vector_to_array
import builtins
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F


from pathlib import Path
from spark_pipeline.infravalorados import comparar_jugadores

WORK_DIR = Path("/opt/airflow/work")
WORK_DIR.mkdir(parents=True, exist_ok=True)

#pesos de las variables por posicion
position_metrics = {
    
    "Goalkeeper": {"Recov_per90": 0.3, "Clearences_per90": 0.2, "Pass_cmp%": 0.2, "GA_per90": 0.4, "SoTA_per90": 0.4, "Save%": 0.6, "CS%": 0.3, "Effective_Penalty_Saves": 0.2,
        "PSxG/SoT_per90": 0.6, "PSxG_per90": 0.6, "PSxG+/-_per90": 0.5, "Launch%": 0.2, "Effective_Cross_stop": 0.5, "#OPA_per90": 0.4
    },
    "Centre-Back": {
        "Int_per90": 0.5, "Blocks_per90": 0.3, "Clearences_per90": 0.5, "Aerialwon%": 0.6,
        "Touch_Def_3rd_per90": 0.3, "Tack_Def_3rd_per90": 0.5, "PrgC_per90": 0.1, "PrgP_per90": 0.2, "Effective_Pass_Short": 0.2,
        "Effective_Pass_Medium": 0.2, "Effective_Pass_Long": 0.3,
        "Effective_Tkl": 0.5, "Block_Shots_per90": 0.5, "Effective_Tackles_vsDrib": 0.3,
        "PrgDist_per90": 0.1
    },
    "Left-Back":{ 
        "PrgC_per90": 0.3, "PrgP_per90": 0.2, "Crosses_per90": 0.4, "Recov_per90": 0.4, "Tack_Def_3rd_per90": 0.4, "Effective_Tkl": 0.5, "Int_per90": 0.5,
        "Block_Shots_per90": 0.4, "Block_Pass_per90": 0.4, "Clearences_per90": 0.5, "Effective_Pass_Short": 0.2,
        "Effective_Pass_Medium": 0.3, "Effective_Pass_Long": 0.1, "xA_per90": 0.2,
        "A-xAG_per90": 0.2, "CrsPA_per90": 0.3, "Touch_Def_3rd_per90": 0.3, "Touch_Att_3rd_per90": 0.4, "Effective_Drib": 0.2,
        "PrgDist_per90": 0.4, "Carries_Att_3rd_per90": 0.2, "Touch_Live_per90": 0.3, "Effective_Tackles_vsDrib": 0.5
    },
    "Right-Back": {
        "PrgC_per90": 0.3, "PrgP_per90": 0.2, "Crosses_per90": 0.4, "Recov_per90": 0.4, "Tack_Def_3rd_per90": 0.4, "Effective_Tkl": 0.5, "Int_per90": 0.5,
        "Block_Shots_per90": 0.4, "Block_Pass_per90": 0.4, "Clearences_per90": 0.5, "Effective_Pass_Short": 0.2,
        "Effective_Pass_Medium": 0.3, "Effective_Pass_Long": 0.1, "xA_per90": 0.2,
        "A-xAG_per90": 0.2, "CrsPA_per90": 0.3, "Touch_Def_3rd_per90": 0.3, "Touch_Att_3rd_per90": 0.4, "Effective_Drib": 0.2,
        "PrgDist_per90": 0.4, "Carries_Att_3rd_per90": 0.2, "Touch_Live_per90": 0.3, "Effective_Tackles_vsDrib": 0.5
    },
    "Defensive Midfield": {
        "Int_per90": 0.4, "Blocks_per90": 0.5, "Clearences_per90": 0.3, "Aerialwon%": 0.3, "Touch_Def_3rd_per90": 0.2,
        "Touch_Mid_3rd_per90": 0.4, "Tack_Def_3rd_per90": 0.4, "Effective_Pass_Short": 0.6,
        "Effective_Pass_Medium": 0.5, "Effective_Pass_Long": 0.5, "PrgC_per90": 0.2, "PrgP_per90": 0.3,
        "Effective_Tkl": 0.5, "Block_Shots_per90": 0.3,"PrgDist_per90": 0.2, "Recov_per90": 0.4,
        "Tack_Mid_3rd_per90": 0.3, "Touch_Live_per90": 0.4
   } ,
    "Central Midfield": {
        "Ast_per90": 0.2, "PrgC_per90": 0.3, "PrgP_per90": 0.4, "Recov_per90": 0.3, "Aerialwon%": 0.2, "Tack_Def_3rd_per90": 0.3, "Tack_Mid_3rd_per90": 0.4,
        "Tack_Att_3rd_per90": 0.4, "Effective_Tkl": 0.4, "Int_per90": 0.4, "Block_Pass_per90": 0.3,"Effective_Pass_Short": 0.6,
        "Effective_Pass_Medium": 0.6, "Effective_Pass_Long": 0.5, "xAG_per90":0.2, "xA": 0.2, "A-xAG_per90": 0.2, "Pass_cmp_Att_3rd_per90": 0.3, "PPA_per90": 0.4,
        "Touch_Def_3rd_per90": 0.3, "Touch_Mid_3rd_per90": 0.4, "Touch_Att_3rd_per90": 0.3, "Effective_Tackles_vsDrib": 0.2,
        "PrgDist_per90": 0.3, "Carries_Att_3rd_per90": 0.4, "Touch_Live_per90": 0.2, "Effective_Drib": 0.2
    },
    "Attacking Midfield": {
        "Ast_per90": 0.3, "PrgC_per90": 0.3, "PrgP_per90": 0.4, "PrgR_per90": 0.4, "Tack_Att_3rd_per90": 0.3, "Tack_Mid_3rd_per90": 0.2, "Block_Pass_per90": 0.2,
        "Effective_Pass_Short": 0.5, "Effective_Pass_Medium": 0.5, "Effective_Pass_Long": 0.3, "xAG_per90": 0.5, "xA_per90": 0.4, "A-xAG_per90":0.3, "Pass_cmp_Att_3rd_per90": 0.5, "PPA_per90": 0.4,
        "Touch_Mid_3rd": 0.3, "Touch_Att_3rd": 0.4, "Touch_Att_Pen": 0.2, "Effective_Drib": 0.3,
        "Effective_Tackles_vsDrib": 0.1, "PrgDist_per90": 0.2, "Carries_Att_3rd_per90": 0.3, "Carries_Att_Pen_per90": 0.4,
        "Sh_per90": 0.2, "SoT_per90": 0.3, "G/Sh_per90": 0.2, "Touch_Live_per90": 0.2, "Effective_Tkl": 0.2
    },
    "Right Midfield": {
        "Gls_per90": 0.1, "Ast_per90": 0.2, "PrgC_per90": 0.4, "PrgP_per90": 0.3, "PrgR_per90": 0.3, "Crosses_per90": 0.4, "Tack_Mid_3rd_per90": 0.3, "Tack_Att_3rd_per90": 0.3,
        "Effective_Tkl": 0.2, "Block_Pass_per90": 0.2, "Effective_Pass_Short": 0.4,
        "Effective_Pass_Medium": 0.4, "Effective_Pass_Long": 0.2, "xAG_per90": 0.3, "xA_per90": 0.3, "A-xAG_per90": 0.3, "Pass_cmp_Att_3rd_per90": 0.4, "PPA_per90": 0.5, "CrsPA_per90": 0.5,
        "Touch_Mid_3rd_per90": 0.3, "Touch_Att_3rd_per90": 0.4, "Effective_Drib": 0.5, "Effective_Tackles_vsDrib": 0.2, "PrgDist_per90": 0.4, "Carries_Att_3rd_per90": 0.5, "Carries_Att_Pen_per90": 0.4,
        "Sh_per90": 0.2, "SoT_per90": 0.2, "G/Sh_per90": 0.2
   } ,
    "Left Midfield": {
        "Gls_per90": 0.1, "Ast_per90": 0.2, "PrgC_per90": 0.4, "PrgP_per90": 0.3, "PrgR_per90": 0.3, "Crosses_per90": 0.4, "Tack_Mid_3rd_per90": 0.3, "Tack_Att_3rd_per90": 0.3,
        "Effective_Tkl": 0.2, "Block_Pass_per90": 0.2, "Effective_Pass_Short": 0.4,
        "Effective_Pass_Medium": 0.4, "Effective_Pass_Long": 0.2, "xAG_per90": 0.3, "xA_per90": 0.3, "A-xAG_per90": 0.3, "Pass_cmp_Att_3rd_per90": 0.4, "PPA_per90": 0.5, "CrsPA_per90": 0.5,
        "Touch_Mid_3rd_per90": 0.3, "Touch_Att_3rd_per90": 0.4, "Effective_Drib": 0.5, "Effective_Tackles_vsDrib": 0.2, "PrgDist_per90": 0.4, "Carries_Att_3rd_per90": 0.5, "Carries_Att_Pen_per90": 0.4,
        "Sh_per90": 0.2, "SoT_per90": 0.2, "G/Sh_per90": 0.2
    },
    "Left Winger": {
        "Gls_per90": 0.2, "Ast_per90": 0.3, "PrgC_per90": 0.5, "PrgP_per90": 0.4, "PrgR_per90": 0.4, "Crosses_per90": 0.3, "Tack_Att_3rd_per90": 0.4, "Effective_Tkl": 0.2,
        "Block_Pass_per90": 0.1, "Effective_Pass_Short": 0.3,
        "Effective_Pass_Medium": 0.3, "Effective_Pass_Long": 0.2, "PPA_per90": 0.5, "CrsPA_per90": 0.6, "Touch_Att_3rd_per90": 0.5,
        "Touch_Att_Pen_per90": 0.5, "Effective_Drib": 0.6, "Effective_Tackles_vsDrib": 0.1,
        "PrgDist_per90": 0.3, "Carries_Att_3rd_per90": 0.4, "Carries_Att_Pen_per90": 0.5, "xAG_per90": 0.3, "xA_per90": 0.3,
        "Sh_per90": 0.4, "SoT_per90": 0.3, "G/Sh_per90": 0.3, "xG_per90": 0.5, "npxG_per90": 0.3, "G-xG_per90": 0.3
    },
    "Right Winger": {
        "Gls_per90": 0.2, "Ast_per90": 0.3, "PrgC_per90": 0.5, "PrgP_per90": 0.4, "PrgR_per90": 0.4, "Crosses_per90": 0.3, "Tack_Att_3rd_per90": 0.4, "Effective_Tkl": 0.2,
        "Block_Pass_per90": 0.1, "Effective_Pass_Short": 0.3,
        "Effective_Pass_Medium": 0.3, "Effective_Pass_Long": 0.2, "PPA_per90": 0.5, "CrsPA_per90": 0.6, "Touch_Att_3rd_per90": 0.5,
        "Touch_Att_Pen_per90": 0.5, "Effective_Drib": 0.6, "Effective_Tackles_vsDrib": 0.1,
        "PrgDist_per90": 0.3, "Carries_Att_3rd_per90": 0.4, "Carries_Att_Pen_per90": 0.5, "xAG_per90": 0.3, "xA_per90": 0.3,
        "Sh_per90": 0.4, "SoT_per90": 0.3, "G/Sh_per90": 0.3, "xG_per90": 0.5, "npxG_per90": 0.3, "G-xG_per90": 0.3
    },
    "Second Striker": {
        "Gls_per90": 0.3, "Ast_per90": 0.3, "PrgC_per90": 0.4, "PrgP_per90": 0.5, "PrgR_per90": 0.6, "Tack_Att_3rd_per90": 0.4, "Block_Pass_per90": 0.1,
        "Effective_Pass_Short": 0.4,
        "Effective_Pass_Medium": 0.3, "Effective_Pass_Long": 0.2, "xAG_per90": 0.5, "xA_per90": 0.4,
        "Pass_cmp_Att_3rd_per90": 0.4, "PPA_per90": 0.5, "Touch_Mid_3rd_per90": 0.3, "Touch_Att_3rd_per90": 0.6, "Touch_Att_Pen_per90": 0.5,
        "Effective_Drib":0.4, "Effective_Tackles_vsDrib": 0.1, "PrgDist_per90": 0.3,
        "Carries_Att_3rd_per90": 0.3, "Carries_Att_Pen_per90": 0.4,
        "Sh_per90": 0.3, "SoT_per90": 0.6, "G/Sh_per90": 0.5, "xG_per90": 0.6, "npxG_per90": 0.4, "G-xG_per90": 0.5, "Touch_Live_per90": 0.2, "Effective_Tkl": 0.1
    },
    "Centre-Forward": {
        "Gls_per90":0.3, "Ast_per90":0.2, "PrgR_per90": 0.3, "Aerialwon%": 0.5, "Tack_Att_3rd_per90": 0.5, "Effective_Tkl": 0.4, "Block_Pass_per90": 0.2,
        "Effective_Pass_Short": 0.3,
        "Effective_Pass_Medium": 0.2, "Effective_Pass_Long": 0.1, "xAG_per90": 0.4,
        "xA_per90": 0.4, "Pass_cmp_Att_3rd_per90": 0.4, "Touch_Att_3rd_per90": 0.5, "Touch_Mid_3rd_per90": 0.2, "Touch_Att_Pen_per90": 0.6,
        "Carries_Att_Pen_per90": 0.5, "Effective_Drib": 0.4, 
        "Sh_per90": 0.5, "SoT_per90": 0.6, "G/Sh_per90": 0.5, "xG_per90": 0.7, "npxG_per90": 0.5, "G-xG_per90": 0.4
    }
}

def cast_all_numeric_to_double(df):
    exclude=("Player","Squad","Season","Liga","Pos")
    for c in df.columns:
        if c not in exclude:
            df = df.withColumn(c, F.col(c).cast("double"))
        else:
            df = df.withColumn(c, F.col(c).cast("string"))
    return df


def normalize_weights_by_position():
    normalized = {}
    for position, weights in position_metrics.items():
        total = sum(weights.values())
        if total == 0:
            raise ValueError(f"La posición '{position}' tiene una suma de pesos igual a cero.")
        normalized[position] = [
            (metric, builtins.round(weight / total, 4)) for metric, weight in weights.items()
        ]
    return normalized

@udf(FloatType())
def sum_vector(vec):
    return float(sum(vec))

def calcular_penalty_score(df):
    penalty_weights = {
        'YellowC_per90': 0.03,
        'RedC_per90': 0.1,
        'Fls_per90': 0.01,
        'Err_per90': 0.05,
        'Loss_Control_Tackle_per90': 0.02,
        'fail_To_Gain_Control_per90': 0.01,
        'Off_per90': 0.02  # solo para ofensivos
    }

    created_penalty_cols = []

    for metric, weight in penalty_weights.items():
        col_name = f"penalty_{metric}" if metric != "Off_per90" else "penalty_Off"

        if metric == 'Off_per90':
            df = df.withColumn(
                col_name,
                when(
                    col("Pos").rlike("Left Winger|Right Winger|Second Striker|Attacking Midfield|Centre-Forward"),
                    col(metric) * lit(weight)
                ).otherwise(0.0)
            )
        elif metric in df.columns:
            df = df.withColumn(col_name, col(metric) * lit(weight))

        created_penalty_cols.append(col_name)

    # Filtrar solo las que realmente existen
    penalty_cols = [c for c in created_penalty_cols if c in df.columns]

    df = df.fillna(0, subset=penalty_cols)
    df = df.withColumn("penalty_score", sum([col(c) for c in penalty_cols]))

    return df

def calcular_performance_score_con_pesos(df):
    def procesar_posicion_x_temporada(df, pos, metric_weight_tuples):
        available_columns = df.columns
        seasons = [row["Season"] for row in df.select("Season").distinct().collect()]
        df_pos_season_list = []

        for season in seasons:
            df_season = df.filter((col("Pos") == pos) & (col("Season") == season))

            metrics = [m for m, _ in metric_weight_tuples if m in available_columns]
            weights = [w for m, w in metric_weight_tuples if m in available_columns]
            metric_weight_tuples_filtered = list(zip(metrics, weights))

            if not metric_weight_tuples_filtered:
                continue

            df_season = df_season.fillna(0, metrics)

            assembler = VectorAssembler(inputCols=metrics, outputCol="features_vec")
            df_season = assembler.transform(df_season)

            scaler = MinMaxScaler(inputCol="features_vec", outputCol="scaled_features")
            scaler_model = scaler.fit(df_season)
            df_season = scaler_model.transform(df_season)

            #convertir el vector a array
            df_season = df_season.withColumn("scaled_array", vector_to_array("scaled_features"))

            # calcular columnas ponderadas
            weighted_cols = []
            for i, (metric, weight) in enumerate(metric_weight_tuples_filtered):
                weighted_col_name = f"{metric}_weighted"
                df_season = df_season.withColumn(weighted_col_name, col("scaled_array")[i] * lit(weight))
                weighted_cols.append(col(weighted_col_name))

            if weighted_cols:
                df_season = df_season.withColumn("performance_score", sum(weighted_cols))
            else:
                df_season = df_season.withColumn("performance_score", lit(0.0))

            # eliminar columnas temporales
            cols_to_drop = ["features_vec", "scaled_features", "scaled_array"] + \
                           [f"{metric}_weighted" for metric, _ in metric_weight_tuples_filtered]
            df_season = df_season.drop(*cols_to_drop)

            df_season = df_season.withColumn("Evaluated_Position", lit(pos))
            df_pos_season_list.append(df_season)

        return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_pos_season_list) if df_pos_season_list else df.limit(0)

    scored_dfs = []
    position_metrics_normalized = normalize_weights_by_position()
    for pos, metric_weight_tuples in position_metrics_normalized.items():
        if metric_weight_tuples:
            scored_df = procesar_posicion_x_temporada(df, pos, metric_weight_tuples)
            if scored_df.count() > 0:
                scored_dfs.append(scored_df)

    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), scored_dfs) if scored_dfs else df.limit(0)

def crear_sesion():
    conf = SparkConf().set("spark.driver.memory", "6g")
    spark = SparkSession.builder \
    .appName("performance_score") \
    .getOrCreate()
    return spark

def cargar_df(spark, input):
    df = spark.read.parquet(input)
    return df

def calcular_adjusted_score(df):
    df = df.withColumn("adjusted_score", col("performance_score") - col("penalty_score"))
    return df

def guardar2(df, output):
    
    metrics = ["Int_per90", "Blocks_per90", "Aerialwon%", "PrgP_per90",
        "Effective_Pass_Short", "Effective_Pass_Medium", "Effective_Pass_Long", "Effective_Tkl", "Crosses_per90", "PrgC_per90", "xA_per90", "CrsPA_per90", "Effective_Drib", "Recov_per90", "SoT_per90", "Carries_Att_Pen_per90"]

    df_norm = df

    #normalizar por min max
    for m in metrics:
        min_val = df.agg(F.min(F.col(m))).first()[0]
        max_val = df.agg(F.max(F.col(m))).first()[0]
            
         # Crea la columna normalizada
        df_norm = df_norm.withColumn(
            f"{m}_norm",
            F.when(F.lit(max_val) == F.lit(min_val), F.lit(0.5))  #evita la divion por 0
            .otherwise((F.col(m) - F.lit(min_val)) / (F.lit(max_val) - F.lit(min_val)))
        )

    #columnas a guardar
    extra = df_norm.select(
        "Player", "Squad", "Season", "Competition", "Pos", "Min", "Value", "Height",
        "performance_score", "penalty_score", "adjusted_score", "Evaluated_Position", "Int_per90_norm", "Blocks_per90_norm", "Aerialwon%_norm", "PrgP_per90_norm",
        "Effective_Pass_Short_norm", "Effective_Pass_Medium_norm", "Effective_Pass_Long_norm", "Effective_Tkl_norm", "Crosses_per90_norm", "PrgC_per90_norm", "xA_per90_norm", "CrsPA_per90_norm", "Effective_Drib_norm", "Recov_per90_norm", "SoT_per90_norm", "Carries_Att_Pen_per90_norm"
    )
    
    (extra).coalesce(1).write.mode("overwrite").parquet(output)
    
def guardar(df, output):

    dir = Path(output)
    extra = df.select(
        "Player", "Squad", "Season", "Liga", "Pos", "Min", "Value", "Height",
        "performance_score", "penalty_score", "adjusted_score"
    )
    (extra).coalesce(1).write.mode("overwrite").parquet(str(dir / "final.parquet"))

def calcular(input, output):
    spark = crear_sesion()
    try:
        print("cargando dataset...")
        print(input)
        df = cargar_df(spark, input)
        
        df = cast_all_numeric_to_double(df)
        
        print("calcular penalty_score...")
        df = calcular_penalty_score(df)
        
        print("calcular performance_score...")
        df = calcular_performance_score_con_pesos(df)
        
        print("calculando adjusted_score...")
        df = calcular_adjusted_score(df)
        
        print("calculando top...")
        comparar_jugadores(df, output)
        print("guardando performance_score...")
        guardar(df, output)
        
        print("el dataset se ha guardado correctamente en formato parquet")
    finally:
        spark.stop()
        
if __name__ == "__main__":
    from pathlib import Path
    WORK = Path("/opt/airflow/work")
    WORK.mkdir(parents=True, exist_ok=True)
    
    #inp = str(WORK / "players_clean.parquet")
    inp = "work/parquets/latest/players.parquet"
    out = "work/parquets/latest"
    print(inp)
    print(out)
    calcular(input=inp, output=out)