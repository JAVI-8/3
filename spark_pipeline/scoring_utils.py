from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from functools import reduce

# UDF para sumar valores de un vector (normalizado)
@udf(FloatType())
def sum_vector(vec):
    return float(sum(vec))

def calcular_penalty_score(df):
    
    # Pesos ajustados y razonables
    penalty_weights = {
        'YellowC': 1.0,
        'RedC': 2.5,
        'Fls': 0.2,
        'Err': 1.0,
        'Loss_Control_Tackle': 0.5,
        'fail_To_Gain_Control': 0.3,
        'Off': 0.5  # solo para ofensivos
    }

    for metric, weight in penalty_weights.items():
        if metric == 'Off':
            df = df.withColumn(
                "penalty_Off",
                when(
                    col("Pos").rlike("Left Winger|Right Winger|Second Striker|Attacking Midfield|Centre-Forward"),
                    when(col("Min") > 0, (col("Off") / col("Min")) * 90 * weight).otherwise(0)
                ).otherwise(0)
            )
        else:
            if metric in df.columns:
                df = df.withColumn(
                    f"penalty_{metric}",
                    when(col("Min") > 0, (col(metric) / col("Min")) * 90 * weight).otherwise(0)
                )

    # Suma de todas las penalizaciones parciales
    penalty_cols = [f"penalty_{k}" for k in penalty_weights.keys()]
    df = df.fillna(0, subset=penalty_cols)

    penalty_expr = sum([col(c) for c in penalty_cols])
    df = df.withColumn("penalty_score", penalty_expr)

    return df

def calcular_performance_score(df, position_metrics):

    def procesar_posicion_x_temporada(df, pos, metrics):
        # Obtener todas las temporadas
        seasons = [row["Season"] for row in df.select("Season").distinct().collect()]
        df_pos_season_list = []

        for season in seasons:
            df_season = df.filter((col("Pos") == pos) & (col("Season") == season)).fillna(0, subset=metrics)

            #combina todas las métricas en un solo vector numérico.
            assembler = VectorAssembler(inputCols=metrics, outputCol="features_vec")
            df_season = assembler.transform(df_season)

            # transforma cada métrica del vector entre 0 y 1, escalando en base a los valores mínimos y máximos de esa posición.
            scaler = MinMaxScaler(inputCol="features_vec", outputCol="scaled_features")
            scaler_model = scaler.fit(df_season)
            df_season = scaler_model.transform(df_season)

            #suma las variables escaladas para calcular el performance_score
            df_season = df_season.withColumn("performance_score", sum_vector("scaled_features")) \
                                 .drop("features_vec", "scaled_features") \
                                 .withColumn("Evaluated_Position", lit(pos))

            df_pos_season_list.append(df_season)
        #une los dataframes por temporada ordenandolo por nombre
        return reduce(lambda df1, df2: df1.unionByName(df2), df_pos_season_list)

    # Procesar todas las posiciones
    scored_dfs = []
    for pos, metrics in position_metrics.items():
        if metrics:  # solo si hay métricas para esa posición
            scored_dfs.append(procesar_posicion_x_temporada(df, pos, metrics))

    return reduce(lambda df1, df2: df1.unionByName(df2), scored_dfs)
