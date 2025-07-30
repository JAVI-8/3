from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from functools import reduce

# UDF para sumar valores de un vector (normalizado)
@udf(FloatType())
def sum_vector(vec):
    return float(sum(vec))

def calcular_penalty_score(df):
    
    return df.fillna(0, subset=[
        'YellowC', 'RedC', 'Fls', 'Off', 'Err', 'Loss_Control_Tackle', 'fail_To_Gain_Control'
    ]).withColumn("penalty_score",
        col("YellowC") * 2.0 +
        col("RedC") * 5.0 +
        col("Fls") * 1.0 +
        col("Err") * 2.0 +
        col("Loss_Control_Tackle") * 1.0 +
        col("fail_To_Gain_Control") * 0.5 +
        when(
            col("Pos").rlike("Forward|Winger|Striker|Attacking Midfield|Second Striker"),
            col("Off") * 1.5
        ).otherwise(0)
    )

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
