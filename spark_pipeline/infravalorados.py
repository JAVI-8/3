from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col, expr
from datetime import date
# Crear SparkSession
# --- 1. Preparación ---
# Ejemplo: métricas seleccionadas

def calcular_edad(df): 
    df = df.withColumn("Edad", date.today().year - col("Born"))
    return df

def train_models_by_position(df, position_metrics, market_col="market_value_scaled"):
    results = []
    df = df.withColumn("market_value_scaled", col("Value") / 1000000)#escalar value para no romper la escala del modelo
    df_con_edad = calcular_edad(df)
    df_lim_min = df_con_edad.filter(col("Min") > 600)
    for posicion, metrics in position_metrics.items():
        print(f"Entrenando modelo para: {posicion}")

        # Filtrar por posición
        df_pos = df_lim_min.filter(col("Pos") == posicion).dropna(subset=metrics + [market_col] + ["Edad"])

        # Casteo a double por seguridad
        for colname in metrics + [market_col] + ["Edad"]:
            df_pos = df_pos.withColumn(colname, col(colname).cast("double"))

        # VectorAssembler
        assembler = VectorAssembler(inputCols=metrics+ ["Edad"], outputCol="features")
        df_feat = assembler.transform(df_pos)

        # Modelo de regresión
        lr = LinearRegression(featuresCol="features", labelCol=market_col, predictionCol="predicted_value")
        model = lr.fit(df_feat)
        df_pred = model.transform(df_feat)

        # Añadir value_gap
        df_pred = df_pred.withColumn("value_gap", (col("predicted_value") - col(market_col))*1000000)
        df_pred = df_pred.withColumn("value_gap_pct", (col("value_gap") / col(market_col)) * 100)
        df_pred = df_pred.withColumn("modelo_posicion", expr(f"'{posicion}'"))
        df_pred = df_pred.withColumn("predicted_value_real", col("predicted_value") * 1_000_000)
        results.append(df_pred.select("Player", "Pos", market_col, "predicted_value_real", "predicted_value", "value_gap", "value_gap_pct", "modelo_posicion", *metrics))

    # Unir resultados de todas las posiciones
    df_final = results[0]
    for df_r in results[1:]:
        df_final = df_final.unionByName(df_r, allowMissingColumns= True)

    return df_final