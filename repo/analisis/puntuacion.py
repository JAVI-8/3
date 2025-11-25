# position_metrics.py (versión corregida)
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

PATH_IN  = r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market"
PATH_OUT = r"C:/Users/jahoy/Documents/scouting/lake/gold/players_score"

# ===================== PESOS POSITIVOS (por posición) =====================
position_weights = {
    "Goalkeeper": {
        "saves": 0.20, "savedShotsFromInsideTheBox": 0.10, "savedShotsFromOutsideTheBox": 0.10,
        "highClaims": 0.10, "runsOut": 0.05, "successfulRunsOut": 0.05, "punches": 0.05,
        "accurateLongBalls": 0.10, "accurateLongBallsPercentage": 0.10, "accuratePassesPercentage": 0.15
    },
    "Centre-Back": {
        "clearances": 0.10, "interceptions": 0.10, "tackles": 0.10,
        "aerialDuelsWon": 0.05, "aerialDuelsWonPercentage": 0.10,
        "groundDuelsWon": 0.05, "groundDuelsWonPercentage": 0.05,
        "totalDuelsWon": 0.05, "totalDuelsWonPercentage": 0.05,
        "accuratePassesPercentage": 0.10, "accurateLongBalls": 0.05,
        "accurateLongBallsPercentage": 0.05, "accuratePasses": 0.05,
        "xGChain_per90": 0.02, "xGBuildup_per90": 0.02, "blockedShots": 0.06
    },
    "Right-Back": {
        "tackles": 0.07, "interceptions": 0.07, "groundDuelsWon": 0.05,
        "groundDuelsWonPercentage": 0.05, "totalDuelsWon": 0.05, "totalDuelsWonPercentage": 0.05,
        "aerialDuelsWon": 0.03, "aerialDuelsWonPercentage": 0.03,
        "accurateCrosses": 0.05, "accurateCrossesPercentage": 0.07,
        "keyPasses": 0.05, "accurateFinalThirdPasses": 0.05, "assists": 0.05,
        "bigChancesCreated": 0.04, "successfulDribbles": 0.03,
        "successfulDribblesPercentage": 0.03, "accuratePassesPercentage": 0.05,
        "accurateLongBalls": 0.03, "accurateLongBallsPercentage": 0.03,
        "accuratePasses": 0.03, "xGChain_per90": 0.02, "xGBuildup_per90": 0.02,
        "clearances": 0.02, "passToAssist": 0.03, "assist_ratio": 0.03
    },
    "Left-Back": {
        "tackles": 0.07, "interceptions": 0.07, "groundDuelsWon": 0.05,
        "groundDuelsWonPercentage": 0.05, "totalDuelsWon": 0.05, "totalDuelsWonPercentage": 0.05,
        "aerialDuelsWon": 0.03, "aerialDuelsWonPercentage": 0.03,
        "accurateCrosses": 0.05, "accurateCrossesPercentage": 0.07, "keyPasses": 0.05,
        "accurateFinalThirdPasses": 0.05, "assists": 0.05, "bigChancesCreated": 0.04,
        "successfulDribbles": 0.03, "successfulDribblesPercentage": 0.03,
        "accuratePassesPercentage": 0.05, "accurateLongBalls": 0.03,
        "accurateLongBallsPercentage": 0.03, "accuratePasses": 0.03,
        "xGChain_per90": 0.02, "xGBuildup_per90": 0.02, "clearances": 0.02,
        "passToAssist": 0.03, "wasFouled": 0.02, "assist_ratio": 0.03
    },
    "Defensive Midfield": {
        "clearances": 0.06, "interceptions": 0.08, "tackles": 0.10,
        "aerialDuelsWon": 0.05, "aerialDuelsWonPercentage": 0.05,
        "groundDuelsWon": 0.05, "groundDuelsWonPercentage": 0.05,
        "totalDuelsWon": 0.05, "totalDuelsWonPercentage": 0.05,
        "accuratePassesPercentage": 0.10, "accurateLongBalls": 0.05,
        "accurateLongBallsPercentage": 0.05, "accuratePasses": 0.05,
        "xGChain_per90": 0.03, "xGBuildup_per90": 0.03, "blockedShots": 0.05,
        "passToAssist": 0.02, "assist_ratio": 0.02
    },
    "Ccentral Midfield": {
        "interceptions": 0.05, "tackles": 0.05, "aerialDuelsWon": 0.04,
        "aerialDuelsWonPercentage": 0.04, "groundDuelsWon": 0.04, "groundDuelsWonPercentage": 0.04,
        "totalDuelsWon": 0.04, "totalDuelsWonPercentage": 0.04,
        "accuratePassesPercentage": 0.08, "accurateLongBalls": 0.05,
        "accurateLongBallsPercentage": 0.05, "accuratePasses": 0.05,
        "xGChain_per90": 0.05, "xGBuildup_per90": 0.05, "blockedShots": 0.03,
        "passToAssist": 0.03, "assist_ratio": 0.05, "keyPasses": 0.05,
        "totalShots": 0.05, "shotsOnTarget": 0.05, "goal_ratio": 0.06
    },
    "Attacking Midfield": {
        "interceptions": 0.03, "totalDuelsWon": 0.03, "totalDuelsWonPercentage": 0.03,
        "accuratePassesPercentage": 0.05, "accurateLongBalls": 0.03, "accurateLongBallsPercentage": 0.03,
        "accuratePasses": 0.03, "xGChain_per90": 0.04, "xGBuildup_per90": 0.04,
        "passToAssist": 0.05, "assist_ratio": 0.08, "keyPasses": 0.10,
        "totalShots": 0.08, "shotsOnTarget": 0.08, "goalConversionPercentage": 0.08,
        "bigChancesCreated": 0.07, "goal_ratio": 0.07
    },
    "Left_Wing": {
        "totalDuelsWon": 0.04, "totalDuelsWonPercentage": 0.04,
        "accuratePassesPercentage": 0.04, "xGChain_per90": 0.03, "xGBuildup_per90": 0.03,
        "passToAssist": 0.05, "assist_ratio": 0.08, "keyPasses": 0.08,
        "totalShots": 0.08, "shotsOnTarget": 0.08, "goalConversionPercentage": 0.08,
        "bigChancesCreated": 0.07, "accurateCrosses": 0.07,
        "accurateCrossesPercentage": 0.07, "goal_ratio": 0.08
    },
    "Right_Wing": {
        "totalDuelsWon": 0.04, "totalDuelsWonPercentage": 0.04,
        "accuratePassesPercentage": 0.04, "xGChain_per90": 0.03, "xGBuildup_per90": 0.03,
        "passToAssist": 0.05, "assist_ratio": 0.08, "keyPasses": 0.08,
        "totalShots": 0.08, "shotsOnTarget": 0.08, "goalConversionPercentage": 0.08,
        "bigChancesCreated": 0.07, "accurateCrosses": 0.07,
        "accurateCrossesPercentage": 0.07, "goal_ratio": 0.08
    },
    "Centre-Foward": {
        "totalDuelsWon": 0.04, "totalDuelsWonPercentage": 0.04,
        "accuratePassesPercentage": 0.03, "xGChain_per90": 0.03, "xGBuildup_per90": 0.03,
        "passToAssist": 0.03, "assist_ratio": 0.07, "keyPasses": 0.07,
        "totalShots": 0.12, "shotsOnTarget": 0.12, "goalConversionPercentage": 0.12,
        "bigChancesCreated": 0.06, "goal_ratio": 0.12
    }
}

# ===================== PENALTIES (por posición) =====================
PENALTY = ["yellowCards","redCards","fouls","dispossessed","goalsConcededInsideTheBox",
           "goalsConcededOutsideTheBox","bigChancesMissed","offsides","errorLeadToGoal",
           "errorLeadToShot","dribbledPast"]

POS_PENALTY_WEIGHTS = {
    "Goalkeeper": {
        "redCards": 2.0, "yellowCards": 0.8,
        "errorLeadToGoal": 2.0, "errorLeadToShot": 1.5,
        "goalsConcededInsideTheBox": 1.6, "goalsConcededOutsideTheBox": 1.2,
        "fouls": 0.8, "offsides": 0.2, "dispossessed": 0.3, "dribbledPast": 0.8,
    },
    "Centre-Back": {
        "redCards": 2.0, "yellowCards": 1.0,
        "errorLeadToGoal": 2.0, "errorLeadToShot": 1.6,
        "dribbledPast": 1.6, "fouls": 1.2, "dispossessed": 0.8,
        "offsides": 0.1, "bigChancesMissed": 0.3,
        "goalsConcededInsideTheBox": 0.8, "goalsConcededOutsideTheBox": 0.5,
    },
    "Right-Back": {
        "redCards": 1.8, "yellowCards": 1.0,
        "errorLeadToGoal": 1.8, "errorLeadToShot": 1.4,
        "dribbledPast": 1.5, "fouls": 1.0, "dispossessed": 0.9,
        "offsides": 0.2, "bigChancesMissed": 0.4,
        "goalsConcededInsideTheBox": 0.7, "goalsConcededOutsideTheBox": 0.4,
    },
    "Left-Back": {
        "redCards": 1.8, "yellowCards": 1.0,
        "errorLeadToGoal": 1.8, "errorLeadToShot": 1.4,
        "dribbledPast": 1.5, "fouls": 1.0, "dispossessed": 0.9,
        "offsides": 0.2, "bigChancesMissed": 0.4,
        "goalsConcededInsideTheBox": 0.7, "goalsConcededOutsideTheBox": 0.4,
    },
    "Defensive Midfield": {
        "redCards": 1.8, "yellowCards": 1.1,
        "errorLeadToGoal": 1.8, "errorLeadToShot": 1.5,
        "fouls": 1.2, "dribbledPast": 1.3, "dispossessed": 1.0,
        "offsides": 0.2, "bigChancesMissed": 0.5,
        "goalsConcededInsideTheBox": 0.5, "goalsConcededOutsideTheBox": 0.3,
    },
    "Central Midfield": {
        "redCards": 1.8, "yellowCards": 1.0,
        "errorLeadToGoal": 1.7, "errorLeadToShot": 1.4,
        "fouls": 1.1, "dribbledPast": 1.2, "dispossessed": 1.1,
        "offsides": 0.2, "bigChancesMissed": 0.6,
        "goalsConcededInsideTheBox": 0.3, "goalsConcededOutsideTheBox": 0.2,
    },
    "Attacking Midfield": {
        "redCards": 1.6, "yellowCards": 0.9,
        "errorLeadToGoal": 1.6, "errorLeadToShot": 1.3,
        "dispossessed": 1.3, "dribbledPast": 1.0, "fouls": 1.0,
        "offsides": 0.4, "bigChancesMissed": 0.8,
        "goalsConcededInsideTheBox": 0.2, "goalsConcededOutsideTheBox": 0.2,
    },
    "Left_Wing": {
        "redCards": 1.5, "yellowCards": 0.9,
        "errorLeadToGoal": 1.4, "errorLeadToShot": 1.2,
        "dispossessed": 1.4, "dribbledPast": 0.9, "fouls": 1.0,
        "offsides": 1.0, "bigChancesMissed": 1.2,
        "goalsConcededInsideTheBox": 0.1, "goalsConcededOutsideTheBox": 0.1,
    },
    "Right_Wing": {
        "redCards": 1.5, "yellowCards": 0.9,
        "errorLeadToGoal": 1.4, "errorLeadToShot": 1.2,
        "dispossessed": 1.4, "dribbledPast": 0.9, "fouls": 1.0,
        "offsides": 1.0, "bigChancesMissed": 1.2,
        "goalsConcededInsideTheBox": 0.1, "goalsConcededOutsideTheBox": 0.1,
    },
    "Centre-Foward": {
        "redCards": 1.6, "yellowCards": 0.8,
        "errorLeadToGoal": 1.5, "errorLeadToShot": 1.3,
        "bigChancesMissed": 1.6, "offsides": 1.3,
        "dispossessed": 1.2, "dribbledPast": 0.7, "fouls": 0.9,
        "goalsConcededInsideTheBox": 0.1, "goalsConcededOutsideTheBox": 0.1,
    },
}

# ===================== SPARK UTILS =====================
def crear_spark():
    return (
        SparkSession.builder
        .appName("scouting")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def leer_parquet(spark):
    return spark.read.parquet(PATH_IN)

def guardar_parquet(df):
    (df.write
        .mode("overwrite")
        .partitionBy("season_part", "liga_part")
        .parquet(PATH_OUT))
    print(f"✅ SCORE escrito en: {PATH_OUT}")

# ===================== FEATURES =====================
def add_per90(df):
    columnas = ["xG", "xA", "xGChain", "xGBuildup"]
    for c in columnas:
        if c in df.columns:
            df = df.withColumn(
                f"{c}_per90",
                F.when(F.col("time") > 0, F.col(c) / F.col("time") * 90)
            )
    return df

def efficiency(df):
    """
    goal_ratio = goals / xG_per90  (o goals/xG si no existe per90)
    assist_ratio = assists / xA_per90 (o assists/xA si no existe per90)
    """
    cols = set(df.columns)
    g_col  = "goals"
    a_col  = "assists"
    xg_col = "xG_per90" if "xG_per90" in cols else ("xG" if "xG" in cols else None)
    xa_col = "xA_per90" if "xA_per90" in cols else ("xA" if "xA" in cols else None)

    if xg_col:
        df = df.withColumn("goal_ratio", F.when(F.col(xg_col) > 0, F.col(g_col) / F.col(xg_col)))
    if xa_col:
        df = df.withColumn("assist_ratio", F.when(F.col(xa_col) > 0, F.col(a_col) / F.col(xa_col)))
    return df

# ===================== PENALTY SCORE (por posición) =====================
def add_penalty_score_spark_pos(
    df,
    penalty_cols=PENALTY,
    pos_weights=POS_PENALTY_WEIGHTS,
    groupby=("liga","season","position"),
    out_col="penalty_score"
):
    w = Window.partitionBy(*groupby)

    available = [c for c in penalty_cols if c in df.columns]
    if not available:
        return df.withColumn(out_col, F.lit(0.0))

    df2 = df
    for c in available:
        mu = F.avg(F.col(c)).over(w)
        sd = F.stddev(F.col(c)).over(w)
        df2 = df2.withColumn(f"{c}__z", F.when(sd > 0, (F.col(c) - mu) / sd).otherwise(F.lit(0.0)))

    def sum_for_position(pos_name: str):
        base = pos_weights.get(pos_name, {})
        usable = [(c, float(base.get(c, 1.0))) for c in available if f"{c}__z" in df2.columns]
        if not usable:
            return F.lit(0.0)
        s = sum(wgt for _, wgt in usable)
        usable = [(c, (wgt / s) if s > 0 else 0.0) for c, wgt in usable]
        terms = [F.coalesce(F.col(f"{c}__z"), F.lit(0.0)) * F.lit(wgt) for c, wgt in usable]
        return -F.reduce(F.array(*terms), F.lit(0.0), lambda a, b: a + b)

    positions = list(pos_weights.keys())
    expr = None
    for pos in positions:
        expr = F.when(F.col("position") == F.lit(pos), sum_for_position(pos)) if expr is None else expr.when(F.col("position") == F.lit(pos), sum_for_position(pos))

    # fallback uniforme si aparece una posición fuera del diccionario
    uniform_terms = [F.coalesce(F.col(f"{c}__z"), F.lit(0.0)) * F.lit(1.0/len(available)) for c in available]
    expr = expr.otherwise(-F.reduce(F.array(*uniform_terms), F.lit(0.0), lambda a, b: a + b))

    return df2.withColumn(out_col, expr)

# ===================== PERFORMANCE SCORE =====================
def compute_weighted_score(
    df,
    position_weights_dict,
    groupby=("liga","season","position"),
    use_zscore: bool = True,
    out_col: str = "performance_score"
):
    """
    position_weights_dict: dict[pos][metric] = weight  (suma ~1 por pos; se reescala sobre métricas presentes)
    """
    # Derivamos position_metrics de las claves de pesos
    position_metrics = {pos: list(mw.keys()) for pos, mw in position_weights_dict.items()}

    if use_zscore:
        w = Window.partitionBy(*groupby)
        all_mets = sorted({m for mets in position_metrics.values() for m in mets})
        for m in all_mets:
            if m in df.columns:
                mu = F.avg(F.col(m)).over(w)
                sd = F.stddev(F.col(m)).over(w)
                df = df.withColumn(f"{m}__z", F.when(sd > 0, (F.col(m)-mu)/sd))

    def score_for_position(pos: str):
        mets = position_metrics.get(pos, [])
        if not mets:
            return F.lit(None)

        usable = []
        for m in mets:
            colname = f"{m}__z" if use_zscore and f"{m}__z" in df.columns else (m if m in df.columns else None)
            if colname:
                usable.append((m, colname))
        if not usable:
            return F.lit(None)

        wpos = position_weights_dict.get(pos, {})
        pairs = []
        for m, cname in usable:
            wgt = wpos.get(m, None)
            pairs.append((cname, (float(wgt) if wgt is not None else 1.0)))

        total = sum(w for _, w in pairs)
        pairs = [(cname, (w/total) if total > 0 else 1.0/len(pairs)) for cname, w in pairs]

        terms = [F.coalesce(F.col(cname), F.lit(0.0)) * F.lit(w) for cname, w in pairs]
        return F.reduce(F.array(*terms), F.lit(0.0), lambda a,b: a+b)

    expr = None
    for pos in position_metrics.keys():
        expr = F.when(F.col("position") == F.lit(pos), score_for_position(pos)) if expr is None else expr.when(F.col("position") == F.lit(pos), score_for_position(pos))
    expr = expr.otherwise(F.lit(None))

    return df.withColumn(out_col, expr)

# ===================== AJUSTE FINAL =====================
def add_adjusted_score(df, perf_col="performance_score", pen_col="penalty_score", out_col="adjusted_score"):
    return df.withColumn(out_col, F.col(perf_col) + F.coalesce(F.col(pen_col), F.lit(0.0)))

# ===================== MAIN =====================
def main():
    spark = crear_spark()
    df = leer_parquet(spark)

    # Features
    df = add_per90(df)
    df = efficiency(df)

    # Scores
    df = add_penalty_score_spark_pos(df)  # crea 'penalty_score'
    df = compute_weighted_score(df, position_weights_dict=position_weights)  # crea 'performance_score'
    df = add_adjusted_score(df, perf_col="performance_score", pen_col="penalty_score", out_col="adjusted_score")

    guardar_parquet(df)
    spark.stop()

if __name__ == "__main__":
    main()
