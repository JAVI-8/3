from pathlib import Path
import math
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

from repo.spark_pipeline.procesamiento_spark import (
    map_squads_by_league,
    cast_all_numeric_to_double,
    normalizar_por_90_min,
    combinacion_variables,
    union_datasets,
    guardar_en_parquet,
    procesar,
)

def test_map_squads_by_league_empata_por_levenshtein(spark):
    tm = spark.createDataFrame(
        [
            Row(Liga="Bundesliga", Season="2024-2025", Squad="Leverkusn"),  # mal escrito
            Row(Liga="Bundesliga", Season="2024-2025", Squad="Bayern Munich"),
        ]
    )
    fb = spark.createDataFrame(
        [
            Row(Liga="Bundesliga", Season="2024-2025", Squad="Leverkusen"),
            Row(Liga="Bundesliga", Season="2024-2025", Squad="Bayern München"),
        ]
    )
    mapped = map_squads_by_league(tm, fb, max_frac=0.5, max_abs=2)
    # Debe corregir "Leverkusn" -> "Leverkusen". "Bayern Munich" podría quedar igual si no entra por el umbral.
    out = { (r.Liga, r.Season, r.Squad) for r in mapped.collect() }
    assert ("Bundesliga", "2024-2025", "Leverkusen") in out

def test_cast_all_numeric_to_double(spark):
    df = spark.createDataFrame(
        [Row(Player="A", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="FW", Min="180", Gls="2")])
    casted = cast_all_numeric_to_double(df)
    assert dict(casted.dtypes)["Min"] == "double"
    assert dict(casted.dtypes)["Gls"] == "double"
    assert dict(casted.dtypes)["Player"] == "string"

def test_normalizar_por_90_min(spark):
    df = spark.createDataFrame(
        [Row(Player="A", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="FW", Min=180.0, Gls=2.0)]
    )
    norm = normalizar_por_90_min(df)
    assert "Gls_per90" in norm.columns
    row = norm.collect()[0]
    # 2 goles en 180 min -> 1 gol por 90
    assert row["Gls_per90"] == pytest.approx(1.0)

def test_combinacion_variables(spark):
    df = spark.createDataFrame(
        [Row(Pass_Short=10.0, Pass_cmp_Short__=0.8, Pass_Medium=5.0, Pass_cmp_Medium__=0.5, 
             Pass_Long=2.0, Pass_cmp_Long__=0.25, TKL__=4.0, TKL___=0.5, 
             drib_Att=6.0, drib_Succ__=0.5, Tckl_Drib=8.0, Tckl_Drib__=0.5,
             PKatt=4.0, P_Save__=0.5, Crosses_Opp=10.0, Crosses_stp__=0.1)]
    )
    df = spark.createDataFrame(
        [Row(Pass_Short=10.0, **{"Pass_cmp_Short%":0.8},
             Pass_Medium=5.0, **{"Pass_cmp_Medium%":0.5},
             Pass_Long=2.0, **{"Pass_cmp_Long%":0.25},
             **{"TKL%":0.5}, TKL=4.0,
             drib_Att=6.0, **{"drib_Succ%":0.5},
             Tckl_Drib=8.0, **{"Tckl_Drib%":0.5},
             PKatt=4.0, **{"P_Save%":0.5},
             Crosses_Opp=10.0, **{"Crosses_stp%":0.1})]
    )
    out = combinacion_variables(df).collect()[0]
    assert out["Effective_Pass_Short"] == pytest.approx(8.0)
    assert out["Effective_Pass_Medium"] == pytest.approx(2.5)
    assert out["Effective_Pass_Long"] == pytest.approx(0.5)
    assert out["Effective_Tkl"] == pytest.approx(2.0)
    assert out["Effective_Drib"] == pytest.approx(3.0)
    assert out["Effective_Tackles_vsDrib"] == pytest.approx(4.0)
    assert out["Effective_Penalty_Saves"] == pytest.approx(2.0)
    assert out["Effective_Cross_stop"] == pytest.approx(1.0)

def test_union_datasets_y_guardado_parquet(spark, tmpdir, make_csv):
    cols_key = ["Player", "Squad", "Season", "Liga"]
    make_csv("stats.csv",     [("A","Lev","2024-2025","Bundesliga")], cols_key + ["Min"])
    make_csv("misc.csv",      [("A","Lev","2024-2025","Bundesliga")], cols_key + ["Fls"])
    make_csv("defense.csv",   [("A","Lev","2024-2025","Bundesliga")], cols_key + ["Tkl"])
    make_csv("passing.csv",   [("A","Lev","2024-2025","Bundesliga")], cols_key + ["Pass_Short"])
    make_csv("possession.csv",[("A","Lev","2024-2025","Bundesliga")], cols_key + ["drib_Att"])
    make_csv("mercado.csv",   [("A","Leverkusen","2024-2025","Bundesliga", 5_000_000)], cols_key[:-1]+["Liga","Value"])
    make_csv("shooting.csv",  [("A","Lev","2024-2025","Bundesliga")], cols_key + ["Sh"])
    make_csv("keepers.csv",   [("A","Lev","2024-2025","Bundesliga")], cols_key + ["GA"])
    make_csv("keepersadv.csv",[("A","Lev","2024-2025","Bundesliga")], cols_key + ["PSxG"])

    out_df = union_datasets(spark, str(tmpdir))
    for k in cols_key:
        assert k in out_df.columns
    out_path = guardar_en_parquet(out_df.limit(1), str(tmpdir))
    assert Path(out_path).exists()

def test_procesar_end_to_end(spark, tmpdir, make_csv):
    cols_key = ["Player", "Squad", "Season", "Liga"]
    make_csv("stats.csv",     [("A","Leverkusen","2024-2025","Bundesliga", 180)], cols_key+["Min"])
    make_csv("misc.csv",      [("A","Leverkusen","2024-2025","Bundesliga", 2)], cols_key+["Fls"])
    make_csv("defense.csv",   [("A","Leverkusen","2024-2025","Bundesliga", 4)], cols_key+["Tkl"])
    make_csv("passing.csv",   [("A","Leverkusen","2024-2025","Bundesliga", 10)], cols_key+["Pass_Short"])
    make_csv("possession.csv",[("A","Leverkusen","2024-2025","Bundesliga", 6)], cols_key+["drib_Att"])
    make_csv("mercado.csv",   [("A","Leverkusn","2024-2025","Bundesliga", 5_000_000)], ["Player","Squad","Season","Liga","Value"])
    make_csv("shooting.csv",  [("A","Leverkusen","2024-2025","Bundesliga", 3)], cols_key+["Sh"])
    make_csv("keepers.csv",   [("A","Leverkusen","2024-2025","Bundesliga", 1)], cols_key+["GA"])
    make_csv("keepersadv.csv",[("A","Leverkusen","2024-2025","Bundesliga", 0.5)], cols_key+["PSxG"])

    out_parquet = procesar(str(tmpdir), str(tmpdir))
    assert Path(out_parquet).exists()
    
    df = spark.read.parquet(out_parquet)
    assert "Pass_Short_per90" in df.columns
    assert "drib_Att_per90" in df.columns
 
    assert dict(df.dtypes)["Min"] == "double"
