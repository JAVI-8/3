import math
from pathlib import Path
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

from repo.spark_pipeline import performance
import math
from pathlib import Path
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

# ⬇️ Ajusta este import si el archivo se llama distinto
from repo.spark_pipeline import performance


def test_normalize_weights_by_position():
    norm = performance.normalize_weights_by_position()
    # todas las posiciones presentes
    assert "Centre-Forward" in norm
    # pesos normalizados suman ~1.0
    s = sum(w for _, w in norm["Centre-Forward"])
    assert s == pytest.approx(1.0, abs=1e-6)
    # formato: lista de (metric, weight)
    assert isinstance(norm["Centre-Forward"][0], tuple)
    assert len(norm["Centre-Forward"][0]) == 2


def test_cast_all_numeric_to_double(spark):
    df = spark.createDataFrame([
        Row(Player="A", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="FW",
            Min="180", Gls="2", SomeFloat="3.14")
    ])
    out = performance.cast_all_numeric_to_double(df)
    dtypes = dict(out.dtypes)
    # excluidos como string
    for c in ["Player", "Squad", "Season", "Liga", "Pos"]:
        assert dtypes[c] == "string"
    # resto double
    assert dtypes["Min"] == "double"
    assert dtypes["Gls"] == "double"
    assert dtypes["SomeFloat"] == "double"


def test_calcular_penalty_score(spark):
    # incluye Off_per90 para un ofensivo y un no-ofensivo
    df = spark.createDataFrame([
        Row(Player="Winger", Pos="Right Winger", YellowC_per90=1.0, RedC_per90=0.0,
            Fls_per90=2.0, Err_per90=1.0, Loss_Control_Tackle_per90=1.0,
            fail_To_Gain_Control_per90=2.0, Off_per90=3.0),
        Row(Player="CB", Pos="Centre-Back", YellowC_per90=1.0, RedC_per90=0.0,
            Fls_per90=2.0, Err_per90=1.0, Loss_Control_Tackle_per90=1.0,
            fail_To_Gain_Control_per90=2.0, Off_per90=3.0),
    ])
    out = performance.calcular_penalty_score(df)
    rows = {r.Player: r.penalty_score for r in out.select("Player", "penalty_score").collect()}
    # Winger: incluye Off_per90 (0.02 * 3.0 = 0.06)
    # Total = 0.03*1 + 0.1*0 + 0.01*2 + 0.05*1 + 0.02*1 + 0.01*2 + 0.02*3
    exp_w = 0.03 + 0 + 0.02 + 0.05 + 0.02 + 0.02 + 0.06
    # CB: mismo pero sin Off_per90
    exp_cb = 0.03 + 0 + 0.02 + 0.05 + 0.02 + 0.02 + 0.0
    assert rows["Winger"] == pytest.approx(exp_w)
    assert rows["CB"] == pytest.approx(exp_cb)


def test_calcular_performance_score_con_pesos_minimo(spark):
    
    df = spark.createDataFrame([
        Row(Player="CF1", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="Centre-Forward",
            Gls_per90=0.5, SoT_per90=0.4, xG_per90=0.6, npxG_per90=0.3, PrgR_per90=0.2)
    ])
    out = performance.calcular_performance_score_con_pesos(df)
    assert "performance_score" in out.columns
    row = out.collect()[0]

    assert 0.0 <= row["performance_score"] <= 1.0
    assert row["Evaluated_Position"] == "Centre-Forward"


def test_calcular_adjusted_score(spark):
    df = spark.createDataFrame([
        Row(performance_score=0.8, penalty_score=0.1),
        Row(performance_score=0.3, penalty_score=0.4),
    ])
    out = performance.calcular_adjusted_score(df)
    vals = [r.adjusted_score for r in out.select("adjusted_score").collect()]
    assert vals == [pytest.approx(0.7), pytest.approx(-0.1)]


def test_guardar_crea_parquet(spark, tmpdir):
    df = spark.createDataFrame([
        Row(Player="A", Squad="Lev", Season="2024-2025", Liga="Bundesliga", Pos="GK", Min=180.0,
            Value=5_000_000.0, Height=1.9,
            performance_score=0.8, penalty_score=0.1, adjusted_score=0.7)
    ])
    performance.guardar(df, str(tmpdir))
    path = Path(tmpdir) / "final.parquet"
    assert path.exists()

    leido = spark.read.parquet(str(path))
    for c in ["Player","Squad","Season","Liga","Pos","Min","Value","Height",
              "performance_score","penalty_score","adjusted_score"]:
        assert c in leido.columns


def test_normalize_weights_by_position():
    norm = performance.normalize_weights_by_position()

    assert "Centre-Forward" in norm
    
    s = sum(w for _, w in norm["Centre-Forward"])
    assert s == pytest.approx(1.0, abs=1e-6)
    assert isinstance(norm["Centre-Forward"][0], tuple)
    assert len(norm["Centre-Forward"][0]) == 2


def test_cast_all_numeric_to_double(spark):
    df = spark.createDataFrame([
        Row(Player="A", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="FW",
            Min="180", Gls="2", SomeFloat="3.14")
    ])
    out = performance.cast_all_numeric_to_double(df)
    dtypes = dict(out.dtypes)
    for c in ["Player", "Squad", "Season", "Liga", "Pos"]:
        assert dtypes[c] == "string"
    
    assert dtypes["Min"] == "double"
    assert dtypes["Gls"] == "double"
    assert dtypes["SomeFloat"] == "double"


def test_calcular_penalty_score(spark):

    df = spark.createDataFrame([
        Row(Player="Winger", Pos="Right Winger", YellowC_per90=1.0, RedC_per90=0.0,
            Fls_per90=2.0, Err_per90=1.0, Loss_Control_Tackle_per90=1.0,
            fail_To_Gain_Control_per90=2.0, Off_per90=3.0),
        Row(Player="CB", Pos="Centre-Back", YellowC_per90=1.0, RedC_per90=0.0,
            Fls_per90=2.0, Err_per90=1.0, Loss_Control_Tackle_per90=1.0,
            fail_To_Gain_Control_per90=2.0, Off_per90=3.0),
    ])
    out = performance.calcular_penalty_score(df)
    rows = {r.Player: r.penalty_score for r in out.select("Player", "penalty_score").collect()}

    exp_w = 0.03 + 0 + 0.02 + 0.05 + 0.02 + 0.02 + 0.06

    exp_cb = 0.03 + 0 + 0.02 + 0.05 + 0.02 + 0.02 + 0.0
    assert rows["Winger"] == pytest.approx(exp_w)
    assert rows["CB"] == pytest.approx(exp_cb)


def test_calcular_performance_score_con_pesos_minimo(spark):

    df = spark.createDataFrame([
        Row(Player="CF1", Squad="X", Season="2024-2025", Liga="Bundesliga", Pos="Centre-Forward",
            Gls_per90=0.5, SoT_per90=0.4, xG_per90=0.6, npxG_per90=0.3, PrgR_per90=0.2)
    ])
    out = performance.calcular_performance_score_con_pesos(df)
    assert "performance_score" in out.columns
    row = out.collect()[0]
    assert 0.0 <= row["performance_score"] <= 1.0
    assert row["Evaluated_Position"] == "Centre-Forward"


def test_calcular_adjusted_score(spark):
    df = spark.createDataFrame([
        Row(performance_score=0.8, penalty_score=0.1),
        Row(performance_score=0.3, penalty_score=0.4),
    ])
    out = performance.calcular_adjusted_score(df)
    vals = [r.adjusted_score for r in out.select("adjusted_score").collect()]
    assert vals == [pytest.approx(0.7), pytest.approx(-0.1)]


def test_guardar_crea_parquet(spark, tmpdir):
    df = spark.createDataFrame([
        Row(Player="A", Squad="Lev", Season="2024-2025", Liga="Bundesliga", Pos="GK", Min=180.0,
            Value=5_000_000.0, Height=1.9,
            performance_score=0.8, penalty_score=0.1, adjusted_score=0.7)
    ])
    performance.guardar(df, str(tmpdir))
    path = Path(tmpdir) / "final.parquet"
    assert path.exists()
    leido = spark.read.parquet(str(path))
    for c in ["Player","Squad","Season","Liga","Pos","Min","Value","Height",
              "performance_score","penalty_score","adjusted_score"]:
        assert c in leido.columns
