# tests/test_infravalorados.py
import shutil
from pathlib import Path
import pytest
from pyspark.sql import SparkSession, Row
from repo.spark_pipeline.infravalorados import comparar_jugadores, guardar_top

# ---------- Fixtures ----------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("infravalorados-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def outdir(tmp_path: Path):
    # carpeta temporal que simula tu carpeta de salida
    return tmp_path / "out"


# ---------- Tests ----------
def test_guardar_top_crea_parquet(spark: SparkSession, outdir: Path):
    # DF mínimo con las columnas esperadas por tu pipeline de salida
    df = spark.createDataFrame(
        [
            Row(player="A", Season="2024-2025", Pos="GK", adjusted_score=95.0, value=5.0),
            Row(player="B", Season="2024-2025", Pos="GK", adjusted_score=60.0, value=30.0),
        ]
    )
    guardar_top(df, str(outdir))
    parquet_path = outdir / "top.parquet"
    assert parquet_path.exists(), "No se ha creado el directorio/archivo parquet"

    # leer y validar
    leido = spark.read.parquet(str(parquet_path))
    assert leido.count() == 2
    assert set(leido.columns) == {"player", "Season", "Pos", "adjusted_score", "value"}


def test_comparar_jugadores_filtra_y_ordena_por_grupo(spark: SparkSession, outdir: Path):
    """
    Crea dos grupos (Season, Pos) y verifica:
      - Se seleccionan solo los jugadores con adjusted_score >= p75 y value < p50 por grupo.
      - El parquet se escribe en la ruta esperada.
      - El orden es por Season, Pos y adjusted_score desc dentro del grupo.
    """
    data = [
        #grupo 1: Season 24-25, portero
        #adjusted_scores: 40, 50, 60, 95  -> p75 ~ entre 60 y 95 mas o menos 70-80 de percentil
        #valores: 10, 20, 30, 5   -> p50 ~ 25
        Row(player="GK_low1", Season="2024-2025", Pos="GK", adjusted_score=40.0, value=10.0),
        Row(player="GK_low2", Season="2024-2025", Pos="GK", adjusted_score=50.0, value=20.0),
        Row(player="GK_mid",  Season="2024-2025", Pos="GK", adjusted_score=60.0, value=30.0),
        Row(player="GK_top",  Season="2024-2025", Pos="GK", adjusted_score=95.0, value=5.0),  
        #grupo 2: Season 24-25, delantero
        #adjusted_scores: 10, 20, 30, 40 -> p75  mas o menos 32.5 aprox
        #valores: 200,150,100,10 -> p50 ~ ~125
        Row(player="FW_10",   Season="2024-2025", Pos="FW", adjusted_score=10.0, value=200.0),
        Row(player="FW_20",   Season="2024-2025", Pos="FW", adjusted_score=20.0, value=150.0),
        Row(player="FW_30",   Season="2024-2025", Pos="FW", adjusted_score=30.0, value=100.0),  # value<mediana pero score< p75
        Row(player="FW_40",   Season="2024-2025", Pos="FW", adjusted_score=40.0, value=10.0),   
    ]
    df = spark.createDataFrame(data)

    #ejecutar
    comparar_jugadores(df, str(outdir))

    #validar salida
    parquet_path = outdir / "top.parquet"
    assert parquet_path.exists(), "No se generó el parquet de salida"

    res = spark.read.parquet(str(parquet_path))

    #esperados
    got = {(r["Season"], r["Pos"], r["player"]) for r in res.collect()}
    assert ("2024-2025", "GK", "GK_top") in got
    assert ("2024-2025", "FW", "FW_40") in got
    #no
    assert ("2024-2025", "FW", "FW_30") not in got  # score < p75 aunque value < p50
    assert ("2024-2025", "GK", "GK_mid") not in got  # score mas o menos 60 < p75

    
    res_fw = res.filter("Season = '2024-2025' AND Pos = 'FW'").orderBy("adjusted_score", ascending=False).collect()
    assert res_fw[0]["player"] == "FW_40"


def test_comparar_jugadores_sin_resultados_da_parquet_vacio(spark: SparkSession, outdir: Path):
    """
    Si ningún jugador cumple (ajustamos valores/umbrales implícitos), el parquet debe existir y tener 0 filas.
    """
    data = [
        Row(player="A", Season="2024-2025", Pos="MF", adjusted_score=10.0, value=500.0),
        Row(player="B", Season="2024-2025", Pos="MF", adjusted_score=11.0, value=600.0),
        Row(player="C", Season="2024-2025", Pos="MF", adjusted_score=12.0, value=700.0),
        Row(player="D", Season="2024-2025", Pos="MF", adjusted_score=13.0, value=800.0),
    ]
    df = spark.createDataFrame(data)

    #limpiar la salida anterior
    if outdir.exists():
        shutil.rmtree(outdir)

    comparar_jugadores(df, str(outdir))

    parquet_path = outdir / "top.parquet"
    assert parquet_path.exists(), "No se generó el parquet de salida"

    res = spark.read.parquet(str(parquet_path))
    assert res.count() == 0
