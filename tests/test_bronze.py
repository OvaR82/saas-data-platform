"""Bronze: date validation, technical columns and idempotent writes."""

from pyspark.sql import functions as F

from saas_pipeline import bronze
from saas_pipeline.config import table_path


def test_process_date_validation_rejects_null_iso_and_impossible_dates(spark):
    # One valid date followed by every invalid shape found in the real dataset.
    values = ["20250314", "20250230", "20251332", "00000000", "2025-03-15", None]
    df = spark.createDataFrame([(v,) for v in values], "fecha_proceso string")
    result = df.select(bronze.is_valid_process_date(F.col("fecha_proceso")).alias("ok")).collect()
    assert [r.ok for r in result] == [True, False, False, False, False, False]


def test_technical_columns_and_tenant_normalization(spark):
    # 'SV' and ' sv ' belong to tenant sv after trim + lowercase; 'HN' is filtered out.
    df = spark.createDataFrame([("SV",), (" sv ",), ("HN",)], "pais string")
    out = bronze.add_technical_columns(df, "sv", "batch-1").collect()
    assert len(out) == 2
    assert {r.pais for r in out} == {"sv"}
    assert all(r._tenant_id == "sv" and r._batch_id == "batch-1" for r in out)
    assert all(r._ingestion_timestamp is not None for r in out)


def test_split_by_process_date(spark):
    # In range, out of range (dropped for this run), null and invalid (quarantined).
    df = spark.createDataFrame(
        [("20250301",), ("20250401",), (None,), ("2025-03-15",)], "fecha_proceso string"
    )
    rows, quarantine = bronze.split_by_process_date(df, "20250301", "20250331")
    assert [r.fecha_proceso for r in rows.collect()] == ["20250301"]
    reasons = {r.fecha_proceso: r._quarantine_reason for r in quarantine.collect()}
    assert reasons == {None: "null_fecha_proceso", "2025-03-15": "invalid_fecha_proceso"}


def test_bronze_rerun_is_idempotent(spark, make_cfg):
    cfg = make_cfg()
    path = table_path(cfg, "bronze", bronze.TABLE)

    # Same range twice: the second run must replace the first, not append to it.
    first = bronze.run(spark, cfg, "batch-1")
    second = bronze.run(spark, cfg, "batch-2")
    stored = spark.read.format("delta").load(path)

    assert first == second
    assert stored.count() == first["written"]
    assert {r._batch_id for r in stored.select("_batch_id").distinct().collect()} == {"batch-2"}


def test_bronze_range_rerun_only_touches_that_range(spark, make_cfg):
    # Load the whole semester, then reprocess only March.
    bronze.run(spark, make_cfg(), "full")
    march = make_cfg("execution.start_date=2025-03-01", "execution.end_date=2025-03-31")
    bronze.run(spark, march, "march")

    # March rows carry the new batch; every other month still has the original one.
    stored = spark.read.format("delta").load(table_path(march, "bronze", bronze.TABLE))
    batches = {
        (r.fecha_proceso[:6], r._batch_id)
        for r in stored.select("fecha_proceso", "_batch_id").distinct().collect()
    }
    assert {b for m, b in batches if m == "202503"} == {"march"}
    assert {b for m, b in batches if m != "202503"} == {"full"}
