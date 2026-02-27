import datetime as dt
import difflib
import logging
import time
from typing import Any, Dict, List

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, LongType, StringType, StructField, StructType, TimestampType

from cot_01_config import PipelineConfig
from cot_03_parsing import (
    calculate_rut_l11,
    extract_codver,
    extract_rut,
    is_certificado_cotizaciones,
    metadata_matches,
    normalize_afp_from_text,
)
from cot_04_pdf_utils import extract_pdf_metadata, extract_pdf_text_normalized
from cot_05_afp_validator import AFPValidationError, AFPValidator

LOGGER = logging.getLogger(__name__)
SOURCE_REQUIRED_COLUMNS = {"DOC_IDN", "LINK", "PERIODO_PRODUCCION", "FECHA_INGRESO"}

RESULT_SCHEMA = StructType(
    [
        StructField("DOC_IDN", LongType(), False),
        StructField("LINK", StringType(), True),
        StructField("PERIODO_PRODUCCION", DateType(), True),
        StructField("FECHA_INGRESO", TimestampType(), True),
        StructField("METADATA_CREATOR", StringType(), True),
        StructField("METADATA_PRODUCER", StringType(), True),
        StructField("METADATA_CREADATE", StringType(), True),
        StructField("METADATA_MODDATE", StringType(), True),
        StructField("ES_METADATA", BooleanType(), True),
        StructField("AFP", StringType(), True),
        StructField("ES_CERT_COT", BooleanType(), True),
        StructField("CODVER", StringType(), True),
        StructField("RUT", StringType(), True),
        StructField("RUT_L11", StringType(), True),
        StructField("RES_AFP", StringType(), True),
        StructField("ES_DIF", BooleanType(), True),
        StructField("RES_DIF", StringType(), True),
        StructField("FECHA_PROCESO", TimestampType(), False),
    ]
)


def _diff_text(left: str, right: str) -> str:
    differ = difflib.Differ()
    differences = list(differ.compare(left.splitlines(), right.splitlines()))
    compact = [item for item in differences if item.startswith("-") or item.startswith("+")]
    return " | ".join(compact)


def _validate_source_columns(source_df: DataFrame) -> None:
    available = {column.upper() for column in source_df.columns}
    missing = sorted(SOURCE_REQUIRED_COLUMNS - available)
    if missing:
        raise ValueError(f"Faltan columnas requeridas en source_table: {missing}")


def _normalize_source_dataframe(source_df: DataFrame) -> DataFrame:
    return source_df.select(
        F.col("DOC_IDN").cast("long").alias("doc_idn"),
        F.col("LINK").cast("string").alias("link"),
        F.to_date(F.col("PERIODO_PRODUCCION")).alias("periodo_produccion"),
        F.col("FECHA_INGRESO").cast("timestamp").alias("fecha_ingreso"),
    )


def _load_candidates(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    cutoff_date = dt.date.today() - dt.timedelta(days=config.lookback_days)
    source_df = spark.table(config.source_table)
    _validate_source_columns(source_df)

    candidates_df = _normalize_source_dataframe(source_df).filter(
        F.to_date(F.col("fecha_ingreso")) >= F.lit(cutoff_date)
    )

    if config.filter_already_processed and spark.catalog.tableExists(config.target_table):
        existing_ids_df = (
            spark.table(config.target_table)
            .select(F.col("DOC_IDN").cast("long").alias("doc_idn"))
            .distinct()
        )
        candidates_df = candidates_df.join(existing_ids_df, on="doc_idn", how="left_anti")

    if config.row_limit > 0:
        candidates_df = candidates_df.orderBy("doc_idn").limit(config.row_limit)

    return candidates_df


def _log_zero_candidate_diagnostics(spark: SparkSession, config: PipelineConfig) -> None:
    cutoff_date = dt.date.today() - dt.timedelta(days=config.lookback_days)
    source_df = spark.table(config.source_table)
    normalized_df = _normalize_source_dataframe(source_df)
    recent_df = normalized_df.filter(F.to_date(F.col("fecha_ingreso")) >= F.lit(cutoff_date))

    total_source = normalized_df.count()
    recent_source = recent_df.count()
    min_max = normalized_df.select(
        F.min("fecha_ingreso").alias("min_fecha_ingreso"),
        F.max("fecha_ingreso").alias("max_fecha_ingreso"),
    ).collect()[0]

    LOGGER.warning(
        "Diagnostico candidatos=0 | total_source=%s | recent_source=%s | cutoff_date=%s | min_fecha_ingreso=%s | max_fecha_ingreso=%s",
        total_source,
        recent_source,
        cutoff_date.isoformat(),
        min_max["min_fecha_ingreso"],
        min_max["max_fecha_ingreso"],
    )

    if config.filter_already_processed and spark.catalog.tableExists(config.target_table):
        existing_ids_df = (
            spark.table(config.target_table)
            .select(F.col("DOC_IDN").cast("long").alias("doc_idn"))
            .distinct()
        )
        already_processed = recent_df.join(existing_ids_df, on="doc_idn", how="inner").count()
        pending_after_dedup = recent_df.join(existing_ids_df, on="doc_idn", how="left_anti").count()
        LOGGER.warning(
            "Diagnostico dedup | target_table=%s | already_processed=%s | pending_after_dedup=%s",
            config.target_table,
            already_processed,
            pending_after_dedup,
        )
    elif not config.filter_already_processed:
        LOGGER.warning("Diagnostico dedup | filtro de procesados DESACTIVADO por configuracion.")
    else:
        LOGGER.warning("Diagnostico dedup | target_table no existe, no aplica left_anti.")


def _download_original_pdf(link: str, timeout_secs: int) -> bytes:
    response = requests.get(link, timeout=timeout_secs)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} al descargar PDF origen")
    return response.content


def _build_base_result(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "DOC_IDN": row["doc_idn"],
        "LINK": row.get("link", ""),
        "PERIODO_PRODUCCION": row.get("periodo_produccion"),
        "FECHA_INGRESO": row.get("fecha_ingreso"),
        "METADATA_CREATOR": "",
        "METADATA_PRODUCER": "",
        "METADATA_CREADATE": "",
        "METADATA_MODDATE": "",
        "ES_METADATA": None,
        "AFP": "",
        "ES_CERT_COT": False,
        "CODVER": "",
        "RUT": "",
        "RUT_L11": "",
        "RES_AFP": "",
        "ES_DIF": None,
        "RES_DIF": "",
        "FECHA_PROCESO": dt.datetime.utcnow(),
    }


def _process_rows(candidate_rows: List[Dict[str, Any]], config: PipelineConfig) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    validator = AFPValidator(
        timeout_secs=config.request_timeout_secs,
        sleep_secs=config.request_sleep_secs,
        enable_selenium=config.enable_selenium,
        selenium_driver_path=config.selenium_driver_path,
        selenium_download_dir=config.selenium_download_dir,
    )

    try:
        for row in candidate_rows:
            doc_idn = row["doc_idn"]
            result = _build_base_result(row)
            original_text = ""

            try:
                original_pdf = _download_original_pdf(row["link"], config.request_timeout_secs)
            except Exception as err:
                LOGGER.error("doc_idn=%s error descargando PDF origen: %s", doc_idn, err)
                result["RES_AFP"] = "error_origen"
                results.append(result)
                continue

            try:
                metadata = extract_pdf_metadata(original_pdf)
                result["METADATA_CREATOR"] = metadata["metadata_creator"]
                result["METADATA_PRODUCER"] = metadata["metadata_producer"]
                result["METADATA_CREADATE"] = metadata["metadata_creadate"]
                result["METADATA_MODDATE"] = metadata["metadata_moddate"]
            except Exception as err:
                LOGGER.error("doc_idn=%s error metadata PDF: %s", doc_idn, err)

            try:
                original_text = extract_pdf_text_normalized(original_pdf)
            except Exception as err:
                LOGGER.error("doc_idn=%s error extrayendo texto PDF: %s", doc_idn, err)
                results.append(result)
                continue

            afp = normalize_afp_from_text(original_text)
            result["AFP"] = afp
            result["ES_CERT_COT"] = is_certificado_cotizaciones(original_text)
            result["RUT"] = extract_rut(original_text)
            result["CODVER"] = extract_codver(afp, original_text)
            result["RUT_L11"] = calculate_rut_l11(result["RUT"])

            metadata_match = metadata_matches(afp, result["METADATA_PRODUCER"])
            if metadata_match is not None:
                result["ES_METADATA"] = metadata_match

            has_required_fields = all(
                [result["ES_CERT_COT"], afp, result["RUT"], result["CODVER"]]
            )
            if has_required_fields:
                try:
                    validated_pdf = validator.download_pdf(
                        afp=afp, rut=result["RUT"], codver=result["CODVER"]
                    )
                    result["RES_AFP"] = "ok"
                    validated_text = extract_pdf_text_normalized(validated_pdf)
                    result["ES_DIF"] = validated_text != original_text
                    result["RES_DIF"] = (
                        _diff_text(validated_text, original_text)
                        if result["ES_DIF"]
                        else ""
                    )
                except AFPValidationError as err:
                    result["RES_AFP"] = "error"
                    LOGGER.error("doc_idn=%s error validando AFP: %s", doc_idn, err)
                except Exception as err:  # pragma: no cover
                    result["RES_AFP"] = "error"
                    LOGGER.error("doc_idn=%s error inesperado validando AFP: %s", doc_idn, err)

            results.append(result)
            time.sleep(config.request_sleep_secs)
    finally:
        validator.close()

    return results


def run_pipeline(spark: SparkSession, config: PipelineConfig) -> int:
    LOGGER.info(
        "Iniciando pipeline | source_table=%s | target_table=%s | lookback_days=%s | row_limit=%s",
        config.source_table,
        config.target_table,
        config.lookback_days,
        config.row_limit,
    )

    candidates_df = _load_candidates(spark=spark, config=config)
    candidate_rows = [row.asDict() for row in candidates_df.toLocalIterator()]
    LOGGER.info("Registros candidatos: %s", len(candidate_rows))

    if not candidate_rows:
        LOGGER.info("Sin registros nuevos para procesar.")
        if config.enable_candidate_diagnostics:
            _log_zero_candidate_diagnostics(spark=spark, config=config)
        return 0

    processed_rows = _process_rows(candidate_rows=candidate_rows, config=config)
    if not processed_rows:
        LOGGER.info("No se generaron resultados para persistir.")
        return 0

    results_df = spark.createDataFrame(processed_rows, schema=RESULT_SCHEMA)
    results_df.write.format("delta").mode("append").saveAsTable(config.target_table)
    LOGGER.info(
        "Pipeline finalizada | target_table=%s | registros_insertados=%s",
        config.target_table,
        len(processed_rows),
    )
    return len(processed_rows)
