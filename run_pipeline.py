import argparse
import importlib.util
import logging
import os
from pathlib import Path
import sys


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Databricks pipeline for Proyecto Cotizaciones"
    )
    parser.add_argument("--source-table", default="opx.p_ddv_opx.afp_certificados")
    parser.add_argument("--target-table", default="opx.p_ddv_opx.afp_certificados_output")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--row-limit", type=int, default=240)
    parser.add_argument("--request-timeout-secs", type=int, default=30)
    parser.add_argument("--request-sleep-secs", type=float, default=1.0)
    parser.add_argument("--enable-selenium", action="store_true")
    parser.add_argument("--selenium-driver-path", default="/databricks/driver/chromedriver")
    parser.add_argument(
        "--selenium-download-dir",
        default="/dbfs/tmp/proyecto_cotizaciones/downloads",
    )
    parser.add_argument(
        "--disable-filter-processed",
        action="store_true",
        help="No aplica left_anti contra target_table (debug/reproceso).",
    )
    parser.add_argument(
        "--disable-candidate-diagnostics",
        action="store_true",
        help="Desactiva conteos de diagnostico cuando hay 0 candidatos.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _default_argv() -> list:
    # Databricks notebooks inject internal args in sys.argv.
    if "DATABRICKS_RUNTIME_VERSION" in os.environ and "ipykernel" in sys.modules:
        return []
    return sys.argv[1:]


def _load_module(module_name: str, file_name: str):
    source_path = Path(__file__).resolve().parent / "src" / file_name
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar modulo: {file_name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_runtime_modules():
    ordered_modules = [
        ("cot_01_config", "01_config.py"),
        ("cot_02_afp_rules", "02_afp_rules.py"),
        ("cot_03_parsing", "03_parsing.py"),
        ("cot_04_pdf_utils", "04_pdf_utils.py"),
        ("cot_05_afp_validator", "05_afp_validator.py"),
        ("cot_06_pipeline", "06_pipeline.py"),
    ]
    loaded = {}
    for module_name, file_name in ordered_modules:
        loaded[module_name] = _load_module(module_name, file_name)
    return loaded


def main(argv=None):
    if argv is None:
        argv = _default_argv()
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    runtime_modules = _load_runtime_modules()
    pipeline_config = runtime_modules["cot_01_config"].PipelineConfig
    run_pipeline = runtime_modules["cot_06_pipeline"].run_pipeline

    config = pipeline_config(
        source_table=args.source_table,
        target_table=args.target_table,
        lookback_days=args.lookback_days,
        row_limit=args.row_limit,
        request_timeout_secs=args.request_timeout_secs,
        request_sleep_secs=args.request_sleep_secs,
        enable_selenium=args.enable_selenium,
        selenium_driver_path=args.selenium_driver_path,
        selenium_download_dir=args.selenium_download_dir,
        filter_already_processed=not args.disable_filter_processed,
        enable_candidate_diagnostics=not args.disable_candidate_diagnostics,
        log_level=args.log_level,
    )

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    inserted_rows = run_pipeline(spark=spark, config=config)
    logging.getLogger(__name__).info(
        "Pipeline finalizada. Registros insertados: %s", inserted_rows
    )


if __name__ == "__main__":
    main()
