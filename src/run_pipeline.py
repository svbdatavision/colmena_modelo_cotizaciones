import argparse
import logging
import sys

from proyecto_cotizaciones.config import PipelineConfig


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Databricks pipeline for Proyecto Cotizaciones")
    parser.add_argument("--source-table", default="opx.p_ddv_opx.afp_certificados")
    parser.add_argument("--target-table", default="opx.p_ddv_opx.afp_certificados_output")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--row-limit", type=int, default=240)
    parser.add_argument("--request-timeout-secs", type=int, default=30)
    parser.add_argument("--request-sleep-secs", type=float, default=1.0)
    parser.add_argument("--enable-selenium", action="store_true")
    parser.add_argument("--selenium-driver-path", default="/databricks/driver/chromedriver")
    parser.add_argument("--selenium-download-dir", default="/dbfs/tmp/proyecto_cotizaciones/downloads")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = PipelineConfig(
        source_table=args.source_table,
        target_table=args.target_table,
        lookback_days=args.lookback_days,
        row_limit=args.row_limit,
        request_timeout_secs=args.request_timeout_secs,
        request_sleep_secs=args.request_sleep_secs,
        enable_selenium=args.enable_selenium,
        selenium_driver_path=args.selenium_driver_path,
        selenium_download_dir=args.selenium_download_dir,
        log_level=args.log_level,
    )

    from pyspark.sql import SparkSession
    from proyecto_cotizaciones.pipeline import run_pipeline

    spark = SparkSession.builder.getOrCreate()
    inserted_rows = run_pipeline(spark=spark, config=config)
    logging.getLogger(__name__).info("Pipeline finalizada. Registros insertados: %s", inserted_rows)


if __name__ == "__main__":
    main()
