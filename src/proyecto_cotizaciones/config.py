from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    source_table: str = "opx.p_ddv_opx.afp_certificados"
    target_table: str = "opx.p_ddv_opx.afp_certificados_output"
    lookback_days: int = 30
    row_limit: int = 240
    request_timeout_secs: int = 30
    request_sleep_secs: float = 1.0
    enable_selenium: bool = False
    selenium_driver_path: str = "/databricks/driver/chromedriver"
    selenium_download_dir: str = "/dbfs/tmp/proyecto_cotizaciones/downloads"
    log_level: str = "INFO"
