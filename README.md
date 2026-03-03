# Proyecto_cotizaciones (Databricks Azure)

Refactor Lift & Shift del flujo `CERT AFP SCRAP` para ejecución en Databricks Azure.

## Cambios principales

- Flujo modular en `src/proyecto_cotizaciones`.
- Eliminación de CSV intermedios como mecanismo de proceso.
- Persistencia final directa a tabla Delta.
- Lectura de origen desde tabla SQL en Databricks (`source_table`).
- Ejecución con un único entrypoint (`src/run_pipeline.py`) para Jobs.

## Estructura

```text
src/
  run_pipeline.py
  proyecto_cotizaciones/
    __init__.py
    config.py
    afp_rules.py
    parsing.py
    pdf_utils.py
    afp_validator.py
    pipeline.py
```

## Parámetros de ejecución

```bash
python src/run_pipeline.py \
  --source-table opx.p_ddv_opx.afp_certificados \
  --target-table opx.p_ddv_opx.afp_certificados_output \
  --lookback-days 30 \
  --row-limit 240
```

Parámetros opcionales:

- `--enable-selenium` para AFP que requieren automatización de navegador (Modelo/Cuprum/Capital).
- `--selenium-driver-path` ruta del chromedriver en el cluster.
- `--selenium-download-dir` directorio temporal en DBFS para descargas del navegador.
- `--disable-filter-processed` para reproceso sin left anti contra tabla salida.
- `--disable-candidate-diagnostics` para apagar logs de diagnóstico de candidatos.

## Tabla de salida esperada

La tabla destino contiene:

- `DOC_IDN`, `LINK`, `PERIODO_PRODUCCION`, `FECHA_INGRESO`
- `METADATA_CREATOR`, `METADATA_PRODUCER`, `METADATA_CREADATE`, `METADATA_MODDATE`
- `ES_METADATA`, `AFP`, `ES_CERT_COT`, `CODVER`, `RUT`, `RUT_L11`
- `RES_AFP`, `ES_DIF`, `RES_DIF`, `FECHA_PROCESO`

## Troubleshooting rápido

Si ves `Registros candidatos: 0`, revisa:

- ventana de fechas (`--lookback-days`),
- datos recientes en tabla fuente (`MAX(FECHA_INGRESO)`),
- deduplicación por `DOC_IDN` contra tabla output.

Puedes forzar reproceso así:

```bash
python src/run_pipeline.py --lookback-days 120 --disable-filter-processed
```
