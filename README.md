# Proyecto_cotizaciones (Databricks Azure)

Refactor Lift & Shift del flujo `CERT AFP SCRAP` para ejecución en Databricks Azure.

## Cambios principales

- Flujo modular en `proyecto_cotizaciones/`.
- Eliminación de CSV intermedios como mecanismo de proceso.
- Persistencia final directa a tabla Delta.
- Lectura de origen desde tabla SQL en Databricks (`source_table`).
- Ejecución con un único entrypoint (`run_pipeline.py`) para Jobs/Notebooks.
- Diagnóstico automático cuando no hay candidatos (`Registros candidatos: 0`).

## Estructura

```text
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
python run_pipeline.py \
  --source-table opx.p_ddv_opx.afp_certificados \
  --target-table opx.p_ddv_opx.afp_certificados_output \
  --lookback-days 30 \
  --row-limit 240
```

Parámetros opcionales:

- `--enable-selenium` para AFP que requieren automatización de navegador (Modelo/Cuprum/Capital).
- `--selenium-driver-path` ruta del chromedriver en el cluster.
- `--selenium-download-dir` directorio temporal en DBFS para descargas del navegador.
- `--disable-filter-processed` desactiva la exclusión de ids ya insertados (reproceso/debug).
- `--disable-candidate-diagnostics` desactiva conteos de diagnóstico cuando no hay candidatos.

## Tabla de salida esperada

La tabla destino contiene:

- `DOC_IDN`, `LINK`, `PERIODO_PRODUCCION`, `FECHA_INGRESO`
- `METADATA_CREATOR`, `METADATA_PRODUCER`, `METADATA_CREADATE`, `METADATA_MODDATE`
- `ES_METADATA`, `AFP`, `ES_CERT_COT`, `CODVER`, `RUT`, `RUT_L11`
- `RES_AFP`, `ES_DIF`, `RES_DIF`, `FECHA_PROCESO`

## Cuando aparezca `Registros candidatos: 0`

El pipeline ahora registra diagnóstico para distinguir:

1. tabla fuente vacía,
2. ventana de fechas (`lookback_days`) sin datos,
3. registros ya cargados y excluidos por `left_anti` contra la tabla destino.
