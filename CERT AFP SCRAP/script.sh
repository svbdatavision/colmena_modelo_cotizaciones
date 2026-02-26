#!/bin/bash
echo "--- Ejecutando proceso..."
echo $(date)
cd /home/certafp/proyecto_cotizaciones
export PYTHONWARNINGS="ignore"
python='/home/certafp/proyecto_cotizaciones/venv/bin/python'
$python src/1_extract.py && $python src/2_download.py && $python src/3_parse.py && $python src/4_afp.py && $python src/5_compare.py && $python src/6_upload.py
echo $(date)
echo "--- Proceso terminado"
