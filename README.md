PT-VIGIL — Public Procurement Risk Monitor
A lightweight Python + browser dashboard project for detecting suspicious public contract patterns in Portuguese procurement data.

pt_vigil_pipeline.py reads local IMPIC Excel files and normalizes contract fields.

It computes a heuristic risk score using signals like:

direct award / ajuste direto
high contract value
large difference between listed price and effective price
missing CPV codes
repeated awards to the same contractor
concentration of contract value
The pipeline exports cleaned JSON/CSV reports that can be loaded into the dashboard.

pt-vigil-real.html is a static interface that:

loads exported JSON,
shows entities by risk level,
filters by city, entity, NIF, procedure type,
displays national average risk and filtered average risk,
offers detail view with contract flags and score breakdown.
Note: this is a risk analysis tool, not a proof of corruption. It highlights suspicious procurement patterns for further review.

Dependencies
Python 3
openpyxl (the script will try to install it automatically if missing)
The script otherwise uses only Python standard library modules.

Usage

# download and process recent years (2022-2026)python3 pt_vigil_pipeline.py# process only 2024python3 pt_vigil_pipeline.py --anos 2024# process all available years (2012-2026)python3 pt_vigil_pipeline.py --anos todos# only process XLSX files already present under pt_vigil_dados/xlsx/python3 pt_vigil_pipeline.py --so-processar# reset the database before runningpython3 pt_vigil_pipeline.py --limpar
What it produces

The pipeline writes output files into pt_vigil_dados:

export_completo.json

alto_risco.csv

relatorio_resumo.txt

pt_vigil.db

plus downloaded/extracted files under xlsx, zip/, and processado/
Dashboard

Use pt-vigil-real.html to visualize the exported JSON.

Load export_completo.json in the browser dashboard.

The dashboard shows entities by risk level and allows filtering by city, entity, NIF, and procedure type.

Notes
This is a heuristic risk analysis tool, not a proof of corruption.
It flags suspicious procurement patterns such as direct award, high value, price deviation, missing CPV, repeated awards, and concentration of value.


THE DATABASE IS THE export_completo.json BUT I RECOMMEND IT WHENEVER I USE TO UPDATE THE DATABASE WITH SCRIPT pt_vigil_pipeline.py

ALL DATA IS PUBLIC AND ON dados.gov.pt

DON'T TRUST 100% NO CODE
