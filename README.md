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

