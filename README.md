# Week 2: ETL & EDA Pipeline

This project implements a reproducible ETL pipeline and exploratory data analysis (EDA) for e-commerce data. It processes raw CSVs into clean Parquet files and generates reports on revenue and data quality.

## Project Structure

* `src/bootcamp_data/`: Main Python package with ETL logic.
* `scripts/`: Executable scripts to run the pipeline.
* `data/`: Contains `raw` inputs and `processed` outputs.
* `notebooks/`: Jupyter notebooks for analysis.
* `reports/`: Generated figures and summary markdown.

## Setup

1. **Create and activate a virtual environment:**

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate
```

## How to Run

### 1. Run the ETL Pipeline

This script cleans the data, joins users and orders, and generates metadata.

```bash
python scripts/run_etl.py
```

**Outputs:**

* `data/processed/analytics_table.parquet` (Joined data)
* `data/processed/_run_meta.json` (Run statistics)
* `data/processed/orders_clean.parquet`

### 2. Run the EDA

Open the notebook `notebooks/eda.ipynb` and run all cells to generate charts and the bootstrap analysis.

```bash
jupyter lab notebooks/eda.ipynb
```

**Outputs:**

* Figures saved to `reports/figures/`
