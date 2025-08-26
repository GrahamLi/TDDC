## TDCC Data Analysis & Visualization System

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Program 1: Fetch TDCC data

Build local database under `data/` with a folder per stock and one CSV per date.

```bash
python program1_fetch_tdcc.py --out data --since 365 --concurrency 4
# Or specific codes
python program1_fetch_tdcc.py --out data --since 365 --codes 2330 2603 8069
```

### Program 2: Query and organize + K-line overlay

Create an Excel with three sheets (`holders`, `shares`, `ratio`) and charts.

```bash
python program2_query_and_organize.py 2330 2024-01-01 2024-12-31 --root data --out out/2330_2024.xlsx
```

### Program 3: Analyze and plot trends (3 Excel files)

Read Program 2 Excel and output three Excel files in `analysis_out/`.

```bash
# Quantity-based classification (share count)
python program3_analyze_and_plot.py out/2330_2024.xlsx 2330 --mode quantity

# Amount-based classification (requires price)
python program3_analyze_and_plot.py out/2330_2024.xlsx 2330 --mode amount --price 650

# Custom bands (share count)
python program3_analyze_and_plot.py out/2330_2024.xlsx 2330 --mode custom --custom 0-30000,30001-100000
```

### Notes
- Websites may change their structure; adjust parsers in the code if needed.
- Program 1 contains conservative fallbacks for TDCC; integrate the official API if available.