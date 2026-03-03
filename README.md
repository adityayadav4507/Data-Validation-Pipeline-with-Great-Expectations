# Data Validation Pipeline with Great Expectations

Python ETL + Data Validation project with automated data quality checks using Great Expectations.

---

## 🚀 Quick Start (How to Run)

1. **Clone the repository**
```bash
git clone https://github.com/adityayadav4507/Data-Validation-Pipeline-with-Great-Expectations.git
cd Data-Validation-Pipeline-with-Great-Expectations
```
2. **Create & activate a virtual environment** (recommended)
   Mac/Linux:
```
python -m venv venv 
source venv/bin/activate
```
   Windows (PowerShell):
```
python -m venv venv
venv\Scripts\Activate.ps1
```
3. **Install dependencies**
```
pip install -r requirements.txt
```
4. **Run the pipeline**
```
python main.py
```
5. **Open the generated dashboard**
```
open data_docs/index.html
```
## Tech Stack
- Python 3.x
- Pandas
- Great Expectations
## Assignment Deliverables (Included)
- Great Expectations project with expectation suite:
  - `great_expectations/expectations/sales_data_suite.json`
- Python ETL + validation workflow:
  - `etl_with_validation.py`
  - `main.py`
- Generated documentation + violation reports:
  - `data_docs/index.html` (clean evaluator dashboard)
  - `data_docs/local_site/index.html` (full Great Expectations docs copy)
  - `reports/validation_summary.json`
  - `reports/validation_summary.md`
  - `reports/checkpoint_result.json`
    
## Project Structure
```text
reaidy/
├─ data/
│  └─ sample_sales.csv
├─ great_expectations/
│  ├─ checkpoints/
│  ├─ expectations/
│  └─ great_expectations.yml
├─ data_docs/
│  ├─ index.html
│  └─ local_site/
├─ reports/
├─ output/
├─ etl_with_validation.py
├─ main.py
├─ requirements.txt
└─ EVALUATION_GUIDE.md
```

## Main File To Review
- `data_docs/index.html`

This is the recruiter-friendly dashboard:
- final decision (`READY` / `NOT READY`)
- top issues
- action plan
- exact row/ID level issue mapping
- highlighted bad values

## Open HTML Report (Optional)
```bash
python -m http.server 5500
```
Then open:
- `http://127.0.0.1:5500/data_docs/index.html`

## Requirement Mapping
1. Define expectations for types, ranges, nulls, uniqueness:
- Implemented in `add_expectations()` in `etl_with_validation.py`
- Persisted in `great_expectations/expectations/sales_data_suite.json`

2. Validate sample CSV and generate data docs:
- Input: `data/sample_sales.csv`
- GX docs: `data_docs/local_site/index.html`

3. Integrate validation in ETL script:
- ETL + validation in `etl_with_validation.py`
- Entry point `main.py`

4. Summary report of violations:
- `reports/validation_summary.json`
- `reports/validation_summary.md`
- `reports/checkpoint_result.json`

## Expected Demo Result
Sample input intentionally has bad data, so output should be `NOT READY`.

Injected issues include:
- duplicate `order_id`
- null values in required columns
- out-of-range numeric values
- invalid category (`payment_method=crypto`)

## Custom Input (Optional)
```bash
python main.py --input data/your_file.csv
```

## Evaluator Guide
See `EVALUATION_GUIDE.md`.

## Upload To GitHub
```bash
git add .
git commit -m "Data Validation Pipeline with Great Expectations"
git push
```

If this is a new repository:
```bash
git init
git branch -M main
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```
