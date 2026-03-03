# Evaluation Guide (Recruiter / Reviewer)

## 1) Run
```bash
python -m pip install -r requirements.txt
python main.py
```

## 2) Open This First
- `data_docs/index.html`

If opening directly is blocked by browser security, run:
```bash
python -m http.server 5500
```
Then open:
- `http://127.0.0.1:5500/data_docs/index.html`

Use this order:
1. Decision
2. Top Issues
3. Fix First
4. Issues (Row + ID)
5. Fix Rows (Highlight = Issue)

## 3) What To Check
- Decision shows `READY` or `NOT READY`
- Exact row numbers and order IDs are visible
- Bad values are highlighted in yellow
- Suggested fixes are clear and actionable

## 4) Assignment Requirement Mapping
1. Expectations for type/range/null/unique:
- `great_expectations/expectations/sales_data_suite.json`

2. Validate sample CSV + data docs:
- input: `data/sample_sales.csv`
- docs: `data_docs/index.html` and `data_docs/local_site/index.html`

3. ETL + validation integration:
- `etl_with_validation.py`
- `main.py`

4. Violation summary:
- `reports/validation_summary.json`
- `reports/validation_summary.md`
- `reports/checkpoint_result.json`

## 5) Expected Demo Outcome
Sample CSV intentionally contains quality issues.
So `NOT READY` is expected in demo run.

## 6) Quick Checklist
- [ ] `python main.py` runs without error
- [ ] `data_docs/index.html` opens
- [ ] row/ID level issues are visible
- [ ] `reports/validation_summary.json` is generated
