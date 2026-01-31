# Statement & Settlement Reconciliation

Python web application to reconcile Statement and Settlement files with an upload interface and view for transactions classified as 5, 6, and 7.

## Requirements

- Python 3.8+
- pandas, openpyxl, Flask

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python app.py
```

Open http://127.0.0.1:5000 in your browser.

## Features

1. **Upload Interface** – Upload Statement (.xlsx/.xls) and Settlement (.xlsx/.xls) files.
2. **Reconciliation Logic** – Processes both files as per specification:
   - Statement: Drops rows 1–9 and 11, extracts partner PIN from Col D, tags duplicated/non-duplicated.
   - Settlement: Drops rows 1–2, adds Amount (USD) = PayoutRoundAmt ÷ APIRate, tags transactions.
   - Matching by Partner PIN for "Should Reconcile" entries.
3. **Classified View** – Page showing transactions with:
   - **5** = Present in Both (Statement & Settlement)
   - **6** = Present in Settlement but not in Statement
   - **7** = Present in Statement but not in Settlement

## GitHub Repo

Push this project to a GitHub repository and share the URL for submission.

```bash
git init
git add .
git commit -m "Initial commit: Reconciliation app"
git remote add origin <your-repo-url>
git push -u origin main
```
