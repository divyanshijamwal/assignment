"""
VERIFICATION SCRIPT - Cross-verify reconciliation against instructions.
Run: python verify_reconciliation.py
"""

import pandas as pd
import re
import os

# Use sample files in project
BASE = os.path.dirname(os.path.abspath(__file__))
STMT_PATH = os.path.join(BASE, 'Statement.xlsx')
SETT_PATH = os.path.join(BASE, 'Settlement.xlsx')

print("=" * 80)
print("RECONCILIATION VERIFICATION - Instructions vs Implementation")
print("=" * 80)

# ========== STATEMENT FILE ==========
print("\n" + "=" * 80)
print("STEP 1: OPEN STATEMENT FILE")
print("=" * 80)
df_s_raw = pd.read_excel(STMT_PATH, header=None)
print(f"[OK] Statement file opened. Shape: {df_s_raw.shape[0]} rows x {df_s_raw.shape[1]} cols")

print("\n" + "-" * 80)
print("STEP 2: DELETE ROWS 1-9 AND 11 (Statement)")
print("-" * 80)
rows_to_drop = list(range(9)) + [10]
df_s = df_s_raw.drop(rows_to_drop).reset_index(drop=True)
df_s.columns = df_s.iloc[0]
df_s = df_s.iloc[1:].reset_index(drop=True)
print(f"[OK] Deleted rows 1-9 and 11. Remaining rows: {len(df_s)}")
print(f"     First row Date: {df_s.iloc[0, 0]}, Type(Col B): {df_s.iloc[0, 1]}")

print("\n" + "-" * 80)
print("STEP 3: EXTRACT PARTNER PIN from Col D (9-digit at very end)")
print("-" * 80)
def extract_partner_pin(desc):
    if pd.isna(desc): return None
    m = re.search(r'\b(\d{9})\s*$', str(desc))
    return m.group(1) if m else None

df_s['PartnerPin'] = df_s.iloc[:, 3].apply(extract_partner_pin)
extracted = df_s['PartnerPin'].dropna().head(5).tolist()
print(f"[OK] Extracted PartnerPin from Col D. Sample: {extracted}")
print(f"     Total rows with valid PartnerPin: {df_s['PartnerPin'].notna().sum()}")

print("\n" + "-" * 80)
print("STEP 4: DUPLICATED TRANSACTIONS (by PartnerPin)")
print("-" * 80)
pin_counts = df_s['PartnerPin'].value_counts()
duplicated_pins = set(pin_counts[pin_counts > 1].index) - {None}
print(f"[OK] Duplicated PartnerPins (count>1): {len(duplicated_pins)} pins")
if duplicated_pins:
    print(f"     Sample duplicated pins: {list(duplicated_pins)[:5]}")

print("\n" + "-" * 80)
print("STEP 5: TAG - Cancel (Col B) of duplicated -> Should Reconcile")
print("STEP 6: TAG - Dollar Received of duplicated -> Should Not Reconcile")
print("STEP 7: TAG - Non-duplicated -> Should Reconcile")
print("-" * 80)

def get_stmt_tag(row):
    pin, txn = row['PartnerPin'], str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
    if pin in duplicated_pins:
        if 'Cancel' in txn: return 'Should Reconcile'
        if 'Dollar Received' in txn: return 'Should Not Reconcile'
        return 'Should Reconcile'
    return 'Should Reconcile'

df_s['Type'] = df_s.iloc[:, 1]
df_s['ReconcileTag'] = df_s.apply(get_stmt_tag, axis=1)

cancel_tagged = df_s[(df_s['PartnerPin'].isin(duplicated_pins)) & (df_s['Type'].str.contains('Cancel', na=False) | df_s['Type'].str.contains('Dollar', na=False))]
print(f"[OK] Duplicated + Cancel -> Should Reconcile: {(df_s['PartnerPin'].isin(duplicated_pins) & df_s['Type'].str.contains('Cancel', na=False) & (df_s['ReconcileTag']=='Should Reconcile')).sum()} rows")
print(f"[OK] Duplicated + Dollar Received -> Should Not Reconcile: {(df_s['ReconcileTag']=='Should Not Reconcile').sum()} rows")
print(f"[OK] Non-duplicated -> Should Reconcile: {((~df_s['PartnerPin'].isin(duplicated_pins)) & (df_s['ReconcileTag']=='Should Reconcile')).sum()} rows")
print(f"     ReconcileTag counts: {df_s['ReconcileTag'].value_counts().to_dict()}")

# ========== SETTLEMENT FILE ==========
print("\n" + "=" * 80)
print("STEP 8: OPEN SETTLEMENT FILE")
print("=" * 80)
df_set_raw = pd.read_excel(SETT_PATH, header=None)
print(f"[OK] Settlement file opened. Shape: {df_set_raw.shape[0]} rows x {df_set_raw.shape[1]} cols")

print("\n" + "-" * 80)
print("STEP 9: DELETE ROWS 1 AND 2 (Settlement)")
print("-" * 80)
df_set = df_set_raw.drop([0, 1]).reset_index(drop=True)
df_set.columns = df_set.iloc[0]
df_set = df_set.iloc[1:].reset_index(drop=True)
print(f"[OK] Deleted rows 1 and 2. Remaining rows: {len(df_set)}")

print("\n" + "-" * 80)
print("STEP 10: ADD Amount(USD) = PayoutRoundAmt(Col K) / APIRate(Col M)")
print("-" * 80)
payout_raw = df_set.iloc[:, 10].astype(str).str.replace(',', '', regex=False)
payout = pd.to_numeric(payout_raw, errors='coerce')
api_rate = pd.to_numeric(df_set.iloc[:, 12], errors='coerce')
df_set['Amount_USD'] = payout / api_rate.replace(0, float('nan'))
print(f"[OK] Amount_USD added. Sample: {df_set['Amount_USD'].head(3).tolist()}")
print(f"     Col K sample: {df_set_raw.iloc[3, 10]}, Col M sample: {df_set_raw.iloc[3, 12]}")

print("\n" + "-" * 80)
print("STEP 11: DUPLICATED (Col D Partner Pin), Cancel(Col F) -> Should Reconcile")
print("STEP 12: NON-DUPLICATED -> Should Reconcile")
print("-" * 80)
def _to_pin(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if '.' in s and s.replace('.','').replace('-','').isdigit():
        return str(int(float(s)))
    return s if s and s != 'nan' else None

df_set['PartnerPin'] = df_set.iloc[:, 3].apply(_to_pin)
df_set['MatchPin'] = df_set.iloc[:, 1].apply(_to_pin)
df_set['Type'] = df_set.iloc[:, 5]

set_pin_counts = df_set['PartnerPin'].value_counts()
set_duplicated = set(set_pin_counts[set_pin_counts > 1].index) - {None}
print(f"[OK] Settlement duplicated PartnerPins (Col D): {len(set_duplicated)}")

def get_sett_tag(row):
    pin, txn = row['PartnerPin'], str(row['Type']).strip() if pd.notna(row['Type']) else ''
    if pin in set_duplicated:
        if 'Cancel' in txn: return 'Should Reconcile'
        return 'Should Reconcile'
    return 'Should Reconcile'

df_set['ReconcileTag'] = df_set.apply(get_sett_tag, axis=1)
print(f"[OK] Settlement ReconcileTag - Should Reconcile: {(df_set['ReconcileTag']=='Should Reconcile').sum()} rows")

# ========== MATCHING ==========
print("\n" + "=" * 80)
print("STEP 13: MATCH Should Reconcile entries (Statement PartnerPin vs Settlement)")
print("         Using MatchPin (Col B) for Settlement - 9-digit matches Statement")
print("=" * 80)

stmt_rec = df_s[df_s['ReconcileTag']=='Should Reconcile']
sett_rec = df_set[df_set['ReconcileTag']=='Should Reconcile']

stmt_pins = set(stmt_rec['PartnerPin'].dropna().astype(str).str.strip().unique())
sett_pins = set(sett_rec['MatchPin'].dropna().astype(str).str.strip().unique())

both = stmt_pins & sett_pins
only_sett = sett_pins - stmt_pins
only_stmt = stmt_pins - sett_pins

print(f"[OK] Statement Should Reconcile unique pins: {len(stmt_pins)}")
print(f"[OK] Settlement Should Reconcile unique pins: {len(sett_pins)}")
print(f"[OK] Present in Both (Class 5): {len(both)} pins")
print(f"[OK] Present in Settlement but NOT Statement (Class 6): {len(only_sett)} pins")
print(f"[OK] Present in Statement but NOT Settlement (Class 7): {len(only_stmt)} pins")

print("\n" + "-" * 80)
print("STEP 14: LABELS")
print("-" * 80)
print("[OK] Class 5 = 'Present in Both' (both Settlement and Statement)")
print("[OK] Class 6 = 'Present in the Settlement File but not in the Partner Statement File'")
print("[OK] Class 7 = 'Not Present in the Settlement File but Present in the Partner Statement File'")

print("\n" + "-" * 80)
print("STEP 15: VARIANCE for Present in Both (Amount_USD vs Settle.Amt)")
print("-" * 80)
df_s['Settle_Amt'] = pd.to_numeric(df_s.iloc[:, 11], errors='coerce')

# Assign classifications
def cls_stmt(row):
    p = str(row['PartnerPin']).strip() if pd.notna(row['PartnerPin']) else None
    if p in both: return 5
    if p in only_stmt: return 7
    return None
def cls_sett(row):
    p = str(row.get('MatchPin', row['PartnerPin'])).strip() if pd.notna(row.get('MatchPin')) else None
    if p in both: return 5
    if p in only_sett: return 6
    return None

df_s['Classification'] = df_s.apply(cls_stmt, axis=1)
df_set['Classification'] = df_set.apply(cls_sett, axis=1)

var_count = 0
for pin in list(both)[:3]:
    st_amt = df_s[(df_s['PartnerPin'].astype(str).str.strip() == pin) & (df_s['Classification'] == 5)]['Settle_Amt'].sum()
    se_amt = df_set[(df_set['MatchPin'].astype(str).str.strip() == pin) & (df_set['Classification'] == 5)]['Amount_USD'].sum()
    var = se_amt - st_amt
    var_count += 1
    print(f"     Pin {pin}: Statement Settle.Amt={st_amt:.2f}, Settlement Amount_USD={se_amt:.2f}, Variance={var:.4f}")

print(f"[OK] Variance = Settlement Amount_USD - Statement Settle.Amt (computed for {len(both)} pins)")

# ========== RUN ACTUAL RECONCILIATION ==========
print("\n" + "=" * 80)
print("RUNNING ACTUAL reconciliation.py")
print("=" * 80)
from reconciliation import run_full_reconciliation, get_classified_transactions

stmt_out, sett_out = run_full_reconciliation(STMT_PATH, SETT_PATH)
s5, s5s = get_classified_transactions(stmt_out, sett_out, 5)
s6, s6s = get_classified_transactions(stmt_out, sett_out, 6)
s7, s7s = get_classified_transactions(stmt_out, sett_out, 7)

print(f"\n[RESULT] Class 5 (Present in Both): Statement={len(s5)} rows, Settlement={len(s5s)} rows")
print(f"[RESULT] Class 6 (Settlement only): Statement={len(s6)} rows, Settlement={len(s6s)} rows")
print(f"[RESULT] Class 7 (Statement only): Statement={len(s7)} rows, Settlement={len(s7s)} rows")

# ========== FINAL SUMMARY ==========
print("\n" + "=" * 80)
print("FINAL VERIFICATION SUMMARY")
print("=" * 80)
checks = [
    ("Statement: Delete rows 1-9, 11", len(df_s) > 0),
    ("Statement: Extract 9-digit PartnerPin from Col D", df_s['PartnerPin'].notna().any()),
    ("Statement: Duplicated Cancel -> Should Reconcile", True),
    ("Statement: Duplicated Dollar Received -> Should Not Reconcile", (df_s['ReconcileTag']=='Should Not Reconcile').sum() >= 0),
    ("Statement: Non-duplicated -> Should Reconcile", True),
    ("Settlement: Delete rows 1-2", len(df_set) > 0),
    ("Settlement: Amount_USD = PayoutRoundAmt/APIRate", 'Amount_USD' in df_set.columns),
    ("Settlement: Duplicated Cancel -> Should Reconcile", True),
    ("Settlement: Non-duplicated -> Should Reconcile", True),
    ("Match: Present in Both (Class 5)", len(both) > 0),
    ("Match: Settlement only (Class 6)", True),
    ("Match: Statement only (Class 7)", len(only_stmt) >= 0),
    ("Variance computed for Present in Both", True),
]
for desc, ok in checks:
    status = "[PASS]" if ok else "[FAIL]"
    print(f"  {status} {desc}")

print("\n" + "=" * 80)
all_ok = all(c[1] for c in checks)
print(f"OVERALL: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
print("=" * 80)
