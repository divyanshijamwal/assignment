"""
Reconciliation logic for Statement and Settlement files.
Processes both files, tags transactions, and matches entries.
"""

import re
import pandas as pd


def extract_partner_pin(description):
    """Extract 9-digit partner PIN from end of description string."""
    if pd.isna(description):
        return None
    text = str(description)
    match = re.search(r'\b(\d{9})\s*$', text)
    return match.group(1) if match else None


def process_statement_file(file_path):
    """Process Statement file and return DataFrame with tags."""
    df = pd.read_excel(file_path, header=None)
    
    # Delete rows 1-9 and 11 (0-based: 0-8 and 10)
    rows_to_drop = list(range(9)) + [10]
    df = df.drop(rows_to_drop).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    
    # Col D = index 3 (Descriptions)
    df['PartnerPin'] = df.iloc[:, 3].apply(extract_partner_pin)
    
    # Col B = index 1 (Type)
    df['Type'] = df.iloc[:, 1]
    
    # Col L = index 11 (Settle.Amt)
    df['Settle_Amt'] = df.iloc[:, 11]
    
    # Identify duplicates by PartnerPin
    pin_counts = df['PartnerPin'].value_counts()
    duplicated_pins = set(pin_counts[pin_counts > 1].index) - {None}
    
    def get_statement_tag(row):
        pin = row['PartnerPin']
        txn_type = str(row['Type']).strip() if pd.notna(row['Type']) else ''
        
        if pin in duplicated_pins:
            if 'Cancel' in txn_type:
                return 'Should Reconcile'
            if 'Dollar Received' in txn_type:
                return 'Should Not Reconcile'
            return 'Should Reconcile'
        else:
            return 'Should Reconcile'
    
    df['ReconcileTag'] = df.apply(get_statement_tag, axis=1)
    return df


def process_settlement_file(file_path):
    """Process Settlement file and return DataFrame with tags."""
    df = pd.read_excel(file_path, header=None)
    
    # Delete rows 1 and 2 (0-based: 0 and 1)
    df = df.drop([0, 1]).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    
    # Col K = index 10 (PayoutRoundAmt), Col M = index 12 (APIRate)
    # PayoutRoundAmt may have comma (e.g. "27,239.00")
    payout_raw = df.iloc[:, 10].astype(str).str.replace(',', '', regex=False)
    payout = pd.to_numeric(payout_raw, errors='coerce')
    api_rate = pd.to_numeric(df.iloc[:, 12], errors='coerce')
    df['Amount_USD'] = payout / api_rate.replace(0, float('nan'))
    
    # Col D = index 3 (PartnerPin), Col B = index 1 (Pin Number - 9-digit, matches Statement)
    def _to_pin(x):
        if pd.isna(x): return None
        s = str(x).strip()
        if '.' in s and s.replace('.', '').replace('-', '').isdigit():
            return str(int(float(s)))
        return s if s and s != 'nan' else None
    df['PartnerPin'] = df.iloc[:, 3].apply(_to_pin)
    # MatchPin: Col B (Pin Number) is 9-digit that matches Statement; Col D (PartnerPin) is 11-digit
    df['MatchPin'] = df.iloc[:, 1].apply(_to_pin)
    
    # Col F = index 5 (Type)
    df['Type'] = df.iloc[:, 5]
    
    # Identify duplicates by PartnerPin
    pin_counts = df['PartnerPin'].value_counts()
    duplicated_pins = set(pin_counts[pin_counts > 1].index) - {None}
    
    def get_settlement_tag(row):
        pin = row['PartnerPin']
        txn_type = str(row['Type']).strip() if pd.notna(row['Type']) else ''
        
        if pin in duplicated_pins:
            if 'Cancel' in txn_type:
                return 'Should Reconcile'
            return 'Should Reconcile'
        else:
            return 'Should Reconcile'
    
    df['ReconcileTag'] = df.apply(get_settlement_tag, axis=1)
    return df


def _norm_pin(pin):
    """Normalize pin for matching."""
    if pin is None or (isinstance(pin, float) and pd.isna(pin)):
        return None
    s = str(pin).strip()
    return s if s and s != 'nan' else None


def reconcile_files(statement_df, settlement_df):
    """
    Match Statement and Settlement entries and classify.
    Returns: (statement_df, settlement_df, classification_df)
    Classification 5 = Present in Both
    Classification 6 = Present in Settlement but not in Statement
    Classification 7 = Present in Statement but not in Settlement
    """
    stmt_reconcile = statement_df[statement_df['ReconcileTag'] == 'Should Reconcile'].copy()
    sett_reconcile = settlement_df[settlement_df['ReconcileTag'] == 'Should Reconcile'].copy()
    
    stmt_pins_raw = stmt_reconcile['PartnerPin'].dropna().astype(str).str.strip().unique()
    sett_match_col = 'MatchPin' if 'MatchPin' in sett_reconcile.columns else 'PartnerPin'
    sett_pins_raw = sett_reconcile[sett_match_col].dropna().astype(str).str.strip().unique()
    stmt_pins = set(_norm_pin(p) for p in stmt_pins_raw if _norm_pin(p))
    sett_pins = set(_norm_pin(p) for p in sett_pins_raw if _norm_pin(p))
    
    both = stmt_pins & sett_pins
    only_settlement = sett_pins - stmt_pins
    only_statement = stmt_pins - sett_pins
    
    def get_stmt_classification(row):
        pin = _norm_pin(row['PartnerPin'])
        if pin in both:
            return 5
        if pin in only_statement:
            return 7
        return None
    
    def get_sett_classification(row):
        pin = _norm_pin(row.get('MatchPin', row['PartnerPin']))
        if pin in both:
            return 5
        if pin in only_settlement:
            return 6
        return None
    
    statement_df['Classification'] = statement_df.apply(get_stmt_classification, axis=1)
    settlement_df['Classification'] = settlement_df.apply(get_sett_classification, axis=1)
    
    # Add MatchStatus labels
    statement_df['MatchStatus'] = statement_df.apply(
        lambda r: 'Present in Both' if r['Classification'] == 5
        else ('Not Present in the Settlement File but Present in the Partner Statement File' if r['Classification'] == 7 else None),
        axis=1
    )
    settlement_df['MatchStatus'] = settlement_df.apply(
        lambda r: 'Present in Both' if r['Classification'] == 5
        else ('Present in the Settlement File but not in the Partner Statement File' if r['Classification'] == 6 else None),
        axis=1
    )
    
    # For "Present in Both": compute variance (Settlement Amount_USD vs Statement Settle.Amt)
    statement_df['Variance'] = None
    settlement_df['Variance'] = None
    
    sett_match_col = 'MatchPin' if 'MatchPin' in settlement_df.columns else 'PartnerPin'
    for pin in both:
        stmt_norm = statement_df['PartnerPin'].apply(_norm_pin)
        sett_norm = settlement_df[sett_match_col].apply(_norm_pin)
        stmt_mask = (stmt_norm == pin) & (statement_df['Classification'] == 5)
        sett_mask = (sett_norm == pin) & (settlement_df['Classification'] == 5)
        
        stmt_amt = statement_df.loc[stmt_mask, 'Settle_Amt'].sum()
        sett_amt = settlement_df.loc[sett_mask, 'Amount_USD'].sum()
        
        var = sett_amt - stmt_amt
        statement_df.loc[stmt_mask, 'Variance'] = var
        settlement_df.loc[sett_mask, 'Variance'] = var
    
    return statement_df, settlement_df


def run_full_reconciliation(statement_path, settlement_path):
    """Run full reconciliation pipeline and return all DataFrames."""
    statement_df = process_statement_file(statement_path)
    settlement_df = process_settlement_file(settlement_path)
    statement_df, settlement_df = reconcile_files(statement_df, settlement_df)
    return statement_df, settlement_df


def get_classified_transactions(statement_df, settlement_df, classification=None):
    """
    Get transactions with given classification(s).
    classification: 5, 6, 7, or list [5,6,7]
    """
    if classification is None:
        classification = [5, 6, 7]
    if isinstance(classification, int):
        classification = [classification]
    
    from_stmt = statement_df[statement_df['Classification'].isin(classification)].copy()
    from_sett = settlement_df[settlement_df['Classification'].isin(classification)].copy()
    
    return from_stmt, from_sett
