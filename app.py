"""
Flask web application for Statement and Settlement file reconciliation.
"""

import os
import uuid
from flask import Flask, request, render_template, redirect, url_for, flash
from werkzeug.utils import secure_filename
from reconciliation import run_full_reconciliation, get_classified_transactions
import pandas as pd

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['RESULTS_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads', 'results')
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    """Upload page for Statement and Settlement files."""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file upload and run reconciliation."""
    statement_file = request.files.get('statement')
    settlement_file = request.files.get('settlement')
    
    if not statement_file or not statement_file.filename:
        flash('Please upload a Statement file.', 'error')
        return redirect(url_for('index'))
    if not settlement_file or not settlement_file.filename:
        flash('Please upload a Settlement file.', 'error')
        return redirect(url_for('index'))
    
    if not allowed_file(statement_file.filename) or not allowed_file(settlement_file.filename):
        flash('Only Excel files (.xlsx, .xls) are allowed.', 'error')
        return redirect(url_for('index'))
    
    try:
        stmt_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(statement_file.filename))
        sett_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(settlement_file.filename))
        statement_file.save(stmt_path)
        settlement_file.save(sett_path)
        
        statement_df, settlement_df = run_full_reconciliation(stmt_path, sett_path)
        
        # Save to files (session cookie has 4KB limit - data is too large)
        result_id = str(uuid.uuid4())
        stmt_file = os.path.join(app.config['RESULTS_FOLDER'], f'{result_id}_stmt.pkl')
        sett_file = os.path.join(app.config['RESULTS_FOLDER'], f'{result_id}_sett.pkl')
        statement_df.to_pickle(stmt_file)
        settlement_df.to_pickle(sett_file)
        
        flash('Files processed successfully! View classified transactions below.', 'success')
        return redirect(url_for('classified', rid=result_id))
        
    except Exception as e:
        flash(f'Error processing files: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/classified')
def classified():
    """Page showing transactions classified as 5, 6, and 7."""
    result_id = request.args.get('rid')
    if not result_id:
        flash('Please upload and process Statement and Settlement files first.', 'warning')
        return redirect(url_for('index'))
    
    stmt_file = os.path.join(app.config['RESULTS_FOLDER'], f'{result_id}_stmt.pkl')
    sett_file = os.path.join(app.config['RESULTS_FOLDER'], f'{result_id}_sett.pkl')
    if not os.path.exists(stmt_file) or not os.path.exists(sett_file):
        flash('Results expired or invalid. Please upload files again.', 'warning')
        return redirect(url_for('index'))
    
    statement_df = pd.read_pickle(stmt_file)
    settlement_df = pd.read_pickle(sett_file)
    
    stmt_5_6_7, sett_5_6_7 = get_classified_transactions(statement_df, settlement_df, [5, 6, 7])
    
    # Split by classification for display
    stmt_by_class = {5: stmt_5_6_7[stmt_5_6_7['Classification'] == 5],
                     6: stmt_5_6_7[stmt_5_6_7['Classification'] == 6],
                     7: stmt_5_6_7[stmt_5_6_7['Classification'] == 7]}
    sett_by_class = {5: sett_5_6_7[sett_5_6_7['Classification'] == 5],
                     6: sett_5_6_7[sett_5_6_7['Classification'] == 6],
                     7: sett_5_6_7[sett_5_6_7['Classification'] == 7]}
    
    return render_template(
        'classified.html',
        stmt_5=stmt_by_class[5].to_html(classes='table table-striped', index=False) if not stmt_by_class[5].empty else '<p>No transactions</p>',
        stmt_6=stmt_by_class[6].to_html(classes='table table-striped', index=False) if not stmt_by_class[6].empty else '<p>No transactions (Statement has no Class 6)</p>',
        stmt_7=stmt_by_class[7].to_html(classes='table table-striped', index=False) if not stmt_by_class[7].empty else '<p>No transactions</p>',
        sett_5=sett_by_class[5].to_html(classes='table table-striped', index=False) if not sett_by_class[5].empty else '<p>No transactions</p>',
        sett_6=sett_by_class[6].to_html(classes='table table-striped', index=False) if not sett_by_class[6].empty else '<p>No transactions</p>',
        sett_7=sett_by_class[7].to_html(classes='table table-striped', index=False) if not sett_by_class[7].empty else '<p>No transactions (Settlement has no Class 7)</p>',
        labels={
            5: 'Present in Both',
            6: 'Present in Settlement but not in Statement',
            7: 'Present in Statement but not in Settlement'
        }
    )


if __name__ == '__main__':
    app.run(debug=True, port=5000)
