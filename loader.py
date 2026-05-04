"""
Data loader for luxury resale retailer.
Pulls Shopify and Traxia POS files from Google Drive, normalizes,
and upserts to Supabase.
"""

import os
import io
import json
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account


# ============================================================
# CONFIG
# ============================================================

DB_URL = os.environ['DATABASE_URL']
GOOGLE_CREDS_JSON = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']
ERRORS_FOLDER_ID = os.environ['ERRORS_FOLDER_ID']

SOURCES = {
    'shopify': {
        'inbox_folder_id': os.environ['SHOPIFY_INBOX_ID'],
        'processed_folder_id': os.environ['SHOPIFY_PROCESSED_ID'],
        'target_table': 'shopify_orders',
        'expected_columns': [
            'Order ID', 'SKU', 'Customer Full Name', 'Address',
            'City', 'State', 'Zip', 'Item Description',
            'Created Date', 'Transaction Date', 'Price',
            'Consigner Split', 'Cost', 'Category', 'Terminal'
        ],
    },
    'pos': {
        'inbox_folder_id': os.environ['POS_INBOX_ID'],
        'processed_folder_id': os.environ['POS_PROCESSED_ID'],
        'target_table': 'pos_orders',
        'expected_columns': [
            'Order ID', 'SKU', 'Customer Full Name', 'Address',
            'City', 'Region', 'Zip', 'Item Description',
            'Created Date', 'Transaction Date', 'Price',
            'Consigner Split', 'Cost', 'Category', 'Terminal'
        ],
    }
}


# ============================================================
# GOOGLE DRIVE HELPERS
# ============================================================

def get_drive_service():
    creds_info = json.loads(GOOGLE_CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def list_files_in_folder(drive, folder_id):
    results = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, mimeType)",
        pageSize=100
    ).execute()
    return results.get('files', [])


def download_file(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer


def move_file(drive, file_id, target_folder_id):
    file = drive.files().get(fileId=file_id, fields='parents').execute()
    prev_parents = ",".join(file.get('parents', []))
    drive.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=prev_parents,
        fields='id, parents'
    ).execute()


# ============================================================
# FILE READING
# ============================================================

def read_file_to_df(buffer, filename):
    name_lower = filename.lower()
    if name_lower.endswith('.csv'):
        return pd.read_csv(buffer)
    elif name_lower.endswith(('.xlsx', '.xls')):
        return pd.read_excel(buffer)
    else:
        raise ValueError(f"Unsupported file type: {filename}")


def validate_columns(df, expected, source_name):
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"{source_name} file missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )


# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def to_cents(value):
    """Convert dollar amount to integer cents. Returns None if blank."""
    if pd.isna(value) or value == '':
        return None
    if isinstance(value, str):
        value = value.replace('$', '').replace(',', '').strip()
        if not value:
            return None
    return int(round(float(value) * 100))


def to_text(value):
    """Convert to clean text, returning None for blanks."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def to_zip(value):
    """ZIPs must be text to preserve leading zeros."""
    if pd.isna(value):
        return None
    # Handle floats like 5678.0 from Excel reading numeric ZIPs
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip() or None


def to_date(value):
    """Parse to a date. Returns None on failure."""
    if pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


# ============================================================
# NORMALIZERS — convert raw exports to standard schema
# ============================================================

def normalize_shopify(df):
    """Map Shopify export columns to shopify_orders schema."""
    out = pd.DataFrame()
    out['order_id'] = df['Order ID'].astype(str).str.strip()
    out['sku'] = df['SKU'].apply(to_text)
    out['customer_full_name'] = df['Customer Full Name'].apply(to_text)
    out['address'] = df['Address'].apply(to_text)
    out['city'] = df['City'].apply(to_text)
    out['state'] = df['State'].apply(to_text)
    out['zip'] = df['Zip'].apply(to_zip)
    out['item_description'] = df['Item Description'].apply(to_text)
    out['created_date'] = df['Created Date'].apply(to_date)
    out['transaction_date'] = df['Transaction Date'].apply(to_date)
    out['price_cents'] = df['Price'].apply(to_cents)
    out['consigner_split_cents'] = df['Consigner Split'].apply(to_cents)
    out['cost_cents'] = df['Cost'].apply(to_cents)
    out['category'] = df['Category'].apply(to_text)
    out['terminal'] = df['Terminal'].apply(to_text)

    # Drop rows with no order_id or no transaction date
    out = out[out['order_id'].notna() & (out['order_id'] != '') 
              & (out['order_id'] != 'nan')]
    out = out[out['transaction_date'].notna()]
    return out


def normalize_pos(df):
    """Map Traxia POS export columns to pos_orders schema.
    
    Note: source uses 'Region', we normalize to 'state' to match Shopify.
    """
    out = pd.DataFrame()
    out['order_id'] = df['Order ID'].astype(str).str.strip()
    out['sku'] = df['SKU'].apply(to_text)
    out['customer_full_name'] = df['Customer Full Name'].apply(to_text)
    out['address'] = df['Address'].apply(to_text)
    out['city'] = df['City'].apply(to_text)
    out['state'] = df['Region'].apply(to_text)  # Region -> state
    out['zip'] = df['Zip'].apply(to_zip)
    out['item_description'] = df['Item Description'].apply(to_text)
    out['created_date'] = df['Created Date'].apply(to_date)
    out['transaction_date'] = df['Transaction Date'].apply(to_date)
    out['price_cents'] = df['Price'].apply(to_cents)
    out['consigner_split_cents'] = df['Consigner Split'].apply(to_cents)
    out['cost_cents'] = df['Cost'].apply(to_cents)
    out['category'] = df['Category'].apply(to_text)
    out['terminal'] = df['Terminal'].apply(to_text)

    out = out[out['order_id'].notna() & (out['order_id'] != '') 
              & (out['order_id'] != 'nan')]
    out = out[out['transaction_date'].notna()]
    return out


NORMALIZERS = {
    'shopify': normalize_shopify,
    'pos': normalize_pos,
}


# ============================================================
# DATABASE OPERATIONS
# ============================================================

def upsert_orders(engine, df, source_filename, target_table):
    """
    Bulk upsert using a temp table + COPY for speed.
    Handles 10k+ rows in seconds vs minutes.
    """
    if len(df) == 0:
        return 0, 0

    df = df.copy()
    df['source_file'] = source_filename

    # Column order must match the target table
    columns = [
        'order_id', 'sku', 'customer_full_name', 'address',
        'city', 'state', 'zip', 'item_description',
        'created_date', 'transaction_date',
        'price_cents', 'consigner_split_cents', 'cost_cents',
        'category', 'terminal', 'source_file'
    ]
    df = df[columns]

    # Build CSV in memory for COPY
    import csv
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    for _, row in df.iterrows():
        writer.writerow([
            '' if pd.isna(v) else v for v in row.tolist()
        ])
    buffer.seek(0)

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            # Create a temp table matching the target table's structure
            cur.execute(f"""
                CREATE TEMP TABLE _staging (LIKE {target_table} INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)

            # Bulk COPY into staging — this is the fast part
            cur.copy_expert(
                f"COPY _staging ({', '.join(columns)}) FROM STDIN WITH CSV NULL ''",
                buffer
            )

            # Single bulk upsert from staging into the real table
            non_key_columns = [c for c in columns if c != 'order_id']
            update_clause = ', '.join(
                f"{c} = EXCLUDED.{c}" for c in non_key_columns
            )

            cur.execute(f"""
                WITH upserted AS (
                    INSERT INTO {target_table} ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM _staging
                    ON CONFLICT (order_id) DO UPDATE SET
                        {update_clause},
                        loaded_at = NOW()
                    RETURNING (xmax = 0) AS was_inserted
                )
                SELECT
                    SUM(CASE WHEN was_inserted THEN 1 ELSE 0 END) AS inserted,
                    SUM(CASE WHEN NOT was_inserted THEN 1 ELSE 0 END) AS updated
                FROM upserted
            """)

            result = cur.fetchone()
            inserted = int(result[0] or 0)
            updated = int(result[1] or 0)

            raw_conn.commit()
        finally:
            cur.close()
    finally:
        raw_conn.close()

    return inserted, updated


def log_upload(engine, filename, source, inserted, updated, df, status, error=None):
    start_date = None
    end_date = None
    if len(df) > 0 and 'transaction_date' in df.columns:
        valid_dates = pd.Series([d for d in df['transaction_date'] if d is not None])
        if len(valid_dates) > 0:
            start_date = valid_dates.min()
            end_date = valid_dates.max()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO upload_log
            (filename, source, rows_inserted, rows_updated, rows_skipped,
             date_range_start, date_range_end, status, error_message)
            VALUES (:filename, :source, :inserted, :updated, 0,
                    :start_date, :end_date, :status, :error)
        """), {
            'filename': filename,
            'source': source,
            'inserted': inserted,
            'updated': updated,
            'start_date': start_date,
            'end_date': end_date,
            'status': status,
            'error': error[:1000] if error else None,
        })


# ============================================================
# MAIN PROCESSING LOOP
# ============================================================

def process_file(drive, engine, file, source_name, config):
    filename = file['name']
    print(f"  Processing: {filename}")

    try:
        buffer = download_file(drive, file['id'])
        df_raw = read_file_to_df(buffer, filename)
        print(f"    Read {len(df_raw)} raw rows")

        validate_columns(df_raw, config['expected_columns'], source_name)

        normalizer = NORMALIZERS[source_name]
        df_normalized = normalizer(df_raw)
        print(f"    Normalized to {len(df_normalized)} valid rows")

        inserted, updated = upsert_orders(
            engine, df_normalized, filename, config['target_table']
        )

        log_upload(engine, filename, source_name, inserted, updated,
                   df_normalized, 'success')
        move_file(drive, file['id'], config['processed_folder_id'])
        print(f"    ✓ Inserted {inserted}, updated {updated}")
        return True

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"    ✗ Failed: {error_msg}")

        try:
            log_upload(engine, filename, source_name, 0, 0,
                       pd.DataFrame({'transaction_date': []}), 'error', error_msg)
            move_file(drive, file['id'], ERRORS_FOLDER_ID)
        except Exception as log_error:
            print(f"    ✗ Additionally failed to log/move: {log_error}")

        return False


def process_source(drive, engine, source_name, config):
    print(f"\n=== {source_name.upper()} ===")
    files = list_files_in_folder(drive, config['inbox_folder_id'])

    if not files:
        print("  No files in inbox.")
        return 0, 0

    print(f"  Found {len(files)} file(s)")
    successes = 0
    failures = 0

    for file in files:
        if process_file(drive, engine, file, source_name, config):
            successes += 1
        else:
            failures += 1

    return successes, failures


def main():
    print(f"=== Data Loader Run: {datetime.utcnow().isoformat()}Z ===")

    drive = get_drive_service()
    engine = create_engine(DB_URL, pool_pre_ping=True)

    total_success = 0
    total_failure = 0

    for source_name, config in SOURCES.items():
        s, f = process_source(drive, engine, source_name, config)
        total_success += s
        total_failure += f

    print(f"\n=== Summary: {total_success} succeeded, {total_failure} failed ===")

    if total_failure > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
