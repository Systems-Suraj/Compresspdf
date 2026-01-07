import io, os, time, tempfile, requests, sys
from PIL import Image
import fitz  # PyMuPDF
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ===================================================================
# CONFIG
# ===================================================================
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()

# ---- SOURCE ----
SHEET_NAME = os.environ.get("SHEET_NAME", "Dispatch Details").strip()

# ---- TARGET ----
GIT_SHEET_NAME = os.environ.get("GIT_SHEET_NAME", "GIT").strip()

DEST_FOLDER_ID = os.environ.get("DEST_FOLDER_ID", "").strip()

COL_URL     = int(os.environ.get("COL_URL", "9"))   # I
COL_INVOICE = int(os.environ.get("COL_INVOICE", "7"))  # G
START_ROW   = int(os.environ.get("START_ROW", "2"))

# ---- GIT OUTPUT ----
GIT_COL_INVOICE = 1   # A
GIT_COL_URL     = 2   # B
GIT_COL_LOG     = 3   # C

# ---- STRICT SIZE LIMIT ----
MAX_TARGET_BYTES = 100 * 1024   # 100 KB only

TARGET_WIDTH_PT, TARGET_HEIGHT_PT = 595, 842  # A4

START_DPI, MIN_DPI = 150, 72
START_JPEG_QUALITY, MIN_JPEG_QUALITY = 85, 30
DPI_STEP, QUALITY_STEP = 10, 5

DOWNLOAD_TIMEOUT, MAX_ROWS_TO_CHECK = 60, 10000

# ===================================================================
# AUTH
# ===================================================================
def get_clients():
    required = ["GOOGLE_OAUTH_REFRESH_TOKEN", "GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing OAuth secrets: {', '.join(missing)}")

    creds = Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_OAUTH_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_OAUTH_CLIENT_SECRET"],
        scopes=SCOPES,
    )
    creds.refresh(Request())
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

# ===================================================================
# HELPERS
# ===================================================================
def _col_letter(col_idx):
    s = ""
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        s = chr(65 + rem) + s
    return s

def ensure_sheet_grid(sheets_svc, spreadsheet_id, sheet_name, min_cols=10, min_rows=2000):
    ss = sheets_svc.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties").execute()
    sheet = next((s["properties"] for s in ss["sheets"] if s["properties"]["title"] == sheet_name), None)
    if not sheet:
        raise RuntimeError(f"Sheet not found: {sheet_name}")
    sheet_id = sheet["sheetId"]
    gp = sheet.get("gridProperties", {})
    reqs = []
    if gp.get("columnCount", 0) < min_cols:
        reqs.append({"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"columnCount": min_cols}},
            "fields": "gridProperties.columnCount"}})
    if gp.get("rowCount", 0) < min_rows:
        reqs.append({"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"rowCount": min_rows}},
            "fields": "gridProperties.rowCount"}})
    if reqs:
        sheets_svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()

def sheet_get_columns(sheets_svc, spreadsheet_id, sheet_name, cols_and_start):
    result = {}
    for col_idx, start_row in cols_and_start:
        col_letter = _col_letter(col_idx)
        rng = f"{sheet_name}!{col_letter}{start_row}:{col_letter}{MAX_ROWS_TO_CHECK}"
        resp = sheets_svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        vals = resp.get("values", [])
        result[col_idx] = [(r[0].strip() if r and r[0] else "") for r in vals]
    return result

def sheet_update_cell(sheets_svc, spreadsheet_id, sheet_name, row, col, value):
    rng = f"{sheet_name}!{_col_letter(col)}{row}"
    sheets_svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        body={"range": rng, "values": [[value]]}
    ).execute()

def get_existing_git_invoices(sheets_svc, spreadsheet_id, git_sheet):
    rng = f"{git_sheet}!A2:A{MAX_ROWS_TO_CHECK}"
    resp = sheets_svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=rng
    ).execute()
    vals = resp.get("values", [])
    return set(v[0].strip() for v in vals if v and v[0].strip())

def extract_drive_file_id(url):
    import re
    for pat in (r"/file/d/([a-zA-Z0-9_-]{10,})", r"[?&]id=([a-zA-Z0-9_-]{10,})"):
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None

def download_drive_file_by_id(drive_svc, file_id, out_path):
    request = drive_svc.files().get_media(fileId=file_id)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return os.path.getsize(out_path)

def download_url_to_file(drive_svc, url, out_path, timeout=60):
    if "drive.google.com" in url:
        fid = extract_drive_file_id(url)
        if fid:
            try:
                return download_drive_file_by_id(drive_svc, fid, out_path)
            except Exception:
                pass
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(1024 * 64):
            if chunk:
                f.write(chunk)
    return os.path.getsize(out_path)

def render_pages_to_images(input_pdf_path, dpi):
    doc = fitz.open(input_pdf_path)
    images = []
    for p in range(len(doc)):
        page = doc.load_page(p)
        pix = page.get_pixmap(matrix=fitz.Matrix(dpi/72, dpi/72), alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        images.append(img)
    doc.close()
    return images

def compose_images_to_target_size(images, target_w_pt, target_h_pt, dpi, jpeg_quality):
    target_w_px, target_h_px = int(target_w_pt * dpi / 72), int(target_h_pt * dpi / 72)
    canvases = []
    for img in images:
        w, h = img.size
        ratio = min(target_w_px / w, target_h_px / h)
        new_w, new_h = int(w * ratio), int(h * ratio)
        resized = img.resize((new_w, new_h), Image.LANCZOS)
        canvas = Image.new("RGB", (target_w_px, target_h_px), (255, 255, 255))
        canvas.paste(resized, ((target_w_px - new_w)//2, (target_h_px - new_h)//2))
        canvases.append(canvas)

    bio = io.BytesIO()
    canvases[0].save(
        bio,
        format="PDF",
        save_all=True,
        append_images=canvases[1:],
        quality=jpeg_quality,
        optimize=True
    )
    bio.seek(0)
    return bio.getvalue()

# ===================================================================
# STRICT COMPRESSION (≤ 100 KB)
# ===================================================================
def iterative_render_and_compress(path, w, h):
    dpi, q = START_DPI, START_JPEG_QUALITY

    while True:
        images = render_pages_to_images(path, dpi)
        pdf_bytes = compose_images_to_target_size(images, w, h, dpi, q)
        size = len(pdf_bytes)

        print(f"try → dpi={dpi}, q={q}, size={size}")

        if size <= MAX_TARGET_BYTES:
            return pdf_bytes, size, dpi, q

        # reduce quality first
        if q - QUALITY_STEP >= MIN_JPEG_QUALITY:
            q -= QUALITY_STEP
            continue

        # then reduce DPI
        if dpi - DPI_STEP >= MIN_DPI:
            dpi -= DPI_STEP
            q = START_JPEG_QUALITY
            continue

        # last fallback
        print("⚠️ Could not reach 100 KB, using smallest possible output")
        return pdf_bytes, size, dpi, q

def upload_file_to_drive_bytes(drive_svc, pdf_bytes, filename, folder_id):
    media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=True)
    meta = {"name": filename}
    if folder_id:
        meta["parents"] = [folder_id]
    f = drive_svc.files().create(body=meta, media_body=media, fields="id,size").execute()
    return f["id"], int(f.get("size", 0))

def set_file_public_anyone(drive_svc, file_id):
    try:
        drive_svc.permissions().create(
            fileId=file_id,
            body={"role": "reader", "type": "anyone"}
        ).execute()
    except Exception:
        pass

def safe_filename(s):
    s = (s or "").strip()
    if not s:
        return f"pdf_{int(time.time())}.pdf"
    import re
    s = re.sub(r'[\\/*?:"<>|]', "_", s)
    return s if s.lower().endswith(".pdf") else s + ".pdf"

# ===================================================================
# MAIN
# ===================================================================
def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID missing")

    drive_svc, sheets_svc = get_clients()

    # ensure sheets exist
    ensure_sheet_grid(sheets_svc, SPREADSHEET_ID, SHEET_NAME, min_cols=10)
    ensure_sheet_grid(sheets_svc, SPREADSHEET_ID, GIT_SHEET_NAME, min_cols=3)

    # load existing GIT invoices
    existing_git_invoices = get_existing_git_invoices(
        sheets_svc, SPREADSHEET_ID, GIT_SHEET_NAME
    )
    print(f"Found {len(existing_git_invoices)} invoices already in GIT")

    # read source columns
    cols = sheet_get_columns(
        sheets_svc, SPREADSHEET_ID, SHEET_NAME,
        [(COL_INVOICE, START_ROW), (COL_URL, START_ROW)]
    )
    invoices = cols.get(COL_INVOICE, [])
    urls = cols.get(COL_URL, [])
    rows_count = max(len(urls), len(invoices))

    # build pending list
    pending = []
    for i in range(rows_count):
        url = (urls[i] if i < len(urls) else "").strip()
        inv = (invoices[i] if i < len(invoices) else "").strip()
        if url and inv and inv not in existing_git_invoices:
            pending.append(i)

    if not pending:
        print("✅ No new invoices to process.")
        return

    print(f"🟢 Processing {len(pending)} new invoice(s)")

    for idx in pending:
        row_num = START_ROW + idx
        url = urls[idx].strip()
        inv = invoices[idx].strip()

        print(f"\nProcessing invoice {inv} from row {row_num}")

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        tmp.close()

        try:
            # download
            download_url_to_file(drive_svc, url, tmp.name, timeout=DOWNLOAD_TIMEOUT)

            # compress
            pdf_bytes, final_size, used_dpi, used_quality = iterative_render_and_compress(
                tmp.name, TARGET_WIDTH_PT, TARGET_HEIGHT_PT
            )

            # upload
            filename = safe_filename(inv)
            file_id, uploaded_size = upload_file_to_drive_bytes(
                drive_svc, pdf_bytes, filename, DEST_FOLDER_ID
            )

            # strict check
            if uploaded_size > MAX_TARGET_BYTES:
                raise RuntimeError(
                    f"Compressed file > 100 KB ({uploaded_size} bytes)"
                )

            set_file_public_anyone(drive_svc, file_id)

            view_url = f"https://drive.google.com/uc?export=view&id={file_id}"
            flag = "COMPRESSED"

            # next empty row in GIT
            git_vals = sheets_svc.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{GIT_SHEET_NAME}!A2:A{MAX_ROWS_TO_CHECK}"
            ).execute().get("values", [])
            git_next_row = len(git_vals) + 2

            # write to GIT
            sheet_update_cell(sheets_svc, SPREADSHEET_ID, GIT_SHEET_NAME,
                              git_next_row, GIT_COL_INVOICE, inv)
            sheet_update_cell(sheets_svc, SPREADSHEET_ID, GIT_SHEET_NAME,
                              git_next_row, GIT_COL_URL, view_url)
            sheet_update_cell(sheets_svc, SPREADSHEET_ID, GIT_SHEET_NAME,
                              git_next_row, GIT_COL_LOG,
                              f"{flag} dpi={used_dpi} q={used_quality} size={uploaded_size}")

            print(f"✅ Added to GIT row {git_next_row}: {inv}")

        except Exception as e:
            print("❌ Error:", e)

        finally:
            try:
                os.remove(tmp.name)
            except Exception:
                pass

    print("\n🎉 Finished all new invoices.")

if __name__ == "__main__":
    main()
