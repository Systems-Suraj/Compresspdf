import io, os, time, tempfile
from PIL import Image
import fitz  # PyMuPDF
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ---------------- CONFIG (env-driven) ----------------
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1JPJ5q2vTvybxlsX29r-YXL3mUBH0Piewz2AIJlSfLwc')
SHEET_NAME = os.environ.get('SHEET_NAME', 'Dispatch Details')
COL_URL = int(os.environ.get('COL_URL', '9'))          # I
COL_INVOICE = int(os.environ.get('COL_INVOICE', '7'))  # G
START_ROW = int(os.environ.get('START_ROW', '2'))
COL_OUTPUT = int(os.environ.get('COL_OUTPUT', '12'))   # L
COL_FLAG = int(os.environ.get('COL_FLAG', '13'))       # M
DEST_FOLDER_ID = os.environ.get('DEST_FOLDER_ID', '14WG909MU8e-GepUIe41PQ9yLev22wChK')

MAX_TARGET_BYTES = 1 * 1024 * 1024  # 1 MB
TARGET_WIDTH_PT = 595   # A4
TARGET_HEIGHT_PT = 842  # A4

START_DPI = 150
MIN_DPI = 72
START_JPEG_QUALITY = 85
MIN_JPEG_QUALITY = 30
DPI_STEP = 10
QUALITY_STEP = 5

DOWNLOAD_TIMEOUT = 60
MAX_ROWS_TO_CHECK = 10000

# ---- Auth: Service Account from env SECRET JSON ----
SA_JSON = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']
creds = service_account.Credentials.from_service_account_info(
    __import__('json').loads(SA_JSON),
    scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
)

drive_svc = build('drive', 'v3', credentials=creds, cache_discovery=False)
sheets_svc = build('sheets', 'v4', credentials=creds, cache_discovery=False)

# ---------------- helpers ----------------
def _col_letter(col_idx):
    col = col_idx
    s = ""
    while col > 0:
        col, rem = divmod(col-1, 26)
        s = chr(65 + rem) + s
    return s

def sheet_get_columns(spreadsheet_id, sheet_name, cols_and_start):
    result = {}
    for col_idx, start_row in cols_and_start:
        col_letter = _col_letter(col_idx)
        rng = f"{sheet_name}!{col_letter}{start_row}:{col_letter}{MAX_ROWS_TO_CHECK}"
        resp = sheets_svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        vals = resp.get('values', [])
        result[col_idx] = [ (r[0].strip() if r and r[0] else '') for r in vals ]
    return result

def sheet_update_cell(spreadsheet_id, sheet_name, row, col, value):
    rng = f"{sheet_name}!{_col_letter(col)}{row}"
    body = {"range": rng, "values": [[value]]}
    sheets_svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=rng,
        valueInputOption='RAW', body=body
    ).execute()

def is_drive_share_url(url):
    return url and 'drive.google.com' in url

def extract_drive_file_id(url):
    if not url: return None
    import re
    m = re.search(r'/file/d/([a-zA-Z0-9_-]{10,})', url)
    if m: return m.group(1)
    m = re.search(r'[?&]id=([a-zA-Z0-9_-]{10,})', url)
    if m: return m.group(1)
    m = re.search(r'open\\?id=([a-zA-Z0-9_-]{10,})', url)
    if m: return m.group(1)
    return None

def download_drive_file_by_id(file_id, out_path):
    request = drive_svc.files().get_media(fileId=file_id)
    fh = io.FileIO(out_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    return os.path.getsize(out_path)

def download_url_to_file(url, out_path, timeout=60):
    if is_drive_share_url(url):
        fid = extract_drive_file_id(url)
        if fid:
            try:
                return download_drive_file_by_id(fid, out_path)
            except Exception as e:
                print("Drive API download failed:", e, "-> fallback to HTTP")
    headers = {'User-Agent': 'Mozilla/5.0 (compatible)'}
    r = requests.get(url, stream=True, headers=headers, timeout=timeout)
    r.raise_for_status()
    with open(out_path, 'wb') as f:
        for chunk in r.iter_content(1024*64):
            if chunk:
                f.write(chunk)
    return os.path.getsize(out_path)

def render_pages_to_images(input_pdf_path, dpi):
    doc = fitz.open(input_pdf_path)
    images = []
    for p in range(len(doc)):
        page = doc.load_page(p)
        mat = fitz.Matrix(dpi/72.0, dpi/72.0)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
        images.append(img)
    doc.close()
    return images

def compose_images_to_target_size(images, target_w_pt, target_h_pt, dpi, jpeg_quality):
    target_w_px = int(round(target_w_pt * dpi / 72.0))
    target_h_px = int(round(target_h_pt * dpi / 72.0))
    canvas_images = []
    for img in images:
        img_w, img_h = img.size
        ratio = min(target_w_px / img_w, target_h_px / img_h)
        new_w = int(round(img_w * ratio))
        new_h = int(round(img_h * ratio))
        resized = img.resize((new_w, new_h), resample=Image.LANCZOS)
        canvas = Image.new("RGB", (target_w_px, target_h_px), (255,255,255))
        left = (target_w_px - new_w)//2
        top = (target_h_px - new_h)//2
        canvas.paste(resized, (left, top))
        canvas_images.append(canvas)
    bio = io.BytesIO()
    save_kwargs = {"save_all": True, "append_images": canvas_images[1:], "format": "PDF", "quality": jpeg_quality, "optimize": True}
    canvas_images[0].save(bio, **save_kwargs)
    bio.seek(0)
    return bio.getvalue()

def iterative_render_and_compress(input_pdf_path, target_w_pt, target_h_pt,
                                  start_dpi, min_dpi,
                                  start_quality, min_quality,
                                  max_bytes):
    dpi = start_dpi
    quality = start_quality
    while True:
        images = render_pages_to_images(input_pdf_path, dpi)
        pdf_bytes = compose_images_to_target_size(images, target_w_pt, target_h_pt, dpi, quality)
        size = len(pdf_bytes)
        print(f"  try dpi={dpi} q={quality} -> {size} bytes")
        if size <= max_bytes:
            return pdf_bytes, size, dpi, quality
        if quality - 5 >= min_quality:
            quality -= 5
            continue
        if dpi - 10 >= min_dpi:
            dpi -= 10
            quality = start_quality
            continue
        return pdf_bytes, size, dpi, quality

def upload_file_to_drive_bytes(pdf_bytes, filename, folder_id):
    bio = io.BytesIO(pdf_bytes)
    media = MediaIoBaseUpload(bio, mimetype='application/pdf', resumable=True)
    body = {'name': filename, 'parents': [folder_id]}
    f = drive_svc.files().create(body=body, media_body=media, fields='id,size').execute()
    return f.get('id'), int(f.get('size',0))

def set_file_public_anyone(file_id):
    try:
        drive_svc.permissions().create(fileId=file_id, body={'role':'reader','type':'anyone'}).execute()
    except Exception as e:
        print("Warning: set public failed:", e)

def safe_filename(s):
    s = (s or "").strip()
    if not s:
        return f'pdf_{int(time.time())}.pdf'
    import re
    s = re.sub(r'[\\/*?:"<>|]', '_', s)
    if not s.lower().endswith('.pdf'):
        s = s + '.pdf'
    return s

def main():
    # Read columns
    cols = sheet_get_columns(SPREADSHEET_ID, SHEET_NAME, [(COL_URL, START_ROW), (COL_INVOICE, START_ROW)])
    urls = cols.get(COL_URL, [])
    invoices = cols.get(COL_INVOICE, [])
    rows_count = max(len(urls), len(invoices))
    print(f"Found up to {rows_count} rows to check (starting at row {START_ROW})")

    for idx in range(rows_count):
        row_num = START_ROW + idx
        url = (urls[idx] if idx < len(urls) else '').strip()
        inv = (invoices[idx] if idx < len(invoices) else '').strip()
        if not url:
            continue

        # skip if already processed
        try:
            existing = sheets_svc.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_NAME}!{_col_letter(COL_OUTPUT)}{row_num}"
            ).execute().get('values', [])
            if existing and existing[0] and existing[0][0]:
                print(f"Row {row_num}: already processed, skipping.")
                continue
        except Exception as e:
            print("Warning reading existing output:", e)

        print(f"\nRow {row_num}: processing URL -> {url} ; invoice -> '{inv}'")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf'); tmp.close()

        try:
            print(" Downloading source PDF...")
            dl_size = download_url_to_file(url, tmp.name, timeout=DOWNLOAD_TIMEOUT)
            print(f" Downloaded {dl_size} bytes")

            print(f" Rendering + compressing to <= {MAX_TARGET_BYTES} bytes ...")
            pdf_bytes, final_size, used_dpi, used_quality = iterative_render_and_compress(
                tmp.name, TARGET_WIDTH_PT, TARGET_HEIGHT_PT,
                START_DPI, MIN_DPI,
                START_JPEG_QUALITY, MIN_JPEG_QUALITY,
                MAX_TARGET_BYTES
            )
            print(f" Result size={final_size} bytes (dpi={used_dpi}, quality={used_quality})")

            filename = safe_filename(inv) if inv else safe_filename(os.path.basename(url.split('?')[0]))
            print(" Uploading as:", filename)

            file_id, uploaded_size = upload_file_to_drive_bytes(pdf_bytes, filename, DEST_FOLDER_ID)
            print(" Uploaded id:", file_id, "size:", uploaded_size)

            set_file_public_anyone(file_id)

            view_url = f"https://drive.google.com/uc?export=view&id={file_id}"
            flag = 'COMPRESSED' if uploaded_size <= MAX_TARGET_BYTES else ('LARGE_FILE' if uploaded_size > MAX_TARGET_BYTES else 'OK')
            sheet_update_cell(SPREADSHEET_ID, SHEET_NAME, row_num, COL_OUTPUT, view_url)
            sheet_update_cell(SPREADSHEET_ID, SHEET_NAME, row_num, COL_FLAG, f"{flag} dpi={used_dpi} q={used_quality} size={uploaded_size}")

            print(f"Row {row_num}: done. flag={flag}")

        except Exception as e:
            print("Row error:", e)
            try:
                sheet_update_cell(SPREADSHEET_ID, SHEET_NAME, row_num, COL_FLAG, f"ERROR: {str(e)[:250]}")
            except Exception as ee:
                print("Also failed to write error to sheet:", ee)
        finally:
            try:
                if os.path.exists(tmp.name):
                    os.remove(tmp.name)
            except:
                pass

    print("\nAll rows processed.")

if __name__ == "__main__":
    main()
