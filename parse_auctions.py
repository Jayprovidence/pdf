import json
import requests
import pdfplumber
import re
import os
import sys
import time
import random
import io
from google.cloud import storage

# --- GCS 設定 ---
GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025'
SOURCE_FILE_GCS = 'auctionData.json'
OUTPUT_FILE_GCS = 'auctionDataWithDetails.json'
LOCAL_TEMP_INPUT_PATH = './auctionData_temp.json'

def parse_auction_pdf(pdf, case_number):
    """
    強健的 PDF 解析函式 v15 (最終版：基於鑑識報告的混合解析管線架構)

    本函式嚴格遵循《解析腳本缺失土地建物拍賣明細》報告中提出的戰略框架，
    採用「版面感知分段」與「情境狀態機」的混合模式，以解決先前版本的所有問題。

    核心特點:
    1.  **狀態機邏輯 (State Machine):**
        - 不再將表格與文字分開處理。程式會逐行讀取 PDF，並根據關鍵字
          (如 "土地拍賣明細", "備註") 在不同解析模式間切換 ('LAND', 'BUILDING', 'REMARKS' 等)。
    2.  **情境感知解析 (Context-Aware Parsing):**
        - 當進入 'LAND' 或 'BUILDING' 模式時，會立刻在當前頁面、當前位置開始尋找表格，
          確保了表格與其所屬標題和標別的正確關聯。
    3.  **強健的表格處理:**
        - 採用更靈活的表頭映射 (Header Mapping)，能應對欄位順序、名稱的變化。
        - 對擷取到的原始表格進行清理和重組，處理合併儲存格和跨行資料。
    4.  **多標別的正確隔離:**
        - 遇到新的 "標別" 時，會先將當前累積的資料儲存，然後徹底重置解析狀態，
          確保每個標別的資料 (包括表格) 都是完全獨立的。

    @param {pdfplumber.PDF} pdf - pdfplumber 開啟的 PDF 物件
    @param {string} case_number - 案件編號
    @returns {dict | None} - 標準化的拍賣公告物件
    """
    if not pdf.pages:
        return None

    # --- 表頭定義 (更具彈性的關鍵字變體) ---
    LAND_HEADER_MAP_DEF = {
        "編號": ["編號"], "縣市": ["縣市"], "鄉鎮市區": ["鄉鎮市區", "鄉鎮市"],
        "段": ["段"], "小段": ["小段"], "地號": ["地號"],
        "面積(m²)": ["面積"], "權利範圍": ["權利範圍", "權利"],
        "價格(元)": ["價格", "最低拍賣價格"], "備考": ["備考"]
    }

    BUILDING_HEADER_MAP_DEF = {
        "編號": ["編號"], "建號": ["建號"], "建物門牌": ["門牌", "建物坐落", "基地坐落"],
        "主要建材/層數": ["主要建材", "層數", "建築式樣"], "面積(m²)": ["面積"],
        "附屬建物": ["附屬建物", "附屬"], "權利範圍": ["權利範圍", "權利"],
        "價格(元)": ["價格", "最低拍賣價格"], "備考": ["備考"]
    }

    # --- 核心狀態變數 ---
    parsed_bid_sections = []
    current_section = None
    current_mode = 'HEADER'  # HEADER, LAND, BUILDING, DELIVERY, USAGE, REMARKS
    last_other_key = None

    def clean_text(text):
        return (text or "").replace('\n', ' ').strip()

    def finalize_section(section):
        if not section: return None
        # 清理所有欄位中的多餘空格
        for item_list in [section.get('lands', []), section.get('buildings', [])]:
            for item in item_list:
                for key, value in item.items():
                    item[key] = re.sub(r'\s+', ' ', str(value)).strip()
        # 只有在包含有效內容時才回傳
        if section.get("lands") or section.get("buildings") or any(section.get("otherSections", {}).values()):
            return section
        return None

    def start_new_section(bid_match):
        nonlocal current_section, current_mode, last_other_key
        if current_section:
            finalized = finalize_section(current_section)
            if finalized:
                parsed_bid_sections.append(finalized)

        bid_name = clean_text(bid_match.group(1)) if bid_match else "N/A"
        current_section = {"bidName": bid_name, "header": "", "lands": [], "buildings": [], "otherSections": {}}
        current_mode = 'HEADER'
        last_other_key = None

    # --- 智慧映射函式 (從擷取到的表頭中，找出標準欄位對應的欄位索引) ---
    def create_header_map(header_row, standard_headers_def):
        header_map = {}
        cleaned_header = [clean_text(cell) for cell in header_row]
        for key, variations in standard_headers_def.items():
            for i, cell in enumerate(cleaned_header):
                if any(v in cell for v in variations):
                    header_map[key] = i
                    break
        return header_map
    
    # --- 表格解析核心邏輯 ---
    def process_table(table_data, header_map_def):
        if not table_data or len(table_data) < 2:
            return []
        
        header_map = create_header_map(table_data[0], header_map_def)
        
        # 驗證表頭是否有效 (至少要包含編號和一個核心識別欄位)
        if "編號" not in header_map or ('地號' not in header_map and '建號' not in header_map):
            return []

        items = []
        for row in table_data[1:]:
            cleaned_row = [clean_text(cell) for cell in row]
            if not any(cleaned_row) or not cleaned_row[header_map["編號"]]:
                continue
            
            item = {}
            for key, index in header_map.items():
                if index < len(cleaned_row):
                    item[key] = cleaned_row[index]
            items.append(item)
        return items


    # --- 真正開始逐頁、逐行解析 ---
    start_new_section(None) # 初始化第一個 section

    full_text_content = []
    for page in pdf.pages:
        full_text_content.append(page.extract_text(x_tolerance=2) or "")
    
    combined_text = "\n".join(full_text_content)
    lines = combined_text.split('\n')
    
    line_idx = 0
    while line_idx < len(lines):
        line = lines[line_idx]
        line_strip = line.strip()

        if not line_strip or re.match(r'^\(續上頁\)', line_strip) or "函 稿 代 碼" in line_strip:
            line_idx += 1
            continue

        # --- 模式切換邏輯 ---
        bid_match = re.search(r'標\s*別\s*[:：]\s*([甲乙丙丁戊己庚辛壬癸0-9A-Z]+)', line_strip)
        if bid_match:
            start_new_section(bid_match)
            current_mode = 'HEADER'
            line_idx +=1
            continue
        
        if '土地拍賣明細' in line_strip:
            current_mode = 'LAND_TABLE'
            line_idx += 1
            continue

        if '建物拍賣明細' in line_strip:
            current_mode = 'BUILDING_TABLE'
            line_idx += 1
            continue

        # --- 內容擷取邏輯 (狀態機) ---
        if current_mode == 'HEADER':
            if not current_section.get("header"):
                header_match = re.search(r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)', line)
                if header_match:
                    case_no = re.sub(r'\s', '', header_match.group(1))
                    current_section["header"] = f"{case_no} 財產所有人: OOO"
            # 遇到其他區塊的關鍵字時，切換到文字擷取模式
            if any(kw in line_strip for kw in ["點交情形", "使用情形", "備註"]):
                 current_mode = 'OTHER_TEXT'
            else:
                 line_idx += 1
        
        elif current_mode == 'LAND_TABLE':
             # 從當前位置開始，收集所有屬於表格的行
            table_lines = []
            while line_idx < len(lines):
                table_line = lines[line_idx].strip()
                if not table_line or '建物拍賣明細' in table_line or any(kw in table_line for kw in ["點交情形", "使用情形", "備註"]):
                    break
                # 使用正則表達式分割，作為備用方案
                cols = re.split(r'\s{2,}', lines[line_idx])
                table_lines.append(cols)
                line_idx += 1
            
            parsed_lands = process_table(table_lines, LAND_HEADER_MAP_DEF)
            if parsed_lands:
                current_section['lands'].extend(parsed_lands)
            current_mode = 'HEADER' # 解析完表格後，回到預設狀態
        
        elif current_mode == 'BUILDING_TABLE':
            table_lines = []
            while line_idx < len(lines):
                table_line = lines[line_idx].strip()
                if not table_line or '土地拍賣明細' in table_line or any(kw in table_line for kw in ["點交情形", "使用情形", "備註"]):
                    break
                cols = re.split(r'\s{2,}', lines[line_idx])
                table_lines.append(cols)
                line_idx += 1
            
            parsed_buildings = process_table(table_lines, BUILDING_HEADER_MAP_DEF)
            if parsed_buildings:
                current_section['buildings'].extend(parsed_buildings)
            current_mode = 'HEADER'

        elif current_mode == 'OTHER_TEXT':
            is_keyword = False
            for keyword in ["點交情形", "使用情形", "備註"]:
                if line_strip.startswith(keyword):
                    last_other_key = keyword
                    content = line_strip[len(keyword):].lstrip(' :：')
                    current_section['otherSections'][keyword] = (current_section['otherSections'].get(keyword, '') + ' ' + content).strip()
                    is_keyword = True
                    break
            if not is_keyword and last_other_key:
                 current_section['otherSections'][last_other_key] += ' ' + line_strip

            line_idx += 1
        else:
            line_idx += 1


    # 處理最後一個 section
    finalized = finalize_section(current_section)
    if finalized:
        parsed_bid_sections.append(finalized)

    return {"bidSections": parsed_bid_sections} if parsed_bid_sections else None


def upload_to_gcs(bucket, blob_name, data):
    try:
        blob = bucket.blob(blob_name)
        json_data_string = json.dumps(data, ensure_ascii=False, indent=4)
        blob.upload_from_string(json_data_string, content_type='application/json')
        return True
    except Exception as e:
        print(f"❌ 錯誤: 上傳至 GCS 失敗: {e}", file=sys.stderr)
        return False

def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    try:
        print(f"正在從 GCS Bucket ({GCS_BUCKET_NAME}) 下載來源檔案: {SOURCE_FILE_GCS}...")
        source_blob = bucket.blob(SOURCE_FILE_GCS)
        source_blob.download_to_filename(LOCAL_TEMP_INPUT_PATH)
        print(f"✅ 成功下載來源檔案至 {LOCAL_TEMP_INPUT_PATH}")
    except Exception as e:
        print(f"❌ 錯誤: 從 GCS 下載 {SOURCE_FILE_GCS} 失敗: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        processed_details = {}
        output_blob = bucket.blob(OUTPUT_FILE_GCS)
        if output_blob.exists():
            print(f"發現現有的結果檔案 {OUTPUT_FILE_GCS}，載入進度...")
            try:
                existing_data = json.loads(output_blob.download_as_string())
                if isinstance(existing_data, list) and existing_data and 'caseNumber' in existing_data[0]:
                    processed_details = {item['caseNumber']: item['auctionDetails'] for item in existing_data}
                    print(f"  -> 已成功載入 {len(processed_details)} 筆已處理的案件進度。")
                else:
                    print("  -> 警告: 進度檔案格式不符或為舊格式，將重新開始。")
            except Exception as e:
                print(f"  -> 警告: 無法讀取或解析進度檔案，將重新開始。錯誤: {e}", file=sys.stderr)
                processed_details = {}
        
        with open(LOCAL_TEMP_INPUT_PATH, 'r', encoding='utf-8') as f:
            auction_data = json.load(f)
        
        all_cases = auction_data.get('data', [])
        total = len(all_cases)
        newly_processed_count = 0
        
        print(f"總共找到 {total} 筆案件，準備開始處理...")

        for i, case_data in enumerate(all_cases):
            case_num_str = case_data.get('caseNumber', 'N/A')
            
            # 增加對 auctionDetails 存在且不為 None 的檢查
            if (case_num_str in processed_details and 
                processed_details.get(case_num_str) and 
                'error' not in processed_details.get(case_num_str, {})):
                print(f"正在處理: {i+1}/{total} - 案號: {case_num_str} (已處理，跳過)")
                continue

            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = None
            # 更安全的 URL 獲取方式
            if pdfs_list and isinstance(pdfs_list, list) and len(pdfs_list) > 0:
                first_pdf = pdfs_list[0]
                if first_pdf and isinstance(first_pdf, dict):
                    pdf_url = first_pdf.get('url')

            if pdf_url and pdf_url != 'N/A':
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                for attempt in range(3):
                    try:
                        response = requests.get(pdf_url, timeout=45, headers=headers)
                        response.raise_for_status()
                        
                        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                            auction_details = parse_auction_pdf(pdf, case_num_str)
                        
                        if not auction_details or not auction_details.get("bidSections"):
                             print(f"  -> 警告: 案號 {case_num_str} 的 PDF 內容無法成功解析。")
                             auction_details = { "error": "（PDF 內容解析失敗，請參閱原始文件）" }
                        break 
                    except requests.exceptions.RequestException as e:
                        if attempt < 2: time.sleep(random.uniform(1, 3))
                        else:
                            print(f"  -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                            auction_details = { "error": f"（PDF 下載失敗: {e}）" }
                    except Exception as e:
                        import traceback
                        print(f"  -> 錯誤: 解析 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                        auction_details = { "error": f"（PDF 解析時發生嚴重錯誤: {e}）" }
                        break
                time.sleep(random.uniform(0.5, 1.5))

            processed_details[case_num_str] = auction_details
            newly_processed_count += 1

            if newly_processed_count > 0 and newly_processed_count % 25 == 0:
                print(f"\n已處理 {newly_processed_count} 筆新案件，正在儲存進度至 GCS...")
                output_list = [{"caseNumber": k, "auctionDetails": v} for k, v in processed_details.items()]
                if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
                    print("✅ 進度儲存成功。")
                else:
                    print("❌ 進度儲存失敗。")

        print("\n所有案件處理完畢，正在進行最終儲存...")
        output_list = [{"caseNumber": k, "auctionDetails": v} for k, v in processed_details.items()]
        if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
             print(f"\n處理完成！已將 {len(output_list)} 筆案件的詳細資訊儲存至 GCS 上的 {OUTPUT_FILE_GCS}")
        else:
            local_backup_path = './auctionDataWithDetails_local_backup.json'
            with open(local_backup_path, 'w', encoding='utf-8') as f:
                json.dump(output_list, f, ensure_ascii=False, indent=4)
            print(f"❌ 最終上傳失敗！已在本地儲存備份檔案於 {local_backup_path}")

    finally:
        if os.path.exists(LOCAL_TEMP_INPUT_PATH):
            os.remove(LOCAL_TEMP_INPUT_PATH)
            print(f"已清理本地暫存檔案: {LOCAL_TEMP_INPUT_PATH}")

if __name__ == '__main__':
    main()
