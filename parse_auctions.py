import json
import requests
import pdfplumber
import camelot
import pandas as pd
import re
import os
import sys
import time
import random
import io
import tempfile
import fitz  # PyMuPDF
from google.cloud import storage
import traceback
from typing import List, Dict, Any, Optional

# --- GCS 設定 ---
# 提醒：在生產環境中，建議將這些設定移至環境變數或專門的設定檔。
GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025'
SOURCE_FILE_GCS = 'auctionData.json'
OUTPUT_FILE_GCS = 'auctionDataWithDetails.json'
LOCAL_TEMP_INPUT_PATH = './auctionData_temp.json'

# --- 階段一：預處理與分流 ---
def is_scanned_pdf(pdf_path: str) -> bool:
    """
    使用 PyMuPDF 快速檢測 PDF 是否為掃描件（基於圖像）。
    一個簡單的啟發式規則是檢查頁面上的文字數量。
    """
    try:
        doc = fitz.open(pdf_path)
        if not doc.page_count:
            return True
        # 只檢查第一頁以提高效率
        page = doc.load_page(0)
        text = page.get_text("text")
        # 如果頁面中可辨識的文字數量極少（例如少於100個字元），則有很高機率是掃描件
        return len(text.strip()) < 100
    except Exception as e:
        print(f"   -> 警告: 檢查 PDF 是否掃描檔時出錯: {e}", file=sys.stderr)
        # 無法開啟或處理檔案，也視為需要特殊處理
        return True

# --- 核心解析邏輯 ---
def parse_auction_pdf_hybrid(pdf_path: str, case_number: str) -> Dict[str, Any]:
    """
    高韌性 PDF 混合式解析管線 (v2.0)
    """
    if is_scanned_pdf(pdf_path):
        return {"error": "文件為掃描檔，無法自動解析。"}

    LAND_HEADER_MAP_DEF = {
        "編號": ["編號"], "縣市": ["縣市"], "鄉鎮市區": ["鄉鎮市區", "鄉鎮市"],
        "段": ["段"], "小段": ["小段"], "地號": ["地號"],
        "面積(m²)": ["面積"], "權利範圍": ["權利範圍", "權利"],
        "價格(元)": ["最低拍賣價格", "價格"], "備考": ["備考"]
    }
    BUILDING_HEADER_MAP_DEF = {
        "編號": ["編號"], "建號": ["建號"], "建物門牌": ["門牌", "建物坐落", "基地坐落"],
        "主要建材/層數": ["主要建材", "層數", "建築式樣"], "面積(m²)": ["面積"],
        "附屬建物": ["附屬建物", "附屬"], "權利範圍": ["權利範圍", "權利"],
        "價格(元)": ["最低拍賣價格", "價格"], "備考": ["備考"]
    }

    def clean_text(text):
        return str(text).replace('\n', ' ').strip() if text else ""

    def create_header_map(header_row: List[str], standard_headers_def: Dict[str, List[str]]) -> Dict[str, int]:
        header_map = {}
        cleaned_header = [clean_text(cell) for cell in header_row]
        for key, variations in standard_headers_def.items():
            found = False
            for i, cell in enumerate(cleaned_header):
                if any(v in cell for v in variations):
                    header_map[key] = i
                    found = True
                    break
            if not found:
                header_map[key] = -1
        return header_map

    def process_dataframe(df: pd.DataFrame, header_map_def: Dict[str, List[str]]) -> Optional[List[Dict[str, Any]]]:
        if df.empty:
            return None

        header_row_index = -1
        for i, row in df.iterrows():
            if any("編號" in str(cell) for cell in row.values):
                header_row_index = i
                break
        
        if header_row_index == -1:
            return None

        header_row = df.iloc[header_row_index].values.tolist()
        header_map = create_header_map(header_row, header_map_def)
        
        if header_map.get("編號", -1) == -1:
            return None

        items = []
        data_rows_raw = df.iloc[header_row_index + 1:].values.tolist()
        
        if not data_rows_raw:
            return None

        # 強化的合併邏輯：處理多行儲存格
        merged_rows = []
        for row_values in data_rows_raw:
            # 檢查編號欄位是否為空或不似編號
            is_new_item = clean_text(row_values[header_map["編號"]]) and clean_text(row_values[header_map["編號"]]).isdigit()
            
            if is_new_item and any(clean_text(c) for c in row_values[1:]):
                merged_rows.append([clean_text(cell) for cell in row_values])
            elif merged_rows and any(clean_text(c) for c in row_values):
                # 合併到上一筆資料
                for i, val in enumerate(row_values):
                    clean_val = clean_text(val)
                    if clean_val:
                        if i < len(merged_rows[-1]):
                            merged_rows[-1][i] = f"{merged_rows[-1][i]} {clean_val}".strip()
        
        for row in merged_rows:
            item = {}
            for key, index in header_map.items():
                if index != -1 and index < len(row):
                    item[key] = re.sub(r'\s+', ' ', row[index]).strip()
            
            if len(item) > 1 and any(item.get(k) for k in item if k != '編號'):
                items.append(item)
        return items if items else None

    def classify_and_process_tables(tables: List[Any], section_data: Dict[str, Any]):
        if not tables: return
        for table in tables:
            df = table.df
            if df.empty or df.shape[1] < 3: continue
            
            header_text = ' '.join([' '.join(map(str, row)) for _, row in df.head(3).iterrows()])
            
            is_land = any(k in header_text for k in ["地號", "地 號"])
            is_building = any(k in header_text for k in ["建號", "建 號", "建物門牌", "建築式樣"])

            if not is_land and not is_building:
                continue

            if is_land:
                parsed_items = process_dataframe(df, LAND_HEADER_MAP_DEF)
                if parsed_items: section_data['lands'].extend(parsed_items)
            elif is_building:
                parsed_items = process_dataframe(df, BUILDING_HEADER_MAP_DEF)
                if parsed_items: section_data['buildings'].extend(parsed_items)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # --- 階段二：版面感知的語意分割 ---
            all_anchors = []
            full_text_for_header = ""
            for i, page in enumerate(pdf.pages):
                full_text_for_header += page.extract_text(x_tolerance=2) or ""
                search_keywords = {
                    'BID_SECTION': r'標\s*別\s*[:：]\s*([\'"]?[甲乙丙丁戊己庚辛壬癸0-9A-Z]+[\'"]?)',
                    'DELIVERY': r'點交情形', 'USAGE': r'使用情形', 'REMARKS': r'備註'
                }
                for key, pattern in search_keywords.items():
                    matches = page.search(pattern, regex=True, use_text_flow=True)
                    for match in matches:
                        match_text = match.get('text', '')
                        if key == 'BID_SECTION':
                            bid_extract = re.search(pattern, match_text, re.IGNORECASE)
                            if bid_extract and len(bid_extract.groups()) > 0:
                                match_text = re.sub(r'^[\'"]|[\'"]$', '', bid_extract.group(1)).strip()
                        
                        all_anchors.append({'type': key, 'page': i, 'top': match['top'], 'bottom': match['bottom'], 'text': match_text})
            
            all_anchors.sort(key=lambda x: (x['page'], x['top']))

            bid_section_definitions = []
            bid_anchors = [a for a in all_anchors if a['type'] == 'BID_SECTION']
            if bid_anchors:
                for i, anchor in enumerate(bid_anchors):
                    next_anchor = bid_anchors[i+1] if i + 1 < len(bid_anchors) else None
                    bid_section_definitions.append({'start_anchor': anchor, 'next_bid_anchor': next_anchor})
            else:
                # 如果沒有標別，則將整個文件視為一個標別
                bid_section_definitions.append({'start_anchor': {'text': 'N/A', 'page': 0, 'top': 0, 'bottom': 20}, 'next_bid_anchor': None})

            parsed_bid_sections = []
            for section_def in bid_section_definitions:
                current_section_data = {"bidName": section_def['start_anchor']['text'], "header": "", "lands": [], "buildings": [], "otherSections": {}}
                start_anchor = section_def['start_anchor']
                next_bid_anchor = section_def['next_bid_anchor']

                # --- 階段三：高保真度表格擷取 ---
                start_page = start_anchor['page']
                end_page = next_bid_anchor['page'] if next_bid_anchor else len(pdf.pages) - 1
                
                page_range_str = ",".join(map(str, range(start_page + 1, end_page + 2)))
                if not page_range_str: continue

                tables = None
                try:
                    # 策略一：優先使用 lattice (適用於有線表格)
                    tables = camelot.read_pdf(pdf_path, pages=page_range_str, flavor='lattice', line_scale=40)
                except Exception:
                    tables = []
                
                # 策略二：如果 lattice 找不到，使用 stream 作為後備 (適用於無線表格)
                if not tables or all(t.df.empty for t in tables):
                    try:
                        table_areas = []
                        for p_idx in range(start_page, end_page + 1):
                            page = pdf.pages[p_idx]
                            y_top = start_anchor['bottom'] if p_idx == start_page else 0
                            y_bottom = next_bid_anchor['top'] if next_bid_anchor and p_idx == end_page else page.height
                            
                            # 轉換為 camelot 座標系 (左下角為原點)
                            y1 = page.height - y_bottom
                            y2 = page.height - y_top
                            if y2 > y1:
                                table_areas.append(f"0,{y1},{page.width},{y2}")

                        if table_areas:
                            tables = camelot.read_pdf(pdf_path, pages=page_range_str, flavor='stream', table_areas=table_areas, row_tol=10)
                    except Exception:
                        tables = []
                
                classify_and_process_tables(tables, current_section_data)

                # --- 處理文字區塊 (點交情形、備註等) ---
                text_anchors = [a for a in all_anchors if a['type'] != 'BID_SECTION' and start_page <= a['page'] <= end_page]
                if next_bid_anchor:
                    text_anchors = [a for a in text_anchors if a['page'] < next_bid_anchor['page'] or (a['page'] == next_bid_anchor['page'] and a['top'] < next_bid_anchor['top'])]

                for i, anchor in enumerate(text_anchors):
                    page_num = anchor['page']
                    page_obj = pdf.pages[page_num]
                    start_pos = anchor['bottom']
                    end_pos = page_obj.height
                    
                    next_anchor_on_page = next((na for na in text_anchors[i+1:] if na['page'] == page_num), None)
                    if next_anchor_on_page:
                        end_pos = next_anchor_on_page['top']
                    elif next_bid_anchor and next_bid_anchor['page'] == page_num:
                        end_pos = next_bid_anchor['top']
                    
                    bbox = (0, start_pos, page_obj.width, end_pos)
                    if bbox[1] >= bbox[3]: continue
                    
                    text = page_obj.crop(bbox).extract_text(x_tolerance=3, y_tolerance=3) or ""
                    clean_content = re.sub(r'\s+', ' ', text).strip()
                    
                    key_map = {'DELIVERY': '點交情形', 'USAGE': '使用情形', 'REMARKS': '備註'}
                    section_key = key_map.get(anchor['type'])
                    if section_key:
                        current_section_data[section_key] = (current_section_data.get(section_key, "") + " " + clean_content).strip()


                # --- 階段四：資料結構化 ---
                header_match = re.search(r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)', full_text_for_header)
                if header_match:
                    case_no_clean = re.sub(r'\s', '', header_match.group(1))
                    current_section_data["header"] = f"{case_no_clean} 財產所有人: OOO"

                if (current_section_data['lands'] or current_section_data['buildings'] or 
                    any(current_section_data.get(k) for k in ['點交情形', '使用情形', '備註'])):
                    parsed_bid_sections.append(current_section_data)

            return {"bidSections": parsed_bid_sections} if parsed_bid_sections else None

    except Exception as e:
        print(f"   -> 嚴重錯誤: 解析 PDF 過程中發生未預期錯誤 ({case_number}): {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return {"error": f"（PDF 解析時發生嚴重錯誤: {e}）"}


def upload_to_gcs(bucket, blob_name, data):
    """上傳資料至 Google Cloud Storage"""
    try:
        blob = bucket.blob(blob_name)
        json_data_string = json.dumps(data, ensure_ascii=False, indent=4)
        blob.upload_from_string(json_data_string, content_type='application/json')
        return True
    except Exception as e:
        print(f"❌ 錯誤: 上傳至 GCS 失敗: {e}", file=sys.stderr)
        return False

def main():
    """主執行函式：協調 GCS 下載、PDF 解析、進度保存和最終上傳。"""
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

    processed_data = {}
    try:
        # 嘗試載入已處理的進度，以支援中斷後繼續
        output_blob = bucket.blob(OUTPUT_FILE_GCS)
        if output_blob.exists():
            print(f"發現現有的結果檔案 {OUTPUT_FILE_GCS}，載入進度...")
            try:
                existing_data = json.loads(output_blob.download_as_string())
                if isinstance(existing_data, list):
                    processed_data = {item['caseNumber']: item for item in existing_data if 'caseNumber' in item}
                    print(f"   -> 已成功載入 {len(processed_data)} 筆已處理的案件進度。")
                else:
                    print("   -> 警告: 進度檔案格式不符，將重新開始。")
            except Exception as e:
                print(f"   -> 警告: 無法讀取或解析進度檔案，將重新開始。錯誤: {e}", file=sys.stderr)
        
        with open(LOCAL_TEMP_INPUT_PATH, 'r', encoding='utf-8') as f:
            auction_data = json.load(f)
        
        all_cases = auction_data.get('data', [])
        total = len(all_cases)
        newly_processed_count = 0
        
        print(f"總共找到 {total} 筆案件，準備開始處理...")

        for i, case_data in enumerate(all_cases):
            case_num_str = case_data.get('caseNumber', f'UNKNOWN_{i}')
            
            # 如果需要，可以取消註解此區塊以跳過已處理的案件
            # if case_num_str in processed_data:
            #     print(f"正在處理: {i+1}/{total} - 案號: {case_num_str} (已處理，跳過)")
            #     continue

            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = pdfs_list[0]['url'] if pdfs_list and isinstance(pdfs_list, list) and len(pdfs_list) > 0 and isinstance(pdfs_list[0], dict) else None

            if pdf_url and pdf_url != 'N/A':
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                for attempt in range(3): # 重試機制
                    try:
                        response = requests.get(pdf_url, timeout=45, headers=headers)
                        response.raise_for_status()
                        
                        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
                            temp_pdf.write(response.content)
                            temp_pdf_path = temp_pdf.name
                        
                        try:
                            # *** 使用全新的混合式解析管線 ***
                            auction_details = parse_auction_pdf_hybrid(temp_pdf_path, case_num_str)
                        finally:
                            os.remove(temp_pdf_path)
                        
                        if not auction_details or not auction_details.get("bidSections"):
                                print(f"   -> 警告: 案號 {case_num_str} 的 PDF 內容無法成功解析。")
                                auction_details = { "error": "（PDF 內容解析失敗，請參閱原始文件）" }
                        break # 成功後跳出重試循環
                    except requests.exceptions.RequestException as e:
                        if attempt < 2: time.sleep(random.uniform(2, 4))
                        else:
                            print(f"   -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                            auction_details = { "error": f"（PDF 下載失敗: {e}）" }
                    except Exception as e:
                        print(f"   -> 錯誤: 處理 PDF 時發生未知錯誤 ({case_num_str}): {e}", file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                        auction_details = { "error": f"（處理 PDF 時發生未知錯誤: {e}）" }
                        break
                time.sleep(random.uniform(0.5, 1.5)) # 避免請求過於頻繁
            else:
                auction_details = { "error": "（無可用的 PDF 連結）" }

            case_data['auctionDetails'] = auction_details
            processed_data[case_num_str] = case_data
            newly_processed_count += 1

            # 每處理 25 筆新案件就儲存一次進度
            if newly_processed_count > 0 and newly_processed_count % 25 == 0:
                print(f"\n已處理 {newly_processed_count} 筆新案件，正在儲存進度至 GCS...")
                output_list = list(processed_data.values())
                if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
                    print("✅ 進度儲存成功。")
                else:
                    print("❌ 進度儲存失敗。")

        print("\n所有案件處理完畢，正在進行最終儲存...")
        output_list = list(processed_data.values())
        if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
            print(f"\n處理完成！已將 {len(output_list)} 筆案件的詳細資訊儲存至 GCS 上的 {OUTPUT_FILE_GCS}")
        else:
            local_backup_path = './auctionDataWithDetails_local_backup.json'
            with open(local_backup_path, 'w', encoding='utf-8') as f:
                json.dump(output_list, f, ensure_ascii=False, indent=4)
            print(f"❌ 最終上傳失敗！已在本地儲存備份檔案於 {local_backup_path}")

    finally:
        # 清理本地暫存檔案
        if os.path.exists(LOCAL_TEMP_INPUT_PATH):
            os.remove(LOCAL_TEMP_INPUT_PATH)
            print(f"已清理本地暫存檔案: {LOCAL_TEMP_INPUT_PATH}")

if __name__ == '__main__':
    # 執行前請確保已安裝必要的套件：
    # pip install requests pdfplumber "camelot-py[cv]" pandas google-cloud-storage PyMuPDF
    # 同時，請確保已設定好 GCS 的認證環境。
    main()
