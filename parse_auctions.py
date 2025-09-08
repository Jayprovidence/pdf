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

def parse_table_with_layout_inference(table_lines, header_map_def):
    """
    [核心升級] 基於版面推斷的智慧型表格解析器。
    此函式徹底拋棄 re.split，轉而模擬人類閱讀行為：
    1. 學習表頭位置：分析表頭中各欄位的起始水平座標。
    2. 推斷欄位邊界：根據表頭位置定義每一欄的範圍。
    3. 按位置分配資料：將後續資料行的文字，根據其座標歸入對應的欄位。
    """
    if not table_lines or len(table_lines) < 2:
        return

    header_line = table_lines
    header_spans =
    
    # 建立一個從標準欄位到其在表頭中位置的映射
    header_map = {}
    for key, variations in header_map_def.items():
        for start_pos, text in header_spans:
            if any(v in text for v in variations):
                header_map[key] = start_pos
                break
    
    if "編號" not in header_map:
        return # 如果連最基本的 "編號" 欄位都找不到，則視為無效表格

    # 根據位置排序，並計算出每個欄位的邊界
    sorted_headers = sorted(header_map.items(), key=lambda item: item[1])
    column_boundaries =
    for i in range(len(sorted_headers)):
        key, start_pos = sorted_headers[i]
        # 下一個欄位的起始位置是當前欄位的結束邊界
        end_pos = sorted_headers[i+1][1] if i + 1 < len(sorted_headers) else len(header_line) + 20
        column_boundaries.append({'key': key, 'start': start_pos, 'end': end_pos})

    items =
    for line in table_lines[1:]:
        # 檢查是否為有效的資料行 (通常以數字編號開頭)
        if not re.match(r'^\s*\d+', line):
            # 如果不是，則將其內容附加到上一筆資料的「備考」欄位
            if items and '備考' in items[-1]:
                items[-1]['備考'] = (items[-1]['備考'] + ' ' + line.strip()).strip()
            continue

        item = {boundary['key']: '' for boundary in column_boundaries}
        
        # 將該行的文字按其在欄位邊界內的位置進行分配
        for boundary in column_boundaries:
            # 提取落在該欄位邊界內的文字
            cell_text = line[boundary['start']:boundary['end']].strip()
            item[boundary['key']] = (item[boundary['key']] + ' ' + cell_text).strip()
        
        # 進行基本的資料驗證
        if item.get("編號"):
            items.append(item)
            
    return items


def parse_auction_pdf(pdf, case_number):
    """
    強健的 PDF 解析函式 v16 (最終版：實現版面感知)
    """
    if not pdf.pages:
        return None

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

    parsed_bid_sections =
    current_section = None
    current_mode = 'HEADER'
    last_other_key = None

    def finalize_section(section):
        if not section: return None
        if section.get("lands") or section.get("buildings") or any(section.get("otherSections", {}).values()):
            return section
        return None

    def start_new_section(bid_match):
        nonlocal current_section, current_mode, last_other_key
        if current_section:
            finalized = finalize_section(current_section)
            if finalized:
                parsed_bid_sections.append(finalized)
        bid_name = (bid_match.group(1) or "").strip() if bid_match else "N/A"
        current_section = {"bidName": bid_name, "header": "", "lands":, "buildings":, "otherSections": {}}
        current_mode = 'HEADER'
        last_other_key = None

    full_text = "\n".join()
    lines = full_text.split('\n')
    
    start_new_section(None)
    line_idx = 0
    while line_idx < len(lines):
        line = lines[line_idx]
        line_strip = line.strip()

        if not line_strip or "函 稿 代 碼" in line_strip:
            line_idx += 1
            continue
        
        # --- 模式切換 ---
        bid_match = re.search(r'標\s*別\s*[:：]\s*([甲乙丙丁戊己庚辛壬癸0-9A-Z]+)', line_strip)
        if bid_match:
            start_new_section(bid_match)
            line_idx += 1
            continue

        if '土地拍賣明細' in line_strip:
            current_mode = 'LAND_TABLE'
            line_idx += 1
            continue
        if '建物拍賣明細' in line_strip:
            current_mode = 'BUILDING_TABLE'
            line_idx += 1
            continue
        if any(line_strip.startswith(kw) for kw in ["點交情形", "使用情形", "備註"]):
            current_mode = 'OTHER_TEXT'
            # 不增加 line_idx，讓 OTHER_TEXT 區塊處理這一行

        # --- 內容擷取 ---
        if current_mode == 'LAND_TABLE' or current_mode == 'BUILDING_TABLE':
            table_lines =
            # 收集表格的所有行，直到遇到下一個區塊的關鍵字或明顯的非表格行
            while line_idx < len(lines):
                table_line = lines[line_idx]
                if (any(kw in table_line for kw in ["建物拍賣明細", "點交情形", "使用情形", "備註"]) or
                    (len(table_lines) > 1 and not re.match(r'^\s*(\d+|[（(])', table_line.strip()))):
                    break
                table_lines.append(table_line)
                line_idx += 1
            
            if current_mode == 'LAND_TABLE':
                parsed_items = parse_table_with_layout_inference(table_lines, LAND_HEADER_MAP_DEF)
                if parsed_items: current_section['lands'].extend(parsed_items)
            else: # BUILDING_TABLE
                parsed_items = parse_table_with_layout_inference(table_lines, BUILDING_HEADER_MAP_DEF)
                if parsed_items: current_section['buildings'].extend(parsed_items)
            
            current_mode = 'HEADER' # 解析完畢，回到預設狀態
            continue

        elif current_mode == 'OTHER_TEXT':
            is_keyword = False
            for keyword in ["點交情形", "使用情形", "備註"]:
                if line_strip.startswith(keyword):
                    last_other_key = keyword
                    content = line_strip[len(keyword):].lstrip(' :：')
                    current_section[keyword] = (current_section.get(keyword, '') + ' ' + content).strip()
                    is_keyword = True
                    break
            if not is_keyword and last_other_key:
                current_section[last_other_key] = (current_section[last_other_key] + ' ' + line_strip).strip()

        elif current_mode == 'HEADER':
            if not current_section.get("header"):
                header_match = re.search(r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)', line)
                if header_match:
                    case_no = re.sub(r'\s', '', header_match.group(1))
                    current_section["header"] = f"{case_no} 財產所有人: OOO"
        
        line_idx += 1

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
                if isinstance(existing_data, list) and existing_data and 'caseNumber' in existing_data:
                    processed_details = {item['caseNumber']: item for item in existing_data}
                    print(f"  -> 已成功載入 {len(processed_details)} 筆已處理的案件進度。")
                else:
                    print("  -> 警告: 進度檔案格式不符或為舊格式，將重新開始。")
            except Exception as e:
                print(f"  -> 警告: 無法讀取或解析進度檔案，將重新開始。錯誤: {e}", file=sys.stderr)
                processed_details = {}
        
        with open(LOCAL_TEMP_INPUT_PATH, 'r', encoding='utf-8') as f:
            auction_data = json.load(f)
        
        all_cases = auction_data.get('data',)
        total = len(all_cases)
        newly_processed_count = 0
        
        print(f"總共找到 {total} 筆案件，準備開始處理...")

        for i, case_data in enumerate(all_cases):
            case_num_str = case_data.get('caseNumber', 'N/A')
            
            if (case_num_str in processed_details and 
                processed_details.get(case_num_str) and 
                'error' not in processed_details.get(case_num_str, {})):
                print(f"正在處理: {i+1}/{total} - 案號: {case_num_str} (已處理，跳過)")
                continue

            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = None
            if pdfs_list and isinstance(pdfs_list, list) and len(pdfs_list) > 0:
                first_pdf = pdfs_list
                if first_pdf and isinstance(first_pdf, dict):
                    pdf_url = first_pdf.get('url')

            if pdf_url and pdf_url!= 'N/A':
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
                output_list =
                if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
                    print("✅ 進度儲存成功。")
                else:
                    print("❌ 進度儲存失敗。")

        print("\n所有案件處理完畢，正在進行最終儲存...")
        output_list =
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
