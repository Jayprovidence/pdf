import json
import requests
import pdfplumber
import re
import os
import sys
import time
import random
import tempfile
import fitz  # PyMuPDF
from google.cloud import storage
import traceback
from typing import List, Dict, Any, Optional

# --- GCS 設定 ---
GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025'
SOURCE_FILE_GCS = 'auctionData.json'
# 修改點：輸出檔案名保持不變，但內容結構會改變
OUTPUT_FILE_GCS = 'auctionDataWithDetails.json'
LOCAL_TEMP_INPUT_PATH = './auctionData_temp.json'


def is_scanned_pdf(pdf_path: str) -> bool:
    """使用 PyMuPDF 快速檢測 PDF 是否為掃描件。"""
    try:
        doc = fitz.open(pdf_path)
        if not doc.page_count: 
            return True
        page = doc.load_page(0)
        text = page.get_text("text")
        return len(text.strip()) < 100
    except Exception as e:
        print(f"   -> 警告: 檢查 PDF 是否掃描檔時出錯: {e}", file=sys.stderr)
        return True

def truncate_at_footer(text: str) -> str:
    """從文字中找到頁腳的起始位置，並移除所有後續內容。"""
    footer_patterns = [
        r'民\s*事\s*執\s*行\s*處',
        r'函\s*稿\s*代\s*碼',
        r'股\s*別\s*[:：]',
        r'擬\s*判\s*[:：]',
        r'司法事務官'
    ]
    earliest_match_pos = len(text)
    for pattern in footer_patterns:
        match = re.search(pattern, text)
        if match and match.start() < earliest_match_pos:
            earliest_match_pos = match.start()
    return text[:earliest_match_pos].strip()

def clean_and_format_text(text: str, section_type: str = None) -> str:
    """清理文字，移除重複的標題、頁碼雜訊並保留格式。"""
    if not text:
        return ""
    cleaned_text = re.sub(r'第\s*[一二三四五六七八九十百]+\s*頁\s*\(續上頁\)\s*(\n\s*\d+)?', '', text)
    if section_type:
        pattern = f"^{re.escape(section_type)}\\s*"
        cleaned_text = re.sub(pattern, "", cleaned_text.strip())
    else:
        cleaned_text = cleaned_text.strip()
    lines = cleaned_text.strip().split('\n')
    non_empty_lines = [line.strip() for line in lines if line.strip()]
    return "\n".join(non_empty_lines)

def detect_and_remove_overlap(text: str, section_type: str = None) -> str:
    """偵測並移除跨欄位的重複內容"""
    if not text:
        return ""
    lines = text.strip().split('\n')
    cleaned_lines = []
    other_section = '備註' if section_type == '使用情形' else '使用情形'
    for i, line in enumerate(lines):
        if other_section and re.match(f'^{re.escape(other_section)}\\s', line):
            if i > 0 and cleaned_lines:
                break
        elif section_type == '備註' and re.match(r'^標別\s*[:：]\s*', line):
            break
        else:
            cleaned_lines.append(line)
    return '\n'.join(cleaned_lines)

def parse_auction_pdf_minimal(pdf_path: str, case_number: str) -> Dict[str, Any]:
    """優化版 PDF 文字解析管線，改進邊界處理以避免重複文字"""
    if is_scanned_pdf(pdf_path):
        return {"error": "文件為掃描檔，無法自動解析。"}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_anchors = []
            full_text_for_header = ""
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text(x_tolerance=2) or ""
                full_text_for_header += page_text
                search_keywords = {
                    'BID_SECTION': r'標\s*別\s*[:：]\s*([\'"]?[甲乙丙丁戊己庚辛壬癸0-9A-Z]+[\'"]?)',
                    'USAGE': r'使用情形',
                    'REMARKS': r'備註'
                }
                for key, pattern in search_keywords.items():
                    matches = page.search(pattern, regex=True)
                    for match in matches:
                        if key in ['USAGE', 'REMARKS'] and match.get('x0', 0) > 100:
                            continue
                        match_text = match.get('text', '')
                        if key == 'BID_SECTION':
                            bid_extract = re.search(pattern, match_text)
                            if bid_extract and len(bid_extract.groups()) > 0 and bid_extract.group(1):
                                match_text = re.sub(r'^[\'"]|[\'"]$', '', bid_extract.group(1)).strip()
                        all_anchors.append({
                            'type': key, 'page_index': i, 'top': match['top'],
                            'bottom': match['bottom'], 'text': match_text, 'x0': match.get('x0', 0)
                        })
            all_anchors.sort(key=lambda x: (x['page_index'], x['top']))
            bid_anchors = [a for a in all_anchors if a['type'] == 'BID_SECTION']
            if not bid_anchors:
                bid_anchors.append({
                    'text': 'N/A', 'page_index': 0, 'top': 0,
                    'bottom': 20, 'type': 'BID_SECTION'
                })
            parsed_bid_sections = []
            for i, bid_anchor in enumerate(bid_anchors):
                current_section_data = {
                    "bidName": bid_anchor['text'], "header": "", "使用情形": "", "備註": ""
                }
                next_bid_anchor = bid_anchors[i + 1] if i + 1 < len(bid_anchors) else None
                section_content_anchors = []
                for anchor in all_anchors:
                    if anchor['type'] in ['USAGE', 'REMARKS']:
                        is_after_current = (anchor['page_index'] > bid_anchor['page_index'] or
                                          (anchor['page_index'] == bid_anchor['page_index'] and anchor['top'] > bid_anchor['top']))
                        is_before_next = True
                        if next_bid_anchor:
                            is_before_next = (anchor['page_index'] < next_bid_anchor['page_index'] or
                                              (anchor['page_index'] == next_bid_anchor['page_index'] and anchor['top'] < next_bid_anchor['top']))
                        if is_after_current and is_before_next:
                            section_content_anchors.append(anchor)
                section_content_anchors.sort(key=lambda x: (x['page_index'], x['top']))
                for j, content_anchor in enumerate(section_content_anchors):
                    start_page, start_y = content_anchor['page_index'], content_anchor['bottom']
                    if j + 1 < len(section_content_anchors):
                        end_anchor = section_content_anchors[j + 1]
                        end_page, end_y = end_anchor['page_index'], end_anchor['top']
                    elif next_bid_anchor:
                        end_page, end_y = next_bid_anchor['page_index'], next_bid_anchor['top']
                    else:
                        end_page, end_y = len(pdf.pages) - 1, pdf.pages[-1].height
                    full_text = ""
                    for page_idx in range(start_page, end_page + 1):
                        page = pdf.pages[page_idx]
                        top = start_y if page_idx == start_page else 0
                        bottom = end_y if page_idx == end_page else page.height
                        if top >= bottom: continue
                        cropped = page.crop((0, top, page.width, bottom))
                        page_text = cropped.extract_text(x_tolerance=3, y_tolerance=3) or ""
                        full_text += page_text
                    key_map = {'USAGE': '使用情形', 'REMARKS': '備註'}
                    section_key = key_map.get(content_anchor['type'])
                    if section_key == '備註':
                        full_text = truncate_at_footer(full_text)
                    clean_content = clean_and_format_text(full_text, section_key)
                    clean_content = detect_and_remove_overlap(clean_content, section_key)
                    if section_key and clean_content:
                        current_section_data[section_key] = clean_content
                header_match = re.search(r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)', full_text_for_header)
                if header_match:
                    case_no_clean = re.sub(r'\s', '', header_match.group(1))
                    current_section_data["header"] = f"{case_no_clean} 財產所有人: OOO"
                if current_section_data.get('使用情形') or current_section_data.get('備註'):
                    parsed_bid_sections.append(current_section_data)
            if parsed_bid_sections:
                return {"bidSections": parsed_bid_sections}
            else:
                return {"error": "無法從文件中擷取指定資訊。"}
    except Exception as e:
        print(f"   -> 嚴重錯誤: 解析 PDF 過程中發生未預期錯誤 ({case_number}): {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return {"error": f"（PDF 解析時發生嚴重錯誤: {e}）"}

def upload_to_gcs(bucket, blob_name: str, data: Any) -> bool:
    """上傳資料到 GCS"""
    try:
        blob = bucket.blob(blob_name)
        json_data_string = json.dumps(data, ensure_ascii=False, indent=4)
        blob.upload_from_string(json_data_string, content_type='application/json')
        return True
    except Exception as e:
        print(f"❌ 錯誤: 上傳至 GCS 失敗: {e}", file=sys.stderr)
        return False

def download_from_gcs(bucket, blob_name: str, local_path: str) -> bool:
    """從 GCS 下載檔案"""
    try:
        print(f"正在從 GCS Bucket ({bucket.name}) 下載檔案: {blob_name}...")
        source_blob = bucket.blob(blob_name)
        json_content_bytes = source_blob.download_as_bytes()
        with open(local_path, 'wb') as f:
            f.write(json_content_bytes)
        print(f"✅ 成功下載檔案至 {local_path}")
        return True
    except Exception as e:
        print(f"❌ 錯誤: 從 GCS 下載 {blob_name} 失敗: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return False

def main():
    """主程式"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    if not download_from_gcs(bucket, SOURCE_FILE_GCS, LOCAL_TEMP_INPUT_PATH):
        sys.exit(1)

    processed_data = {}
    
    try:
        output_blob = bucket.blob(OUTPUT_FILE_GCS)
        if output_blob.exists():
            print(f"發現現有的結果檔案 {OUTPUT_FILE_GCS}，載入進度...")
            try:
                existing_data = json.loads(output_blob.download_as_string())
                if isinstance(existing_data, list):
                    processed_data = {
                        item['caseNumber']: item 
                        for item in existing_data 
                        if 'caseNumber' in item
                    }
                    print(f"   -> 已成功載入 {len(processed_data)} 筆已處理的案件進度。")
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
            
            if case_num_str in processed_data:
                print(f"跳過已處理: {i+1}/{total} - 案號: {case_num_str}")
                continue
            
            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = (pdfs_list[0]['url'] if pdfs_list and len(pdfs_list) > 0 else None)

            if pdf_url and pdf_url != 'N/A':
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                for attempt in range(3):
                    try:
                        response = requests.get(pdf_url, timeout=45, headers=headers)
                        response.raise_for_status()
                        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
                            temp_pdf.write(response.content)
                            temp_pdf_path = temp_pdf.name
                        try:
                            auction_details = parse_auction_pdf_minimal(temp_pdf_path, case_num_str)
                        finally:
                            if os.path.exists(temp_pdf_path):
                                os.remove(temp_pdf_path)
                        break
                    except requests.exceptions.RequestException as e:
                        if attempt < 2: time.sleep(random.uniform(2, 4))
                        else:
                            print(f"   -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                            auction_details = {"error": f"（PDF 下載失敗: {e}）"}
                    except Exception as e:
                        print(f"   -> 錯誤: 處理 PDF 時發生未知錯誤 ({case_num_str}): {e}", file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                        auction_details = {"error": f"（處理 PDF 時發生未知錯誤: {e}）"}
                        break
                time.sleep(random.uniform(0.5, 1.5))
            else:
                auction_details = {"error": "（無可用的 PDF 連結）"}

            # *** 修改重點 ***
            # 建立只包含關鍵資訊的物件
            parsed_result = {
                'caseNumber': case_num_str,
                'auctionDetails': auction_details
            }
            processed_data[case_num_str] = parsed_result
            newly_processed_count += 1

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
            print(f"\n✅ 處理完成！已將 {len(output_list)} 筆案件的解析詳情儲存至 GCS 上的 {OUTPUT_FILE_GCS}")
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

