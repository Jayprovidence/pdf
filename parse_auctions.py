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

def clean_and_format_text(text: str, section_type: str = None) -> str:
    """清理文字，移除重複的標題並保留格式。
    
    Args:
        text: 要清理的文字
        section_type: 區段類型 ('使用情形' 或 '備註')
    """
    if not text:
        return ""
    
    # 移除標題關鍵字（如果出現在開頭）
    if section_type:
        # 移除該區段的標題，但保留可能出現在內容中的其他區段標題
        pattern = f"^{re.escape(section_type)}\\s*"
        cleaned_text = re.sub(pattern, "", text.strip())
    else:
        cleaned_text = text.strip()
    
    # 分割成行並清理
    lines = cleaned_text.strip().split('\n')
    non_empty_lines = []
    
    for line in lines:
        line = line.strip()
        # 跳過頁碼標記
        if line and not re.match(r'^(第\s*\S+\s*頁|（續上頁）)$', line.strip()):
            non_empty_lines.append(line)
    
    return "\n".join(non_empty_lines)

def detect_and_remove_overlap(text: str, section_type: str = None) -> str:
    """偵測並移除跨欄位的重複內容
    
    Args:
        text: 要處理的文字
        section_type: 區段類型 ('使用情形' 或 '備註')
    
    Returns:
        處理後的文字
    """
    if not text:
        return ""
    
    lines = text.strip().split('\n')
    cleaned_lines = []
    
    # 檢查是否有其他區段的標題混入
    other_section = '備註' if section_type == '使用情形' else '使用情形'
    
    for i, line in enumerate(lines):
        # 如果在當前區段中發現其他區段的標題，且該標題出現在行首
        if other_section and re.match(f'^{re.escape(other_section)}\\s', line):
            # 這可能是重複內容的開始，檢查前一行
            if i > 0 and cleaned_lines:
                # 移除前一行（可能是重複的結尾）
                # 但保留當前行之前的所有內容
                break
        # 檢查是否有「標別:」這類的標記出現在備註中
        elif section_type == '備註' and re.match(r'^標別\s*[:：]\s*', line):
            # 這是下一個標別的開始，應該停止
            break
        else:
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def parse_auction_pdf_minimal(pdf_path: str, case_number: str) -> Dict[str, Any]:
    """
    優化版 PDF 文字解析管線，改進邊界處理以避免重複文字
    """
    if is_scanned_pdf(pdf_path):
        return {"error": "文件為掃描檔，無法自動解析。"}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_anchors = []
            full_text_for_header = ""
            
            # 第一遍掃描：收集所有錨點
            for i, page in enumerate(pdf.pages):
                page_text = page.extract_text(x_tolerance=2) or ""
                full_text_for_header += page_text
                
                # 搜尋關鍵字
                search_keywords = {
                    'BID_SECTION': r'標\s*別\s*[:：]\s*([\'"]?[甲乙丙丁戊己庚辛壬癸0-9A-Z]+[\'"]?)',
                    'USAGE': r'使用情形',
                    'REMARKS': r'備註'
                }
                
                for key, pattern in search_keywords.items():
                    matches = page.search(pattern, regex=True)
                    for match in matches:
                        # 過濾掉不在左側的使用情形和備註標記
                        if key in ['USAGE', 'REMARKS'] and match.get('x0', 0) > 100:
                            continue
                        
                        match_text = match.get('text', '')
                        
                        # 特殊處理標別提取
                        if key == 'BID_SECTION':
                            bid_extract = re.search(pattern, match_text)
                            if bid_extract and len(bid_extract.groups()) > 0 and bid_extract.group(1):
                                match_text = re.sub(r'^[\'"]|[\'"]$', '', bid_extract.group(1)).strip()
                        
                        all_anchors.append({
                            'type': key,
                            'page_index': i,
                            'top': match['top'],
                            'bottom': match['bottom'],
                            'text': match_text,
                            'x0': match.get('x0', 0)
                        })

            # 按頁面和位置排序錨點
            all_anchors.sort(key=lambda x: (x['page_index'], x['top']))
            
            # 分離標別錨點
            bid_anchors = [a for a in all_anchors if a['type'] == 'BID_SECTION']
            
            # 如果沒有找到標別，創建預設標別
            if not bid_anchors:
                bid_anchors.append({
                    'text': 'N/A',
                    'page_index': 0,
                    'top': 0,
                    'bottom': 20,
                    'type': 'BID_SECTION'
                })

            # 解析每個標別區段
            parsed_bid_sections = []
            
            for i, bid_anchor in enumerate(bid_anchors):
                current_section_data = {
                    "bidName": bid_anchor['text'],
                    "header": "",
                    "使用情形": "",
                    "備註": ""
                }
                
                # 確定下一個標別的位置（如果存在）
                next_bid_anchor = bid_anchors[i + 1] if i + 1 < len(bid_anchors) else None
                
                # 找出屬於當前標別的內容錨點
                section_content_anchors = []
                for anchor in all_anchors:
                    if anchor['type'] in ['USAGE', 'REMARKS']:
                        # 檢查是否在當前標別之後
                        is_after_current = (
                            anchor['page_index'] > bid_anchor['page_index'] or
                            (anchor['page_index'] == bid_anchor['page_index'] and 
                             anchor['top'] > bid_anchor['top'])
                        )
                        
                        # 檢查是否在下一個標別之前
                        is_before_next = True
                        if next_bid_anchor:
                            is_before_next = (
                                anchor['page_index'] < next_bid_anchor['page_index'] or
                                (anchor['page_index'] == next_bid_anchor['page_index'] and 
                                 anchor['top'] < next_bid_anchor['top'])
                            )
                        
                        if is_after_current and is_before_next:
                            section_content_anchors.append(anchor)
                
                # 排序內容錨點
                section_content_anchors.sort(key=lambda x: (x['page_index'], x['top']))
                
                # 解析每個內容區段
                for j, content_anchor in enumerate(section_content_anchors):
                    # 確定內容的起始位置
                    start_page = content_anchor['page_index']
                    start_y = content_anchor['bottom']  # 從錨點底部開始
                    
                    # 確定內容的結束位置
                    if j + 1 < len(section_content_anchors):
                        # 下一個內容錨點的頂部
                        end_anchor = section_content_anchors[j + 1]
                        end_page = end_anchor['page_index']
                        end_y = end_anchor['top']
                    elif next_bid_anchor:
                        # 下一個標別的頂部
                        end_page = next_bid_anchor['page_index']
                        end_y = next_bid_anchor['top']
                    else:
                        # 文件結尾
                        end_page = len(pdf.pages) - 1
                        end_y = pdf.pages[end_page].height
                    
                    # 擷取文字
                    full_text = ""
                    for page_idx in range(start_page, end_page + 1):
                        page = pdf.pages[page_idx]
                        
                        # 計算該頁的擷取邊界
                        if page_idx == start_page:
                            top = start_y
                        else:
                            top = 0
                            
                        if page_idx == end_page:
                            bottom = end_y
                        else:
                            bottom = page.height
                        
                        # 確保邊界有效
                        if top >= bottom:
                            continue
                            
                        # 擷取指定區域的文字
                        bbox = (0, top, page.width, bottom)
                        cropped = page.crop(bbox)
                        page_text = cropped.extract_text(x_tolerance=3, y_tolerance=3) or ""
                        full_text += page_text
                    
                    # 確定區段類型
                    key_map = {'USAGE': '使用情形', 'REMARKS': '備註'}
                    section_key = key_map.get(content_anchor['type'])
                    
                    # 清理文字（傳入區段類型以進行更精確的清理）
                    clean_content = clean_and_format_text(full_text, section_key)
                    
                    # 偵測並移除跨欄位的重複內容
                    clean_content = detect_and_remove_overlap(clean_content, section_key)
                    
                    # 儲存內容
                    if section_key and clean_content:
                        current_section_data[section_key] = clean_content
                
                # 提取標題資訊
                header_match = re.search(
                    r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)',
                    full_text_for_header
                )
                if header_match:
                    case_no_clean = re.sub(r'\s', '', header_match.group(1))
                    current_section_data["header"] = f"{case_no_clean} 財產所有人: OOO"
                
                # 只有在有實際內容時才加入結果
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
        
        # 使用 download_as_bytes() 並寫入本地檔案
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
    
    # 下載來源檔案
    if not download_from_gcs(bucket, SOURCE_FILE_GCS, LOCAL_TEMP_INPUT_PATH):
        sys.exit(1)

    processed_data = {}
    
    try:
        # 載入已處理的進度（如果存在）
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
        
        # 載入待處理資料
        with open(LOCAL_TEMP_INPUT_PATH, 'r', encoding='utf-8') as f:
            auction_data = json.load(f)
        
        all_cases = auction_data.get('data', [])
        total = len(all_cases)
        newly_processed_count = 0
        
        print(f"總共找到 {total} 筆案件，準備開始處理...")

        for i, case_data in enumerate(all_cases):
            case_num_str = case_data.get('caseNumber', f'UNKNOWN_{i}')
            
            # 跳過已處理的案件
            if case_num_str in processed_data:
                print(f"跳過已處理: {i+1}/{total} - 案號: {case_num_str}")
                continue
            
            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = (
                pdfs_list[0]['url'] 
                if pdfs_list and isinstance(pdfs_list, list) 
                and len(pdfs_list) > 0 and isinstance(pdfs_list[0], dict) 
                else None
            )

            if pdf_url and pdf_url != 'N/A':
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                for attempt in range(3):
                    try:
                        response = requests.get(pdf_url, timeout=45, headers=headers)
                        response.raise_for_status()
                        
                        # 儲存 PDF 到暫存檔案
                        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
                            temp_pdf.write(response.content)
                            temp_pdf_path = temp_pdf.name
                        
                        try:
                            # 解析 PDF
                            auction_details = parse_auction_pdf_minimal(temp_pdf_path, case_num_str)
                        finally:
                            # 清理暫存檔案
                            if os.path.exists(temp_pdf_path):
                                os.remove(temp_pdf_path)
                        
                        break
                        
                    except requests.exceptions.RequestException as e:
                        if attempt < 2:
                            time.sleep(random.uniform(2, 4))
                        else:
                            print(f"   -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                            auction_details = {"error": f"（PDF 下載失敗: {e}）"}
                    except Exception as e:
                        print(f"   -> 錯誤: 處理 PDF 時發生未知錯誤 ({case_num_str}): {e}", file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                        auction_details = {"error": f"（處理 PDF 時發生未知錯誤: {e}）"}
                        break
                
                # 加入延遲以避免過度請求
                time.sleep(random.uniform(0.5, 1.5))
            else:
                auction_details = {"error": "（無可用的 PDF 連結）"}

            # 儲存處理結果
            case_data['auctionDetails'] = auction_details
            processed_data[case_num_str] = case_data
            newly_processed_count += 1

            # 定期儲存進度
            if newly_processed_count > 0 and newly_processed_count % 25 == 0:
                print(f"\n已處理 {newly_processed_count} 筆新案件，正在儲存進度至 GCS...")
                output_list = list(processed_data.values())
                if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
                    print("✅ 進度儲存成功。")
                else:
                    print("❌ 進度儲存失敗。")

        # 最終儲存
        print("\n所有案件處理完畢，正在進行最終儲存...")
        output_list = list(processed_data.values())
        if upload_to_gcs(bucket, OUTPUT_FILE_GCS, output_list):
            print(f"\n✅ 處理完成！已將 {len(output_list)} 筆案件的詳細資訊儲存至 GCS 上的 {OUTPUT_FILE_GCS}")
        else:
            # 本地備份
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
    main()
