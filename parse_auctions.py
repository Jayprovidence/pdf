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
import signal

# --- GCS 設定 ---
GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025'
SOURCE_FILE_GCS = 'auctionData.json'
OUTPUT_FILE_GCS = 'auctionDataWithDetails.json'
LOCAL_TEMP_INPUT_PATH = './auctionData_temp.json'

# --- Timeout Handler ---
def timeout_handler(signum, frame):
    """當鬧鐘響起時，拋出 TimeoutError"""
    raise TimeoutError("單一 PDF 解析時間超過設定上限")

def is_scanned_pdf(pdf_path: str) -> bool:
    """使用 PyMuPDF 快速檢測 PDF 是否為掃描件。"""
    try:
        doc = fitz.open(pdf_path)
        if not doc.page_count: 
            return True
        # 增加對頁面文字內容的檢查，避免因頁面很小而誤判
        page_text_total_len = 0
        for page in doc:
            page_text_total_len += len(page.get_text("text").strip())
        
        return page_text_total_len < 100 * doc.page_count
    except Exception as e:
        print(f"   -> 警告: 檢查 PDF 是否掃描檔時出錯: {e}", file=sys.stderr)
        return True

def clean_and_format_text(text: str, section_type: Optional[str] = None) -> str:
    """清理文字，移除頁首/頁尾、重複標題並保留格式。"""
    if not text:
        return ""

    # 偵測並移除文件結尾的樣板文字
    footer_keywords = [
        '民事執行處', '函稿代碼', '股別', '擬判', '司法事務官', '書記官'
    ]
    for keyword in footer_keywords:
        if keyword in text:
            # 從第一個出現的關鍵字處切斷
            text = text.split(keyword, 1)[0]

    # 使用更強的正規表示式一次性移除頁碼雜訊 (跨越多行)
    # 例如: "第八頁(續上頁)\n01"
    text = re.sub(r'第\s*[一二三四五六七八九十百千]+\s*頁\s*\(續上頁\)\s*\n\s*\d+\s*', '', text)
    
    # 移除單行的頁碼標記
    lines = text.strip().split('\n')
    cleaned_lines = []
    for line in lines:
        stripped_line = line.strip()
        # 排除常見的頁首/頁尾標記
        if not re.fullmatch(r'(第\s*\S+\s*頁)|(（續上頁）)|(\d+)', stripped_line):
            cleaned_lines.append(line)
    
    return "\n".join(cleaned_lines).strip()


def parse_auction_pdf_minimal(pdf_path: str, case_number: str) -> Dict[str, Any]:
    """
    優化版 PDF 文字解析管線，處理掃描檔、毀損檔案和逾時問題
    """
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
                # 如果沒有標別，則將整個文件視為一個區段
                usage_anchor = next((a for a in all_anchors if a['type'] == 'USAGE'), None)
                remarks_anchor = next((a for a in all_anchors if a['type'] == 'REMARKS'), None)
                if usage_anchor or remarks_anchor:
                     bid_anchors.append({'text': 'N/A', 'page_index': 0, 'top': 0, 'type': 'BID_SECTION'})
                else:
                    # 如果連 使用情形/備註 都沒有，直接返回錯誤
                    return {"error": "文件中找不到'標別'、'使用情形'或'備註'等關鍵字。"}


            parsed_bid_sections = []
            
            for i, bid_anchor in enumerate(bid_anchors):
                current_section_data = {"bidName": bid_anchor['text'], "使用情形": "", "備註": ""}
                next_bid_anchor = bid_anchors[i + 1] if i + 1 < len(bid_anchors) else None
                
                section_content_anchors = []
                for anchor in all_anchors:
                    if anchor['type'] in ['USAGE', 'REMARKS']:
                        is_after_current = (anchor['page_index'] > bid_anchor['page_index'] or 
                                            (anchor['page_index'] == bid_anchor['page_index'] and anchor['top'] > bid_anchor['top']))
                        is_before_next = (not next_bid_anchor or anchor['page_index'] < next_bid_anchor['page_index'] or
                                          (anchor['page_index'] == next_bid_anchor['page_index'] and anchor['top'] < next_bid_anchor['top']))
                        if is_after_current and is_before_next:
                            section_content_anchors.append(anchor)
                
                section_content_anchors.sort(key=lambda x: (x['page_index'], x['top']))
                
                for j, content_anchor in enumerate(section_content_anchors):
                    start_page, start_y = content_anchor['page_index'], content_anchor['bottom']
                    
                    end_anchor = section_content_anchors[j + 1] if j + 1 < len(section_content_anchors) else next_bid_anchor
                    if end_anchor:
                        end_page, end_y = end_anchor['page_index'], end_anchor['top']
                    else:
                        end_page, end_y = len(pdf.pages) - 1, pdf.pages[-1].height
                    
                    full_text = ""
                    for page_idx in range(start_page, end_page + 1):
                        page = pdf.pages[page_idx]
                        top = start_y if page_idx == start_page else 0
                        bottom = end_y if page_idx == end_page else page.height
                        if top >= bottom: continue
                        
                        bbox = (0, top, page.width, bottom)
                        cropped = page.crop(bbox)
                        page_text = cropped.extract_text(x_tolerance=3, y_tolerance=3) or ""
                        full_text += page_text
                    
                    key_map = {'USAGE': '使用情形', 'REMARKS': '備註'}
                    section_key = key_map.get(content_anchor['type'])
                    
                    if section_key:
                        current_section_data[section_key] = clean_and_format_text(full_text)
                
                if current_section_data.get('使用情形') or current_section_data.get('備註'):
                    parsed_bid_sections.append(current_section_data)

            if parsed_bid_sections:
                return {"bidSections": parsed_bid_sections}
            else:
                # 即使找到錨點，也可能因版面配置問題無法擷取到文字
                return {"error": "無法從文件中擷取有效的公告內容。"}

    except Exception as e:
        error_message = str(e)
        # **【新增】** 檢查是否為特定的字型毀損錯誤
        if "FontBBox" in error_message:
            specific_error = f"（PDF 檔案字型描述符毀損 (FontBBox)，無法解析）"
            print(f"   -> 警告: 偵測到 PDF 字型問題 ({case_number}): {error_message}", file=sys.stderr)
            return {"error": specific_error}
        else:
            # 維持對其他通用錯誤的處理
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
                existing_data_raw = output_blob.download_as_string()
                existing_data = json.loads(existing_data_raw)
                if isinstance(existing_data, list):
                    processed_data = {item['caseNumber']: item for item in existing_data if 'caseNumber' in item}
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
                # print(f"跳過已處理: {i+1}/{total} - 案號: {case_num_str}")
                continue
            
            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            pdfs_list = case_data.get('assets', {}).get('pdfs')
            pdf_url = pdfs_list[0]['url'] if pdfs_list else None

            if pdf_url and pdf_url != 'N/A':
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                
                # 在處理單一檔案的區塊設定鬧鐘
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(60)  # 設定 60 秒的鬧鐘

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
                    
                except TimeoutError:
                    print(f"   -> 嚴重錯誤: 解析 PDF 超過 60 秒 ({case_num_str})", file=sys.stderr)
                    auction_details = {"error": "（PDF 解析逾時，可能檔案過於複雜或毀損）"}
                except requests.exceptions.RequestException as e:
                    print(f"   -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                    auction_details = {"error": f"（PDF 下載失敗: {e}）"}
                except Exception as e:
                    print(f"   -> 錯誤: 處理 PDF 時發生未知錯誤 ({case_num_str}): {e}", file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                    auction_details = {"error": f"（處理 PDF 時發生未知錯誤: {e}）"}
                finally:
                    signal.alarm(0) # 無論如何，都要取消鬧鐘

                time.sleep(random.uniform(0.5, 1.5))
            else:
                auction_details = {"error": "（無可用的 PDF 連結）"}

            # 只儲存解析相關的資訊
            final_result = {
                "caseNumber": case_num_str,
                "auctionDetails": auction_details
            }
            processed_data[case_num_str] = final_result
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
            print(f"\n✅ 處理完成！已將 {len(output_list)} 筆案件的詳細資訊儲存至 GCS 上的 {OUTPUT_FILE_GCS}")
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

