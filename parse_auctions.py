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

def is_land_header(line):
    """更寬鬆地判斷是否為土地表格標頭"""
    return '土地拍賣明細' in line or (
        '編' in line and '地' in line and '號' in line and '坐' in line and '面積' in line
    )

def is_building_header(line):
    """更寬鬆地判斷是否為建物表格標頭"""
    return '建物拍賣明細' in line or (
        '建號' in line and '面積' in line and ('門牌' in line or '基地' in line or '建築式' in line)
    )

def parse_auction_pdf(text, case_number):
    """
    強健的 PDF 解析函式 v12 (逐行重組架構)，能正確處理多標別與複雜表格結構。
    @param {string} text - 從 pdfplumber 取得的原始 PDF 文字
    @returns {dict | None} - 標準化的拍賣公告物件
    """
    if not text:
        return None

    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        line = line.strip()
        if not line or re.match(r'^第 \d+ 頁', line) or re.match(r'^\(續上頁\)', line) or "函 稿 代 碼" in line or "民 事 執 行 處" in line or "司 法 事 務 官" in line:
            continue
        clean_lines.append(line)

    parsed_bid_sections = []
    current_section = None
    current_mode = 'header'
    last_other_key = None

    def finalize_section(section):
        if not section: return None
        for item_list in [section.get('lands', []), section.get('buildings', [])]:
            for item in item_list:
                for key, value in item.items():
                    item[key] = re.sub(r'\s+', ' ', str(value)).strip()
        if section.get("lands") or section.get("buildings") or section.get("otherSections"):
            return section
        return None
    
    def start_new_section(bid_match):
        nonlocal current_section, current_mode
        if current_section:
            finalized = finalize_section(current_section)
            if finalized:
                parsed_bid_sections.append(finalized)
        
        bid_name = bid_match.group(1).strip() if bid_match else "N/A"
        current_section = {"bidName": bid_name, "header": "", "lands": [], "buildings": [], "otherSections": {}}
        current_mode = 'header'

    for line in clean_lines:
        line_strip = line.strip()

        bid_match = re.search(r'標\s*別\s*[:：]\s*([甲乙丙丁戊己庚辛壬癸0-9A-Z]+)', line_strip)
        
        if bid_match:
            start_new_section(bid_match)
            continue
        
        if current_section is None:
            start_new_section(None)

        # 優先切換模式
        if is_land_header(line_strip):
            current_mode = 'land'
            continue
        if is_building_header(line_strip):
            current_mode = 'building'
            continue
        
        is_other_keyword = False
        for keyword in ["點交情形", "使用情形", "備註"]:
            if line_strip.startswith(keyword):
                current_mode = 'other'
                last_other_key = keyword
                is_other_keyword = True
                content = line_strip[len(keyword):].lstrip(' :：')
                current_section['otherSections'][keyword] = (current_section['otherSections'].get(keyword, '') + ' ' + content).strip()
                break
        if is_other_keyword:
            continue

        try:
            if current_mode == 'land':
                if re.match(r'^\d+', line_strip):
                    cols = re.split(r'\s{2,}', line_strip)
                    if len(cols) >= 5:
                        duan_raw = cols[3]
                        duan_parts = duan_raw.replace('段',' 段').strip().split()
                        land_item = {
                            "編號": cols[0], "縣市": cols[1], "鄉鎮市區": cols[2],
                            "段": duan_parts[0] if duan_parts else '', "小段": duan_parts[1] if len(duan_parts) > 1 else '', 
                            "地號": cols[4], "面積(m²)": cols[5] if len(cols) > 5 else '',
                            "權利範圍": cols[6] if len(cols) > 6 else '', "價格(元)": cols[7] if len(cols) > 7 else '',
                            "備考": ' '.join(cols[8:]) if len(cols) > 8 else ''
                        }
                        current_section['lands'].append(land_item)
                    elif current_section.get('lands'):
                        current_section['lands'][-1]['備考'] = (current_section['lands'][-1].get('備考', '') + ' ' + line_strip).strip()
                elif current_section.get('lands'):
                    current_section['lands'][-1]['備考'] = (current_section['lands'][-1].get('備考', '') + ' ' + line_strip).strip()
            
            elif current_mode == 'building':
                if re.match(r'^\d+', line_strip):
                    cols = re.split(r'\s{2,}', line_strip)
                    if len(cols) >= 3:
                        building_item = {
                            "編號": cols[0], "建號": cols[1], "建物門牌": cols[2],
                            "主要建材/層數": cols[3] if len(cols) > 3 else '', "面積(m²)": cols[4] if len(cols) > 4 else '',
                            "附屬建物": cols[5] if len(cols) > 5 else '', "權利範圍": cols[6] if len(cols) > 6 else '',
                            "價格(元)": cols[7] if len(cols) > 7 else '', "備考": ' '.join(cols[8:]) if len(cols) > 8 else ''
                        }
                        current_section['buildings'].append(building_item)
                    elif current_section.get('buildings'):
                        current_section['buildings'][-1]['備考'] = (current_section['buildings'][-1].get('備考', '') + ' ' + line_strip).strip()
                elif current_section.get('buildings'):
                    current_section['buildings'][-1]['備考'] = (current_section['buildings'][-1].get('備考', '') + ' ' + line_strip).strip()
            
            elif current_mode == 'other' and last_other_key:
                current_section['otherSections'][last_other_key] += ' ' + line_strip
            
            elif current_mode == 'header' and not current_section.get("header"):
                header_match = re.search(r'(\d+\s*年\s*司\s*執\s*\S*\s*字\s*(?:第)?\s*\d+\s*號)', line)
                if header_match:
                    case_no = re.sub(r'\s', '', header_match.group(1))
                    current_section["header"] = f"{case_no} 財產所有人: OOO"
        except IndexError:
            if current_mode == 'building' and current_section.get('buildings'):
                current_section['buildings'][-1]['備考'] = (current_section['buildings'][-1].get('備考', '') + ' ' + line_strip).strip()
            elif current_mode == 'land' and current_section.get('lands'):
                current_section['lands'][-1]['備考'] = (current_section['lands'][-1].get('備考', '') + ' ' + line_strip).strip()
            elif current_mode == 'other' and last_other_key:
                 current_section['otherSections'][last_other_key] += ' ' + line_strip
            continue

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
                
                if isinstance(existing_data, dict) and 'data' in existing_data:
                    print("  -> 偵測到舊的資料格式 ('data' key)，為確保資料正確性，將重新處理所有案件。")
                    processed_details = {} 
                elif isinstance(existing_data, list):
                    if existing_data and ('caseNumber' not in existing_data[0] or 'auctionDetails' not in existing_data[0]):
                         print("  -> 偵測到格式不符的列表，將重新處理所有案件。")
                         processed_details = {}
                    else:
                        processed_details = {item['caseNumber']: item['auctionDetails'] for item in existing_data if 'caseNumber' in item}
                        print(f"  -> 已成功載入 {len(processed_details)} 筆已處理的案件進度。")
                else:
                    print("  -> 警告: 未知的進度檔案格式，將重新開始。")
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
            
            details = processed_details.get(case_num_str)
            if details and 'error' not in details and details is not None:
                print(f"正在處理: {i+1}/{total} - 案號: {case_num_str} (已處理，跳過)")
                continue

            print(f"正在處理: {i+1}/{total} - 案號: {case_num_str}")
            
            auction_details = None
            if (case_data.get('assets') and case_data['assets'].get('pdfs') and 
                isinstance(case_data['assets']['pdfs'], list) and len(case_data['assets']['pdfs']) > 0):
                
                pdf_url = case_data['assets']['pdfs'][0].get('url')
                if pdf_url and pdf_url != 'N/A':
                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                    for attempt in range(3):
                        try:
                            response = requests.get(pdf_url, timeout=30, headers=headers)
                            response.raise_for_status()
                            
                            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                                full_text = "".join([page.extract_text() for page in pdf.pages if page.extract_text() is not None])
                            
                            auction_details = parse_auction_pdf(full_text, case_num_str)
                            if not auction_details or not auction_details.get("bidSections"):
                                 print(f"  -> 警告: 案號 {case_num_str} 的 PDF 內容無法成功解析。")
                                 auction_details = { "error": "（PDF 內容解析失敗，請參閱原始文件）" }
                            break 
                        except requests.exceptions.RequestException as e:
                            if attempt < 2:
                                time.sleep(1)
                            else:
                                print(f"  -> 錯誤: 下載 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                                auction_details = { "error": f"（PDF 下載失敗: {e}）" }
                        except Exception as e:
                            print(f"  -> 錯誤: 解析 PDF 失敗 ({case_num_str}): {e}", file=sys.stderr)
                            auction_details = { "error": f"（PDF 解析時發生錯誤: {e}）" }
                            break
                    time.sleep(random.uniform(0.2, 0.8))

            processed_details[case_num_str] = auction_details
            newly_processed_count += 1

            if newly_processed_count > 0 and newly_processed_count % 50 == 0:
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
