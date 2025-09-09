const ejs = require('ejs');
const fs = require('fs-extra');
const path = require('path');
const { Storage } = require('@google-cloud/storage');

// --- GCS 設定 ---
const GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025';
const SOURCE_FILE_GCS = 'auctionData.json';
const DETAILS_FILE_GCS = 'auctionDataWithDetails.json';

// --- 本地路徑設定 ---
const TEMPLATE_PATH = path.join(__dirname, 'case-template.ejs');
const DIST_PATH = path.join(__dirname, 'dist');

async function main() {
    console.log('--- 靜態網站生成開始 ---');

    const storage = new Storage();
    const bucket = storage.bucket(GCS_BUCKET_NAME);

    try {
        // 1. 下載主要的案件資料
        console.log(`正在從 GCS 下載主要案件檔案: ${SOURCE_FILE_GCS}...`);
        const [sourceBuffer] = await bucket.file(SOURCE_FILE_GCS).download();
        const allCasesData = JSON.parse(sourceBuffer.toString()).data || [];
        console.log(`✅ 成功下載並解析 ${allCasesData.length} 筆主要案件資料。`);

        // 2. 下載 PDF 解析後的詳細資料
        console.log(`正在從 GCS 下載解析後的公告檔案: ${DETAILS_FILE_GCS}...`);
        let detailsList = [];
        const detailsFile = bucket.file(DETAILS_FILE_GCS);
        const [detailsExists] = await detailsFile.exists();
        if (detailsExists) {
            const [detailsBuffer] = await detailsFile.download();
            detailsList = JSON.parse(detailsBuffer.toString());
            console.log(`✅ 成功下載並解析 ${detailsList.length} 筆公告詳情。`);
        } else {
            console.warn(`⚠️ 警告：找不到公告詳情檔案 ${DETAILS_FILE_GCS}，將不會生成任何頁面。`);
            return;
        }

        // 3. 建立 Map
        const detailsMap = new Map();
        for (const detail of detailsList) {
            if (detail.caseNumber) {
                detailsMap.set(detail.caseNumber, detail.auctionDetails);
            }
        }
        console.log('公告詳情查找表建立完成。');

        // 4. 準備模板和輸出目錄
        const template = fs.readFileSync(TEMPLATE_PATH, 'utf-8');
        await fs.ensureDir(DIST_PATH);
        await fs.emptyDir(DIST_PATH);
        console.log(`模板讀取成功，輸出目錄 '${DIST_PATH}' 已準備就緒並清空。`);

        // 5. 遍歷並生成 HTML
        let successCount = 0;
        let skippedCount = 0;

        for (const caseData of allCasesData) {
            if (!caseData.caseNumber) continue;

            const auctionDetails = detailsMap.get(caseData.caseNumber);
            
            // *** 修改重點：加入 else 區塊來印出錯誤日誌 ***
            if (auctionDetails) {
                if (auctionDetails.error) {
                    // 如果有錯誤，印出日誌並跳過
                    console.log(`[略過] 案號 ${caseData.caseNumber} 因解析錯誤而被跳過: ${auctionDetails.error}`);
                    skippedCount++;
                } else {
                    // 如果沒有錯誤，才生成頁面
                    caseData.auctionDetails = auctionDetails;
                    const filename = `${caseData.caseNumber}.html`;
                    const filepath = path.join(DIST_PATH, filename);
                    
                    try {
                        const html = ejs.render(template, { caseData: caseData });
                        fs.writeFileSync(filepath, html);
                        successCount++;
                    } catch (renderError) {
                        console.error(`❌ 渲染案件 ${caseData.caseNumber} 時發生錯誤:`, renderError);
                    }
                }
            }
        }
        
        console.log(`\n🎉 處理完成！`);
        console.log(`   -> 成功生成 ${successCount} 個 HTML 檔案。`);
        console.log(`   -> 因解析錯誤而跳過 ${skippedCount} 個案件。`);

    } catch (error) {
        console.error('❌ 在生成過程中發生嚴重錯誤:', error);
        process.exit(1);
    }
}

main();

