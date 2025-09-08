const ejs = require('ejs');
const fs = require('fs-extra');
const path = require('path');
const { Storage } = require('@google-cloud/storage');

// --- GCS 設定 ---
const GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025';
const SOURCE_FILE_GCS = 'auctionData.json';
const DETAILS_FILE_GCS = 'auctionDataWithDetails.json';

// --- 本地路徑設定 ---
const TEMPLATE_PATH = path.join(__dirname, '..', 'case-template.ejs');
const DIST_PATH = path.join(__dirname, '..', 'dist');

async function main() {
    console.log('--- 靜態網站生成開始 ---');

    const storage = new Storage();
    const bucket = storage.bucket(GCS_BUCKET_NAME);

    try {
        // 1. 下載兩個資料檔案
        console.log(`正在從 GCS 下載來源檔案: ${SOURCE_FILE_GCS}...`);
        const [sourceBuffer] = await bucket.file(SOURCE_FILE_GCS).download();
        const sourceJson = JSON.parse(sourceBuffer.toString());
        const allCases = sourceJson.data;
        console.log(`✅ 成功下載並解析 ${allCases.length} 筆主要案件資料。`);

        console.log(`正在從 GCS 下載詳細資料檔案: ${DETAILS_FILE_GCS}...`);
        const [detailsBuffer] = await bucket.file(DETAILS_FILE_GCS).download();
        const detailsList = JSON.parse(detailsBuffer.toString());
        console.log(`✅ 成功下載並解析 ${detailsList.length} 筆案件的詳細公告內容。`);
        
        // 2. 建立詳細資料的查找表 (Map)
        const detailsMap = new Map(detailsList.map(item => [item.caseNumber, item.auctionDetails]));

        // 3. 智慧合併資料
        const mergedCases = allCases.map(caseItem => {
            const details = detailsMap.get(caseItem.caseNumber);
            if (details) {
                return { ...caseItem, auctionDetails: details };
            }
            return { ...caseItem, auctionDetails: null };
        });
        console.log('✅ 成功將主要資料與詳細公告內容合併。');


        // 4. 準備模板和輸出目錄
        const template = fs.readFileSync(TEMPLATE_PATH, 'utf-8');
        await fs.ensureDir(DIST_PATH);
        console.log(`模板讀取成功，輸出目錄 '${DIST_PATH}' 已準備就緒。`);

        // 5. 遍歷合併後的資料並生成 HTML 檔案
        let successCount = 0;
        for (const caseData of mergedCases) {
            if (!caseData.caseNumber) continue;

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
        
        console.log(`\n🎉 處理完成！成功生成 ${successCount} 個 HTML 檔案。`);

    } catch (error) {
        console.error('❌ 在生成過程中發生嚴重錯誤:', error);
        process.exit(1);
    }
}

main();
