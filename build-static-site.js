const ejs = require('ejs');
const fs = require('fs-extra');
const path = require('path');
const { Storage } = require('@google-cloud/storage');

// --- GCS 設定 ---
const GCS_BUCKET_NAME = 'foreclosure-data-bucket-lin-2025';
const SOURCE_FILE_GCS = 'auctionDataWithDetails.json';

// --- 本地路徑設定 ---
// *** 已修正：移除 '..'，讓腳本在目前目錄下尋找 case-template.ejs ***
const TEMPLATE_PATH = path.join(__dirname, 'case-template.ejs');
const DIST_PATH = path.join(__dirname, 'dist');

async function main() {
    console.log('--- 靜態網站生成開始 ---');

    const storage = new Storage();
    const bucket = storage.bucket(GCS_BUCKET_NAME);

    try {
        // 1. 下載已包含所有詳細資訊的合併後檔案
        console.log(`正在從 GCS 下載來源檔案: ${SOURCE_FILE_GCS}...`);
        const [sourceBuffer] = await bucket.file(SOURCE_FILE_GCS).download();
        const mergedCases = JSON.parse(sourceBuffer.toString());
        console.log(`✅ 成功下載並解析 ${mergedCases.length} 筆已合併的案件資料。`);

        // 2. 準備模板和輸出目錄
        const template = fs.readFileSync(TEMPLATE_PATH, 'utf-8');
        await fs.ensureDir(DIST_PATH);
        console.log(`模板讀取成功，輸出目錄 '${DIST_PATH}' 已準備就緒。`);

        // 3. 遍歷合併後的資料並生成 HTML 檔案
        let successCount = 0;
        for (const caseData of mergedCases) {
            if (!caseData.caseNumber) continue;

            const filename = `${caseData.caseNumber}.html`;
            const filepath = path.join(DIST_PATH, filename);
            
            try {
                // 將 caseData 傳遞給模板
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
