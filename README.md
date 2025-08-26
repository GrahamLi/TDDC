# 股權分佈資料分析與視覺化系統
TDCC Data Analysis & Visualization System v1.0

## 專案簡介

本專案提供一個完整的台股股權分佈數據分析系統，從資料爬取、整理到深度分析與視覺化，建立一個自動化的工作流程。

### 系統架構

```
[程式 1: 資料抓取] → [本地資料庫] → [程式 2: 查詢與整理] → [程式 3: 分析與繪圖]
     ↓                    ↓                    ↓                      ↓
  TDCC網站           股票資料夾          Excel報表+K線圖        趨勢分析圖表
```

## 安裝說明

### 系統需求

- Python 3.8 或以上版本
- Google Chrome 瀏覽器（用於網頁爬蟲）
- ChromeDriver（與Chrome版本相符）

### 安裝步驟

1. 複製專案到本地：
```bash
git clone <repository_url>
cd stock-analysis-system
```

2. 安裝Python套件：
```bash
pip install -r requirements.txt
```

3. 安裝ChromeDriver：
   - 下載對應Chrome版本的ChromeDriver：https://chromedriver.chromium.org/
   - 將ChromeDriver放到系統PATH中，或放在專案目錄下

## 使用說明

### 程式一：股權分佈資料爬蟲 (program1_tdcc_scraper.py)

從TDCC網站爬取台股股權分佈歷史數據，建立本地資料庫。

#### 功能特點：
- 自動從MoneyDJ獲取台股清單
- 排除ETF、債券等非個股
- 增量更新（只下載新數據）
- 自動重試與錯誤處理

#### 使用方法：

```bash
# 爬取所有股票數據
python program1_tdcc_scraper.py

# 測試模式（只爬取前5支股票）
# 修改程式中的 main() 函數：
# scraper.run(limit=5)
```

#### SSL 驗證設定：

爬蟲預設會驗證 HTTPS 憑證。可透過環境變數 `TDCC_SSL_VERIFY` 調整行為：

- 未設定：使用系統預設 CA
- 設為 CA 檔案路徑：使用指定憑證檔
- 設為 `0` 或 `false`：停用驗證（僅限測試環境，正式執行請提供正確 CA 憑證）

#### 輸出結果：
- 建立 `stock_data/` 目錄
- 每支股票一個子目錄（例如：`stock_data/2330/`）
- 每個日期一個JSON檔案（例如：`2024-01-15.json`）

### 程式二：資料查詢與整理 (program2_data_query.py)

從本地資料庫查詢指定期間的數據，並疊加K線圖。

#### 功能特點：
- 智慧日期匹配（自動找最接近的可用日期）
- 從Wearn.com獲取K線數據
- 產生三種表格：人數、股數、占比
- K線圖與成交量疊加顯示

#### 使用方法：

```bash
# 基本查詢
python program2_data_query.py 2330 2024-01-01 2024-03-31

# 指定輸出檔名
python program2_data_query.py 2330 2024-01-01 2024-03-31 --output my_report.xlsx

# 指定資料目錄
python program2_data_query.py 2330 2024-01-01 2024-03-31 --data-dir /path/to/data
```

#### 參數說明：
- `stock_code`: 股票代碼（必填）
- `start_date`: 起始日期，格式 YYYY-MM-DD（必填）
- `end_date`: 結束日期，格式 YYYY-MM-DD（必填）
- `--output`: 輸出檔案名稱（選填，預設為 `{股號}_{起始日}_{結束日}_analysis.xlsx`）
- `--data-dir`: 資料目錄路徑（選填，預設為 `stock_data`）

#### 輸出結果：
- 一個Excel檔案，包含三個工作表
- 每個工作表包含數據表格和疊加K線的圖表

### 程式三：數據分析與繪圖 (program3_analysis_visualization.py)

對程式二的輸出進行深度分析，根據不同分類標準繪製趨勢圖。

#### 功能特點：
- 三種分類方式：股數分類、金額分類、自定義分類
- 15個持股級距的趨勢分析
- 動態Y軸刻度調整
- 雙Y軸顯示

#### 使用方法：

```bash
# 基本分析（只做股數分類）
python program3_analysis_visualization.py 2330_2024-01-01_2024-03-31_analysis.xlsx

# 加入金額分類（需提供股價）
python program3_analysis_visualization.py 2330_2024-01-01_2024-03-31_analysis.xlsx --price 580

# 加入自定義分類
python program3_analysis_visualization.py 2330_2024-01-01_2024-03-31_analysis.xlsx --custom-ranges "0-100,100-500,500-1000,1000+"

# 完整分析（三種分類都執行）
python program3_analysis_visualization.py 2330_2024-01-01_2024-03-31_analysis.xlsx --price 580 --custom-ranges "0-100,100-500,500-1000,1000+"
```

#### 參數說明：
- `input_file`: 程式二輸出的Excel檔案路徑（必填）
- `--price`: 股價，用於金額分類（選填）
- `--custom-ranges`: 自定義範圍，格式如 "0-100,100-500,500+"（選填）

#### 輸出結果：
根據啟用的分類方式，產生對應的Excel檔案：
- 股數分類：`{股號}_analysis_shares_股數分類_*.xlsx`（3個檔案）
- 金額分類：`{股號}_analysis_amount_金額分類_*.xlsx`（3個檔案）
- 自定義分類：`{股號}_analysis_custom_自定義分類_*.xlsx`（3個檔案）

每個分類產生3個檔案，分別對應：
1. 持股人數趨勢
2. 持股股數趨勢  
3. 占集保庫存比例趨勢

## 完整工作流程範例

```bash
# Step 1: 爬取股票數據
python program1_tdcc_scraper.py

# Step 2: 查詢特定股票特定期間的數據
python program2_data_query.py 2330 2024-01-01 2024-03-31

# Step 3: 進行深度分析
python program3_analysis_visualization.py 2330_2024-01-01_2024-03-31_analysis.xlsx --price 580
```

## 分類說明

### 類別一：股數分類
- **散戶**：1-400,000 股
- **中實戶**：400,001-1,000,000 股
- **大戶**：1,000,001 股以上

### 類別二：金額分類（需提供股價）
- **散戶**：< 500萬元
- **小中實戶**：500萬-1,000萬元
- **中實戶**：1,000萬-3,000萬元
- **大戶**：> 3,000萬元

### 類別三：自定義分類
使用者可自行定義持股範圍，例如：
- "0-30,30-100,100-500,500+"
- "0-50,50-200,200-1000,1000+"

## 注意事項

1. **網路連線**：程式一和程式二需要網路連線來爬取數據
2. **執行時間**：首次執行程式一可能需要較長時間（視股票數量而定）
3. **資料更新**：TDCC通常每週五更新數據
4. **Chrome版本**：確保ChromeDriver版本與Chrome瀏覽器版本相符
5. **資料儲存**：確保有足夠的硬碟空間儲存數據（每支股票約需1-5MB）

## 錯誤處理

### 常見問題：

1. **ChromeDriver錯誤**
   - 確認ChromeDriver已安裝且版本正確
   - 確認ChromeDriver在系統PATH中

2. **無法連線到網站**
   - 檢查網路連線
   - 確認目標網站是否正常運作

3. **找不到數據**
   - 確認已執行程式一下載數據
   - 檢查日期範圍是否有可用數據

4. **Excel檔案錯誤**
   - 確認openpyxl套件已正確安裝
   - 關閉正在開啟的Excel檔案

## 資料來源

- 股權分佈數據：[臺灣集中保管結算所 (TDCC)](https://www.tdcc.com.tw/)
- 股票清單：[MoneyDJ理財網](https://www.moneydj.com/)
- K線數據：[聚財網](https://stock.wearn.com/)

## 授權說明

本專案僅供教育與研究用途，使用者需自行承擔使用風險。請遵守相關網站的使用條款。

## 版本歷史

- v1.0 (2025-01-26)：初始版本，包含三個核心程式

## 聯絡資訊

如有問題或建議，請透過GitHub Issues回報。