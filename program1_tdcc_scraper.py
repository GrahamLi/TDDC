#!/usr/bin/env python3
"""
程式一：股權分佈資料爬蟲
目的：建立台股所有個股的股權分佈歷史資料庫
從TDCC網站抓取歷史股權分佈數據，建立本地資料庫
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
from bs4 import BeautifulSoup
import re

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tdcc_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TDCCScraper:
    """TDCC股權分佈資料爬蟲"""
    
    def __init__(self, data_dir: str = "stock_data"):
        """
        初始化爬蟲
        
        Args:
            data_dir: 資料儲存目錄
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.tdcc_url = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
        self.moneydj_url = "https://moneydj.emega.com.tw/js/StockTable.htm"
        self.driver = None
        self.exclude_keywords = ['ETF', '美債', '債券', '期貨', '權證', '認購', '認售', 'REITs']
        
    def init_driver(self):
        """初始化Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # 無頭模式
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("WebDriver初始化成功")
        except Exception as e:
            logger.error(f"WebDriver初始化失敗: {e}")
            raise
            
    def close_driver(self):
        """關閉WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver已關閉")
            
    def get_stock_list(self) -> List[Dict[str, str]]:
        """
        從MoneyDJ獲取台股股票清單
        
        Returns:
            股票清單，包含股號和股票名稱
        """
        try:
            logger.info("開始獲取股票清單...")
            response = requests.get(self.moneydj_url, timeout=30)
            response.encoding = 'big5'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            stock_list = []
            
            # 解析股票表格
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        # 提取股號和股票名稱
                        stock_text = cols[0].text.strip()
                        match = re.match(r'(\d+)\s*(.*)', stock_text)
                        if match:
                            stock_code = match.group(1)
                            stock_name = match.group(2) if match.group(2) else ""
                            
                            # 排除ETF和其他非個股
                            if not any(keyword in stock_name for keyword in self.exclude_keywords):
                                stock_list.append({
                                    'code': stock_code,
                                    'name': stock_name
                                })
                                
            # 如果MoneyDJ無法訪問，使用備用股票清單
            if not stock_list:
                logger.warning("無法從MoneyDJ獲取股票清單，使用預設清單")
                stock_list = self.get_default_stock_list()
                
            logger.info(f"獲取到 {len(stock_list)} 支股票")
            return stock_list
            
        except Exception as e:
            logger.error(f"獲取股票清單失敗: {e}")
            return self.get_default_stock_list()
            
    def get_default_stock_list(self) -> List[Dict[str, str]]:
        """
        獲取預設的股票清單（作為備用）
        
        Returns:
            預設股票清單
        """
        return [
            {'code': '2330', 'name': '台積電'},
            {'code': '2317', 'name': '鴻海'},
            {'code': '2454', 'name': '聯發科'},
            {'code': '2308', 'name': '台達電'},
            {'code': '2303', 'name': '聯電'},
            {'code': '2002', 'name': '中鋼'},
            {'code': '2412', 'name': '中華電'},
            {'code': '2882', 'name': '國泰金'},
            {'code': '2881', 'name': '富邦金'},
            {'code': '1301', 'name': '台塑'},
        ]
        
    def get_available_dates(self, stock_code: str) -> List[str]:
        """
        獲取股票可查詢的日期清單
        
        Args:
            stock_code: 股票代碼
            
        Returns:
            可查詢的日期清單
        """
        try:
            self.driver.get(self.tdcc_url)
            time.sleep(2)
            
            # 輸入股票代碼
            stock_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "StockNo"))
            )
            stock_input.clear()
            stock_input.send_keys(stock_code)
            
            # 等待日期下拉選單載入
            time.sleep(1)
            date_select = Select(self.driver.find_element(By.ID, "scaDate"))
            
            # 獲取所有可選日期
            dates = []
            for option in date_select.options:
                date_value = option.get_attribute('value')
                if date_value:
                    dates.append(date_value)
                    
            logger.info(f"股票 {stock_code} 有 {len(dates)} 個可查詢日期")
            return dates
            
        except Exception as e:
            logger.error(f"獲取股票 {stock_code} 可查詢日期失敗: {e}")
            return []
            
    def scrape_stock_data(self, stock_code: str, date: str) -> Optional[Dict]:
        """
        抓取特定股票在特定日期的股權分佈數據
        
        Args:
            stock_code: 股票代碼
            date: 日期 (YYYYMMDD格式)
            
        Returns:
            股權分佈數據
        """
        try:
            self.driver.get(self.tdcc_url)
            time.sleep(2)
            
            # 輸入股票代碼
            stock_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "StockNo"))
            )
            stock_input.clear()
            stock_input.send_keys(stock_code)
            
            # 選擇日期
            time.sleep(1)
            date_select = Select(self.driver.find_element(By.ID, "scaDate"))
            date_select.select_by_value(date)
            
            # 點擊查詢按鈕
            query_button = self.driver.find_element(By.ID, "btnQuery")
            query_button.click()
            
            # 等待結果載入
            time.sleep(3)
            
            # 解析結果表格
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "table"))
            )
            
            # 提取表格數據
            rows = table.find_elements(By.TAG_NAME, "tr")
            data = {
                'stock_code': stock_code,
                'date': date,
                'distribution': []
            }
            
            for row in rows[1:]:  # 跳過標題行
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 4:
                    distribution_item = {
                        'level': cols[0].text.strip(),  # 持股分級
                        'holders': cols[1].text.strip().replace(',', ''),  # 人數
                        'shares': cols[2].text.strip().replace(',', ''),  # 股數
                        'percentage': cols[3].text.strip()  # 占比
                    }
                    data['distribution'].append(distribution_item)
                    
            # 獲取其他統計資訊
            summary_info = self.driver.find_elements(By.CLASS_NAME, "summary-item")
            for item in summary_info:
                label = item.find_element(By.CLASS_NAME, "label").text.strip()
                value = item.find_element(By.CLASS_NAME, "value").text.strip()
                data[label] = value
                
            return data
            
        except Exception as e:
            logger.error(f"抓取股票 {stock_code} 日期 {date} 數據失敗: {e}")
            return None
            
    def save_data(self, stock_code: str, date: str, data: Dict):
        """
        儲存股權分佈數據
        
        Args:
            stock_code: 股票代碼
            date: 日期
            data: 股權分佈數據
        """
        # 建立股票資料夾
        stock_dir = self.data_dir / stock_code
        stock_dir.mkdir(exist_ok=True)
        
        # 格式化日期 (YYYYMMDD -> YYYY-MM-DD)
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        
        # 儲存為JSON檔案
        file_path = stock_dir / f"{formatted_date}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"已儲存 {stock_code} 在 {formatted_date} 的數據")
        
    def check_existing_dates(self, stock_code: str) -> Set[str]:
        """
        檢查本地已存在的數據日期
        
        Args:
            stock_code: 股票代碼
            
        Returns:
            已存在的日期集合
        """
        stock_dir = self.data_dir / stock_code
        if not stock_dir.exists():
            return set()
            
        existing_dates = set()
        for file_path in stock_dir.glob("*.json"):
            # 從檔名提取日期 (YYYY-MM-DD.json -> YYYYMMDD)
            date_str = file_path.stem
            date_parts = date_str.split('-')
            if len(date_parts) == 3:
                existing_dates.add(''.join(date_parts))
                
        return existing_dates
        
    def run(self, limit: Optional[int] = None):
        """
        執行爬蟲主程序
        
        Args:
            limit: 限制爬取的股票數量（用於測試）
        """
        try:
            # 初始化WebDriver
            self.init_driver()
            
            # 獲取股票清單
            stock_list = self.get_stock_list()
            
            if limit:
                stock_list = stock_list[:limit]
                
            total_stocks = len(stock_list)
            logger.info(f"開始爬取 {total_stocks} 支股票的數據")
            
            for idx, stock_info in enumerate(stock_list, 1):
                stock_code = stock_info['code']
                stock_name = stock_info['name']
                
                logger.info(f"[{idx}/{total_stocks}] 處理股票: {stock_code} {stock_name}")
                
                # 檢查已存在的數據
                existing_dates = self.check_existing_dates(stock_code)
                
                # 獲取可查詢的日期
                available_dates = self.get_available_dates(stock_code)
                
                if not available_dates:
                    logger.warning(f"股票 {stock_code} 無可查詢日期，跳過")
                    continue
                    
                # 過濾出需要下載的日期
                dates_to_download = [d for d in available_dates if d not in existing_dates]
                
                if not dates_to_download:
                    logger.info(f"股票 {stock_code} 所有數據已是最新，跳過")
                    continue
                    
                logger.info(f"股票 {stock_code} 需要下載 {len(dates_to_download)} 個日期的數據")
                
                # 下載數據
                for date in dates_to_download:
                    data = self.scrape_stock_data(stock_code, date)
                    if data:
                        self.save_data(stock_code, date, data)
                        time.sleep(1)  # 避免請求過快
                        
                # 每處理完一支股票休息一下
                time.sleep(2)
                
        except KeyboardInterrupt:
            logger.info("使用者中斷執行")
        except Exception as e:
            logger.error(f"執行失敗: {e}")
        finally:
            self.close_driver()
            
def main():
    """主程序"""
    scraper = TDCCScraper(data_dir="stock_data")
    
    # 可以設定limit參數來限制爬取數量（測試用）
    # scraper.run(limit=5)  # 只爬取前5支股票
    
    # 正式執行（爬取所有股票）
    scraper.run()
    
if __name__ == "__main__":
    main()