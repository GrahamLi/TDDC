#!/usr/bin/env python3
"""
程式一：股權分佈資料爬蟲
目的：建立台股所有個股的股權分佈歷史資料庫

作者：AI Assistant
版本：v1.0
日期：2025-01-26
"""

import requests
import json
import csv
import os
import time
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd


class TDCCDataScraper:
    def __init__(self):
        self.tdcc_url = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
        self.moneydj_url = "https://moneydj.emega.com.tw/js/StockTable.htm"
        self.data_dir = "stock_data"
        self.stock_codes = []
        self.excluded_keywords = ["ETF", "美債", "REITs", "期信基金", "指數股票型"]
        
        # 建立資料目錄
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def setup_driver(self):
        """設定 Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 無頭模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e:
            print(f"Chrome WebDriver 初始化失敗: {e}")
            print("請確保已安裝 Chrome 瀏覽器和 ChromeDriver")
            return None
    
    def get_stock_codes_from_moneydj(self):
        """從 MoneyDJ 網站獲取台股股號列表"""
        print("正在從 MoneyDJ 獲取股號列表...")
        
        try:
            response = requests.get(self.moneydj_url, timeout=30)
            response.encoding = 'big5'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 找到股票表格
            tables = soup.find_all('table')
            stock_codes = []
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        # 第一列通常是股號，第二列是股票名稱
                        code_cell = cells[0].get_text(strip=True)
                        name_cell = cells[1].get_text(strip=True)
                        
                        # 檢查是否為有效股號（4位數字）
                        if re.match(r'^\d{4}$', code_cell):
                            # 排除 ETF 等非個股
                            if not any(keyword in name_cell for keyword in self.excluded_keywords):
                                stock_codes.append(code_cell)
            
            # 去重並排序
            stock_codes = sorted(list(set(stock_codes)))
            print(f"成功獲取 {len(stock_codes)} 個股票代碼")
            return stock_codes
            
        except Exception as e:
            print(f"獲取股號列表失敗: {e}")
            # 備用股號列表
            backup_codes = ["2330", "2317", "3008", "2454", "2382", "2412", "2891"]
            print(f"使用備用股號列表: {backup_codes}")
            return backup_codes
    
    def get_existing_dates(self, stock_code):
        """獲取已存在的資料日期"""
        stock_dir = os.path.join(self.data_dir, stock_code)
        if not os.path.exists(stock_dir):
            return set()
        
        existing_dates = set()
        for filename in os.listdir(stock_dir):
            if filename.endswith('.json') or filename.endswith('.csv'):
                date_str = filename.split('.')[0]
                try:
                    # 驗證日期格式
                    datetime.strptime(date_str, '%Y-%m-%d')
                    existing_dates.add(date_str)
                except ValueError:
                    continue
        
        return existing_dates
    
    def scrape_tdcc_data(self, stock_code, target_date):
        """從 TDCC 網站抓取特定股號和日期的資料"""
        driver = self.setup_driver()
        if not driver:
            return None
        
        try:
            print(f"正在抓取 {stock_code} 於 {target_date} 的資料...")
            
            # 訪問 TDCC 網站
            driver.get(self.tdcc_url)
            time.sleep(3)
            
            # 輸入股票代號
            stock_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "StockNo"))
            )
            stock_input.clear()
            stock_input.send_keys(stock_code)
            
            # 選擇查詢日期
            date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            
            # 選擇年份
            year_select = Select(driver.find_element(By.NAME, "qryYear"))
            year_select.select_by_value(str(date_obj.year - 1911))  # 民國年
            
            # 選擇月份
            month_select = Select(driver.find_element(By.NAME, "qryMonth"))
            month_select.select_by_value(f"{date_obj.month:02d}")
            
            # 選擇日期
            day_select = Select(driver.find_element(By.NAME, "qryDate"))
            day_select.select_by_value(f"{date_obj.day:02d}")
            
            # 提交查詢
            submit_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
            submit_button.click()
            
            # 等待結果載入
            time.sleep(5)
            
            # 抓取表格資料
            try:
                table = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                
                # 解析表格
                rows = table.find_elements(By.TAG_NAME, "tr")
                data = []
                
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 3:  # 確保有足夠的欄位
                        row_data = [cell.text.strip() for cell in cells]
                        data.append(row_data)
                
                if data:
                    return {
                        'stock_code': stock_code,
                        'date': target_date,
                        'data': data,
                        'scraped_at': datetime.now().isoformat()
                    }
                else:
                    print(f"  警告: {stock_code} 於 {target_date} 無資料")
                    return None
                    
            except Exception as e:
                print(f"  解析表格失敗: {e}")
                return None
                
        except Exception as e:
            print(f"  抓取失敗: {e}")
            return None
        finally:
            driver.quit()
    
    def save_data(self, stock_code, date, data):
        """儲存資料到檔案"""
        stock_dir = os.path.join(self.data_dir, stock_code)
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir)
        
        # 儲存為 JSON 格式
        json_filename = os.path.join(stock_dir, f"{date}.json")
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # 儲存為 CSV 格式
        csv_filename = os.path.join(stock_dir, f"{date}.csv")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if 'data' in data and data['data']:
                writer.writerows(data['data'])
        
        print(f"  已儲存: {json_filename}")
    
    def get_trading_dates(self, start_date, end_date):
        """獲取交易日列表（排除週末）"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        trading_dates = []
        current = start
        
        while current <= end:
            # 排除週末（週六=5, 週日=6）
            if current.weekday() < 5:
                trading_dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return trading_dates
    
    def run_initial_scrape(self):
        """首次執行：抓取過去一年的資料"""
        print("開始首次資料抓取...")
        
        # 獲取股號列表
        self.stock_codes = self.get_stock_codes_from_moneydj()
        
        # 設定日期範圍（過去一年）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # 獲取交易日列表
        trading_dates = self.get_trading_dates(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        total_tasks = len(self.stock_codes) * len(trading_dates)
        completed = 0
        
        print(f"預計處理 {len(self.stock_codes)} 個股號，共 {total_tasks} 個任務")
        
        for stock_code in self.stock_codes:
            print(f"處理股票 {stock_code}...")
            
            # 檢查已存在的資料
            existing_dates = self.get_existing_dates(stock_code)
            
            for date in trading_dates:
                if date in existing_dates:
                    print(f"  跳過已存在的資料: {date}")
                    completed += 1
                    continue
                
                # 抓取資料
                data = self.scrape_tdcc_data(stock_code, date)
                if data:
                    self.save_data(stock_code, date, data)
                
                completed += 1
                progress = (completed / total_tasks) * 100
                print(f"  進度: {progress:.1f}% ({completed}/{total_tasks})")
                
                # 避免過於頻繁的請求
                time.sleep(2)
    
    def run_update_scrape(self):
        """後續執行：僅抓取新資料"""
        print("開始更新資料抓取...")
        
        # 獲取現有股號列表
        existing_stocks = [d for d in os.listdir(self.data_dir) 
                          if os.path.isdir(os.path.join(self.data_dir, d))]
        
        if not existing_stocks:
            print("未找到現有資料，執行首次抓取...")
            self.run_initial_scrape()
            return
        
        # 設定日期範圍（最近一週）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        trading_dates = self.get_trading_dates(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        for stock_code in existing_stocks:
            print(f"更新股票 {stock_code}...")
            
            existing_dates = self.get_existing_dates(stock_code)
            
            for date in trading_dates:
                if date not in existing_dates:
                    data = self.scrape_tdcc_data(stock_code, date)
                    if data:
                        self.save_data(stock_code, date, data)
                    time.sleep(2)


def main():
    """主程式"""
    scraper = TDCCDataScraper()
    
    print("股權分佈資料爬蟲程式 v1.0")
    print("=" * 50)
    
    # 檢查是否為首次執行
    if os.path.exists(scraper.data_dir) and os.listdir(scraper.data_dir):
        print("檢測到現有資料，執行更新模式...")
        scraper.run_update_scrape()
    else:
        print("首次執行，建立完整資料庫...")
        scraper.run_initial_scrape()
    
    print("資料抓取完成！")


if __name__ == "__main__":
    main()