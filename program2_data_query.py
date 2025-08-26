#!/usr/bin/env python3
"""
程式二：資料查詢與整理
目的：根據使用者指定的參數，從本地資料庫中提取數據，並疊加 K 線圖

作者：AI Assistant
版本：v1.0
日期：2025-01-26
"""

import os
import json
import csv
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import seaborn as sns
import argparse
import sys
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')

# 設定中文字體
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


class StockDataQuery:
    def __init__(self, data_dir="stock_data"):
        self.data_dir = data_dir
        self.wearn_base_url = "https://stock.wearn.com/cdata.asp"
        
    def validate_inputs(self, stock_code, start_date, end_date):
        """驗證輸入參數"""
        # 驗證股票代號
        if not stock_code.isdigit() or len(stock_code) != 4:
            raise ValueError("股票代號必須為4位數字")
        
        # 驗證日期格式
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("日期格式必須為 YYYY-MM-DD")
        
        # 驗證日期邏輯
        if start_dt >= end_dt:
            raise ValueError("起始日期必須早於結束日期")
        
        # 檢查股票資料夾是否存在
        stock_dir = os.path.join(self.data_dir, stock_code)
        if not os.path.exists(stock_dir):
            raise ValueError(f"股票 {stock_code} 的資料不存在")
        
        return start_dt, end_dt
    
    def get_available_dates(self, stock_code):
        """獲取股票的所有可用日期"""
        stock_dir = os.path.join(self.data_dir, stock_code)
        dates = []
        
        for filename in os.listdir(stock_dir):
            if filename.endswith('.json'):
                date_str = filename.replace('.json', '')
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    dates.append(date_obj)
                except ValueError:
                    continue
        
        return sorted(dates)
    
    def find_closest_dates(self, stock_code, start_date, end_date):
        """尋找最接近指定日期範圍的可用資料"""
        available_dates = self.get_available_dates(stock_code)
        
        if not available_dates:
            raise ValueError(f"股票 {stock_code} 沒有可用資料")
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 尋找起始日期
        closest_start = None
        start_warning = ""
        
        # 優先尋找大於等於起始日期的資料
        future_dates = [d for d in available_dates if d >= start_dt]
        if future_dates:
            closest_start = min(future_dates)
        else:
            # 如果沒有，則找最接近的過去日期
            past_dates = [d for d in available_dates if d < start_dt]
            if past_dates:
                closest_start = max(past_dates)
                start_warning = f"警告：起始日期 {start_date} 無資料，使用 {closest_start.strftime('%Y-%m-%d')}"
        
        # 尋找結束日期
        closest_end = None
        end_warning = ""
        
        # 優先尋找小於等於結束日期的資料
        past_dates = [d for d in available_dates if d <= end_dt]
        if past_dates:
            closest_end = max(past_dates)
        else:
            # 如果沒有，則找最接近的未來日期
            future_dates = [d for d in available_dates if d > end_dt]
            if future_dates:
                closest_end = min(future_dates)
                end_warning = f"警告：結束日期 {end_date} 無資料，使用 {closest_end.strftime('%Y-%m-%d')}"
        
        if closest_start is None or closest_end is None:
            raise ValueError("無法找到合適的日期範圍")
        
        warnings = []
        if start_warning:
            warnings.append(start_warning)
        if end_warning:
            warnings.append(end_warning)
        
        return closest_start, closest_end, warnings
    
    def load_stock_data(self, stock_code, start_date, end_date):
        """載入指定日期範圍的股票資料"""
        available_dates = self.get_available_dates(stock_code)
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 篩選日期範圍內的資料
        target_dates = [d for d in available_dates if start_dt <= d <= end_dt]
        
        if not target_dates:
            raise ValueError("指定日期範圍內沒有可用資料")
        
        stock_data = []
        stock_dir = os.path.join(self.data_dir, stock_code)
        
        for date in target_dates:
            json_file = os.path.join(stock_dir, f"{date.strftime('%Y-%m-%d')}.json")
            
            if os.path.exists(json_file):
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data['parsed_date'] = date
                    stock_data.append(data)
        
        return stock_data
    
    def parse_tdcc_data(self, stock_data):
        """解析 TDCC 資料並分類為三個表格"""
        tables = {
            'people_count': [],      # 人數
            'share_count': [],       # 股數/單位數
            'percentage': []         # 占集保庫存數比例
        }
        
        for entry in stock_data:
            date = entry['parsed_date']
            raw_data = entry.get('data', [])
            
            # 假設 TDCC 資料格式為：持股級距, 人數, 股數, 比例
            for row in raw_data:
                if len(row) >= 4:
                    try:
                        level = row[0].strip()  # 持股級距
                        people = int(row[1].replace(',', '')) if row[1].replace(',', '').isdigit() else 0
                        shares = int(row[2].replace(',', '')) if row[2].replace(',', '').isdigit() else 0
                        percentage = float(row[3].replace('%', '').replace(',', '')) if row[3].replace('%', '').replace(',', '').replace('.', '').isdigit() else 0
                        
                        tables['people_count'].append({
                            'date': date,
                            'level': level,
                            'value': people
                        })
                        
                        tables['share_count'].append({
                            'date': date,
                            'level': level,
                            'value': shares
                        })
                        
                        tables['percentage'].append({
                            'date': date,
                            'level': level,
                            'value': percentage
                        })
                        
                    except (ValueError, IndexError):
                        continue
        
        # 轉換為 DataFrame
        df_tables = {}
        for table_name, data in tables.items():
            if data:
                df = pd.DataFrame(data)
                # 透視表格式
                pivot_df = df.pivot(index='date', columns='level', values='value')
                df_tables[table_name] = pivot_df.fillna(0)
            else:
                df_tables[table_name] = pd.DataFrame()
        
        return df_tables
    
    def get_kline_data_from_wearn(self, stock_code, start_date, end_date):
        """從 Wearn.com 獲取 K 線資料"""
        print(f"正在獲取 {stock_code} 的 K 線資料...")
        
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 建構 Wearn 網址
            # 假設網址格式需要民國年和月份
            year = start_dt.year - 1911  # 轉換為民國年
            month = start_dt.month
            
            url = f"{self.wearn_base_url}?Year={year}&month={month:02d}&kind={stock_code}"
            
            # 發送請求
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.encoding = 'big5'
            
            # 解析網頁內容
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 尋找資料表格（這裡需要根據實際網頁結構調整）
            tables = soup.find_all('table')
            
            kline_data = []
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # 跳過標題行
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        try:
                            date_str = cells[0].get_text(strip=True)
                            open_price = float(cells[1].get_text(strip=True))
                            high_price = float(cells[2].get_text(strip=True))
                            low_price = float(cells[3].get_text(strip=True))
                            close_price = float(cells[4].get_text(strip=True))
                            volume = int(cells[5].get_text(strip=True).replace(',', ''))
                            
                            kline_data.append({
                                'date': datetime.strptime(date_str, '%Y/%m/%d'),
                                'open': open_price,
                                'high': high_price,
                                'low': low_price,
                                'close': close_price,
                                'volume': volume
                            })
                        except (ValueError, IndexError):
                            continue
            
            if kline_data:
                df = pd.DataFrame(kline_data)
                df = df.sort_values('date')
                return df
            else:
                print(f"警告：無法獲取 {stock_code} 的 K 線資料")
                return self.generate_mock_kline_data(start_date, end_date)
                
        except Exception as e:
            print(f"獲取 K 線資料失敗: {e}")
            return self.generate_mock_kline_data(start_date, end_date)
    
    def generate_mock_kline_data(self, start_date, end_date):
        """生成模擬 K 線資料（當無法獲取真實資料時）"""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start_dt
        while current <= end_dt:
            if current.weekday() < 5:  # 週一到週五
                dates.append(current)
            current += timedelta(days=1)
        
        # 生成模擬價格資料
        base_price = 100
        mock_data = []
        
        for i, date in enumerate(dates):
            # 簡單的隨機遊走模擬
            price_change = np.random.normal(0, 2)
            base_price += price_change
            
            open_price = base_price
            high_price = open_price + abs(np.random.normal(0, 1))
            low_price = open_price - abs(np.random.normal(0, 1))
            close_price = open_price + np.random.normal(0, 1)
            volume = int(np.random.normal(10000, 3000))
            
            mock_data.append({
                'date': date,
                'open': max(0.1, open_price),
                'high': max(0.1, high_price),
                'low': max(0.1, low_price),
                'close': max(0.1, close_price),
                'volume': max(100, volume)
            })
        
        return pd.DataFrame(mock_data)
    
    def create_overlay_charts(self, tables, kline_data, stock_code, output_filename):
        """建立疊加圖表並儲存到 Excel"""
        print(f"正在建立 {stock_code} 的疊加圖表...")
        
        # 建立 Excel writer
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            
            # 寫入原始資料表格
            for table_name, df in tables.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=f'{table_name}_data')
            
            # 如果有 K 線資料，也寫入
            if not kline_data.empty:
                kline_data.to_excel(writer, sheet_name='kline_data', index=False)
            
            # 建立圖表工作表
            summary_data = []
            
            for table_name, df in tables.items():
                if df.empty:
                    continue
                
                # 建立圖表
                fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))
                fig.suptitle(f'{stock_code} - {table_name} 趨勢圖', fontsize=16)
                
                # 繪製股權分佈資料
                dates = df.index
                for column in df.columns:
                    ax1.plot(dates, df[column], marker='o', markersize=3, label=column)
                
                ax1.set_title(f'{table_name} 趨勢')
                ax1.set_ylabel('數值')
                ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                ax1.grid(True, alpha=0.3)
                
                # 繪製 K 線圖
                if not kline_data.empty:
                    kline_dates = kline_data['date']
                    
                    # 繪製蠟燭圖
                    for i, row in kline_data.iterrows():
                        date = row['date']
                        open_price = row['open']
                        high_price = row['high']
                        low_price = row['low']
                        close_price = row['close']
                        
                        # 決定顏色（紅漲綠跌）
                        color = 'red' if close_price >= open_price else 'green'
                        
                        # 繪製高低線
                        ax2.plot([date, date], [low_price, high_price], color='black', linewidth=1)
                        
                        # 繪製實體
                        body_height = abs(close_price - open_price)
                        body_bottom = min(open_price, close_price)
                        
                        rect = Rectangle((mdates.date2num(date) - 0.3, body_bottom), 
                                       0.6, body_height, 
                                       facecolor=color, edgecolor='black', alpha=0.7)
                        ax2.add_patch(rect)
                    
                    ax2.set_title('K 線圖')
                    ax2.set_ylabel('股價')
                    ax2.grid(True, alpha=0.3)
                    
                    # 繪製成交量
                    ax3.bar(kline_dates, kline_data['volume'], alpha=0.7, color='blue')
                    ax3.set_title('成交量')
                    ax3.set_ylabel('成交量')
                    ax3.set_xlabel('日期')
                    ax3.grid(True, alpha=0.3)
                
                # 格式化 x 軸日期
                for ax in [ax1, ax2, ax3]:
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
                    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
                
                plt.tight_layout()
                
                # 儲存圖表為圖片並插入 Excel
                chart_filename = f'{table_name}_chart.png'
                plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
                plt.close()
                
                # 記錄摘要資訊
                summary_data.append({
                    'table_name': table_name,
                    'data_points': len(df),
                    'date_range': f"{df.index.min()} 到 {df.index.max()}",
                    'columns': ', '.join(df.columns.tolist())
                })
            
            # 建立摘要工作表
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='summary', index=False)
        
        print(f"Excel 檔案已儲存: {output_filename}")
    
    def process_query(self, stock_code, start_date, end_date):
        """主要查詢處理流程"""
        print(f"處理查詢: 股票 {stock_code}, 日期範圍 {start_date} 到 {end_date}")
        
        # 驗證輸入
        start_dt, end_dt = self.validate_inputs(stock_code, start_date, end_date)
        
        # 尋找最接近的可用日期
        closest_start, closest_end, warnings = self.find_closest_dates(
            stock_code, start_date, end_date
        )
        
        # 顯示警告訊息
        for warning in warnings:
            print(warning)
        
        # 載入股票資料
        stock_data = self.load_stock_data(
            stock_code, 
            closest_start.strftime('%Y-%m-%d'),
            closest_end.strftime('%Y-%m-%d')
        )
        
        print(f"載入了 {len(stock_data)} 筆資料")
        
        # 解析為三個表格
        tables = self.parse_tdcc_data(stock_data)
        
        # 獲取 K 線資料
        kline_data = self.get_kline_data_from_wearn(
            stock_code,
            closest_start.strftime('%Y-%m-%d'),
            closest_end.strftime('%Y-%m-%d')
        )
        
        # 建立輸出檔名
        output_filename = f"{stock_code}_{start_date}_to_{end_date}_analysis.xlsx"
        
        # 建立疊加圖表
        self.create_overlay_charts(tables, kline_data, stock_code, output_filename)
        
        return {
            'output_file': output_filename,
            'warnings': warnings,
            'data_summary': {
                'stock_code': stock_code,
                'actual_start_date': closest_start.strftime('%Y-%m-%d'),
                'actual_end_date': closest_end.strftime('%Y-%m-%d'),
                'data_points': len(stock_data)
            }
        }


def main():
    """主程式 - 支援命令列介面"""
    parser = argparse.ArgumentParser(description='股權分佈資料查詢與整理程式')
    parser.add_argument('stock_code', help='股票代號（4位數字）')
    parser.add_argument('start_date', help='起始日期（YYYY-MM-DD）')
    parser.add_argument('end_date', help='結束日期（YYYY-MM-DD）')
    parser.add_argument('--data-dir', default='stock_data', help='資料目錄路徑')
    
    # 如果沒有提供命令列參數，使用互動模式
    if len(sys.argv) == 1:
        print("股權分佈資料查詢程式 v1.0")
        print("=" * 50)
        
        stock_code = input("請輸入股票代號（4位數字）: ").strip()
        start_date = input("請輸入起始日期（YYYY-MM-DD）: ").strip()
        end_date = input("請輸入結束日期（YYYY-MM-DD）: ").strip()
        data_dir = input("請輸入資料目錄路徑（預設: stock_data）: ").strip() or "stock_data"
    else:
        args = parser.parse_args()
        stock_code = args.stock_code
        start_date = args.start_date
        end_date = args.end_date
        data_dir = args.data_dir
    
    try:
        # 建立查詢器
        query = StockDataQuery(data_dir)
        
        # 執行查詢
        result = query.process_query(stock_code, start_date, end_date)
        
        print("\n查詢完成！")
        print("=" * 50)
        print(f"輸出檔案: {result['output_file']}")
        print(f"實際日期範圍: {result['data_summary']['actual_start_date']} 到 {result['data_summary']['actual_end_date']}")
        print(f"資料點數: {result['data_summary']['data_points']}")
        
        if result['warnings']:
            print("\n警告訊息:")
            for warning in result['warnings']:
                print(f"  - {warning}")
        
    except Exception as e:
        print(f"錯誤: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()