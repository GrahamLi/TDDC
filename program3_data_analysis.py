#!/usr/bin/env python3
"""
程式三：數據分析與繪圖
目的：對程式二的輸出進行深度分析，並根據多種自定義標準繪製趨勢圖

作者：AI Assistant
版本：v1.0
日期：2025-01-26
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import seaborn as sns
import argparse
import sys
from openpyxl import load_workbook
import warnings
warnings.filterwarnings('ignore')

# 設定中文字體
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 設定圖表樣式
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (15, 10)


class StockDataAnalyzer:
    def __init__(self):
        # 定義持股級距分類
        self.classification_types = {
            'stock_based': {
                'name': '一般定義（股數）',
                'ranges': [
                    ('散戶', 1, 400000),
                    ('中實戶', 400001, 1000000),
                    ('大戶', 1000001, float('inf'))
                ]
            },
            'value_based': {
                'name': '金額定義',
                'ranges': [
                    ('散戶', 0, 5000000),
                    ('小中實戶', 5000001, 10000000),
                    ('中實戶', 10000001, 30000000),
                    ('大戶', 30000001, float('inf'))
                ]
            },
            'custom': {
                'name': '自定義級距',
                'ranges': []  # 由使用者輸入
            }
        }
        
        # TDCC 標準持股級距對應
        self.tdcc_ranges = {
            '1-999': (1, 999),
            '1,000-5,000': (1000, 5000),
            '5,001-10,000': (5001, 10000),
            '10,001-15,000': (10001, 15000),
            '15,001-20,000': (15001, 20000),
            '20,001-30,000': (20001, 30000),
            '30,001-40,000': (30001, 40000),
            '40,001-50,000': (40001, 50000),
            '50,001-100,000': (50001, 100000),
            '100,001-200,000': (100001, 200000),
            '200,001-400,000': (200001, 400000),
            '400,001-600,000': (400001, 600000),
            '600,001-800,000': (600001, 800000),
            '800,001-1,000,000': (800001, 1000000),
            '1,000,001以上': (1000001, float('inf'))
        }
    
    def load_excel_data(self, excel_file):
        """載入程式二產生的 Excel 檔案"""
        print(f"正在載入 Excel 檔案: {excel_file}")
        
        if not os.path.exists(excel_file):
            raise FileNotFoundError(f"找不到檔案: {excel_file}")
        
        try:
            # 讀取各個工作表
            excel_data = {}
            
            with pd.ExcelFile(excel_file) as xls:
                sheet_names = xls.sheet_names
                print(f"發現工作表: {sheet_names}")
                
                for sheet_name in sheet_names:
                    if 'data' in sheet_name.lower():
                        df = pd.read_excel(xls, sheet_name=sheet_name, index_col=0)
                        # 確保索引為日期格式
                        if not isinstance(df.index, pd.DatetimeIndex):
                            df.index = pd.to_datetime(df.index)
                        excel_data[sheet_name] = df
            
            print(f"成功載入 {len(excel_data)} 個資料表")
            return excel_data
            
        except Exception as e:
            raise Exception(f"載入 Excel 檔案失敗: {e}")
    
    def classify_data_by_stock_ranges(self, data, classification_type='stock_based'):
        """根據持股級距分類資料"""
        ranges = self.classification_types[classification_type]['ranges']
        classified_data = {}
        
        for category_name, min_shares, max_shares in ranges:
            category_data = []
            
            for tdcc_range, (range_min, range_max) in self.tdcc_ranges.items():
                # 檢查 TDCC 級距是否落在分類範圍內
                if (range_min >= min_shares and range_min <= max_shares) or \
                   (range_max >= min_shares and range_max <= max_shares) or \
                   (range_min <= min_shares and range_max >= max_shares):
                    
                    # 如果該級距存在於資料中，則加入
                    if tdcc_range in data.columns:
                        if not category_data:
                            category_data = data[tdcc_range].copy()
                        else:
                            category_data = category_data + data[tdcc_range]
            
            if len(category_data) > 0:
                classified_data[category_name] = category_data
        
        return pd.DataFrame(classified_data)
    
    def classify_data_by_value_ranges(self, data, stock_price, classification_type='value_based'):
        """根據金額級距分類資料（需要股價）"""
        ranges = self.classification_types[classification_type]['ranges']
        classified_data = {}
        
        for category_name, min_value, max_value in ranges:
            category_data = []
            
            for tdcc_range, (range_min, range_max) in self.tdcc_ranges.items():
                # 計算該級距的金額範圍
                value_min = range_min * stock_price
                value_max = range_max * stock_price if range_max != float('inf') else float('inf')
                
                # 檢查金額級距是否落在分類範圍內
                if (value_min >= min_value and value_min <= max_value) or \
                   (value_max >= min_value and value_max <= max_value) or \
                   (value_min <= min_value and value_max >= max_value):
                    
                    if tdcc_range in data.columns:
                        if not category_data:
                            category_data = data[tdcc_range].copy()
                        else:
                            category_data = category_data + data[tdcc_range]
            
            if len(category_data) > 0:
                classified_data[category_name] = category_data
        
        return pd.DataFrame(classified_data)
    
    def classify_data_by_custom_ranges(self, data, custom_ranges):
        """根據自定義級距分類資料"""
        classified_data = {}
        
        for category_name, min_shares, max_shares in custom_ranges:
            category_data = []
            
            for tdcc_range, (range_min, range_max) in self.tdcc_ranges.items():
                # 檢查 TDCC 級距是否落在自定義範圍內
                if (range_min >= min_shares and range_min <= max_shares) or \
                   (range_max >= min_shares and range_max <= max_shares) or \
                   (range_min <= min_shares and range_max >= max_shares):
                    
                    if tdcc_range in data.columns:
                        if not category_data:
                            category_data = data[tdcc_range].copy()
                        else:
                            category_data = category_data + data[tdcc_range]
            
            if len(category_data) > 0:
                classified_data[category_name] = category_data
        
        return pd.DataFrame(classified_data)
    
    def detect_micro_changes(self, series, window=3, threshold=0.01):
        """檢測微小變化並調整刻度"""
        if len(series) < window:
            return False
        
        # 計算滾動變化
        rolling_change = series.rolling(window=window).std()
        total_range = series.max() - series.min()
        
        # 如果變化小於總範圍的1%，則認為是微小變化
        return (rolling_change.mean() / total_range) < threshold
    
    def create_trend_chart(self, data, title, ylabel, filename, stock_code):
        """建立趨勢圖表"""
        fig, ax = plt.subplots(figsize=(15, 10))
        
        # 顏色調色盤
        colors = plt.cm.Set3(np.linspace(0, 1, len(data.columns)))
        
        # 繪製每個類別的趨勢線
        for i, column in enumerate(data.columns):
            series = data[column]
            
            # 檢查是否需要調整刻度
            if self.detect_micro_changes(series):
                # 使用雙 Y 軸處理微小變化
                ax2 = ax.twinx()
                ax2.plot(series.index, series.values, 
                        marker='o', markersize=4, linewidth=2, 
                        label=f"{column} (右軸)", color=colors[i], alpha=0.8)
                ax2.set_ylabel(f"{ylabel} (放大)", fontsize=12)
                ax2.legend(loc='upper right')
            else:
                ax.plot(series.index, series.values, 
                       marker='o', markersize=4, linewidth=2, 
                       label=column, color=colors[i], alpha=0.8)
        
        # 設定圖表格式
        ax.set_title(f'{stock_code} - {title}', fontsize=16, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.legend(loc='upper left', frameon=True, shadow=True)
        ax.grid(True, alpha=0.3)
        
        # 格式化 x 軸日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # 調整佈局
        plt.tight_layout()
        
        # 儲存圖表
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"圖表已儲存: {filename}")
    
    def create_detailed_trend_chart(self, original_data, title, ylabel, filename, stock_code):
        """建立詳細的15級距趨勢圖"""
        fig, ax = plt.subplots(figsize=(18, 12))
        
        # 選取前15個級距或所有可用級距
        columns_to_plot = original_data.columns[:15] if len(original_data.columns) >= 15 else original_data.columns
        
        # 顏色調色盤
        colors = plt.cm.tab20(np.linspace(0, 1, len(columns_to_plot)))
        
        # 繪製每個級距的趨勢線
        for i, column in enumerate(columns_to_plot):
            series = original_data[column]
            ax.plot(series.index, series.values, 
                   marker='o', markersize=3, linewidth=1.5, 
                   label=column, color=colors[i], alpha=0.7)
        
        # 設定圖表格式
        ax.set_title(f'{stock_code} - {title} (詳細級距)', fontsize=16, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        
        # 圖例設定（分兩欄顯示）
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=1, frameon=True, shadow=True)
        ax.grid(True, alpha=0.3)
        
        # 格式化 x 軸日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # 調整佈局
        plt.tight_layout()
        
        # 儲存圖表
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"詳細圖表已儲存: {filename}")
    
    def save_to_excel(self, data_dict, filename):
        """將分析結果儲存到 Excel"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, data in data_dict.items():
                if isinstance(data, pd.DataFrame) and not data.empty:
                    data.to_excel(writer, sheet_name=sheet_name)
                    
        print(f"Excel 檔案已儲存: {filename}")
    
    def analyze_data(self, excel_file, stock_price=None, custom_ranges=None, output_prefix=None):
        """主要分析流程"""
        # 載入資料
        excel_data = self.load_excel_data(excel_file)
        
        # 確定輸出前綴
        if output_prefix is None:
            base_name = os.path.splitext(os.path.basename(excel_file))[0]
            output_prefix = base_name.split('_')[0]  # 取股票代號
        
        stock_code = output_prefix
        
        # 分析結果字典
        analysis_results = {}
        
        # 處理每種資料類型
        data_types = {
            'people_count': '人數',
            'share_count': '股數/單位數', 
            'percentage': '占集保庫存數比例(%)'
        }
        
        for data_key, data_name in data_types.items():
            sheet_name = f"{data_key}_data"
            
            if sheet_name not in excel_data:
                print(f"警告: 找不到 {sheet_name} 工作表")
                continue
            
            original_data = excel_data[sheet_name]
            print(f"分析 {data_name} 資料，共 {len(original_data.columns)} 個級距")
            
            # 1. 一般定義（股數）分類
            stock_classified = self.classify_data_by_stock_ranges(original_data, 'stock_based')
            if not stock_classified.empty:
                chart_filename = f"{output_prefix}_{data_key}_stock_based.png"
                self.create_trend_chart(
                    stock_classified, 
                    f"{data_name} - 一般定義",
                    data_name,
                    chart_filename,
                    stock_code
                )
                analysis_results[f"{data_key}_stock_based"] = stock_classified
            
            # 2. 金額定義分類（如果有提供股價）
            if stock_price:
                value_classified = self.classify_data_by_value_ranges(original_data, stock_price, 'value_based')
                if not value_classified.empty:
                    chart_filename = f"{output_prefix}_{data_key}_value_based.png"
                    self.create_trend_chart(
                        value_classified,
                        f"{data_name} - 金額定義",
                        data_name,
                        chart_filename,
                        stock_code
                    )
                    analysis_results[f"{data_key}_value_based"] = value_classified
            
            # 3. 自定義分類（如果有提供）
            if custom_ranges:
                custom_classified = self.classify_data_by_custom_ranges(original_data, custom_ranges)
                if not custom_classified.empty:
                    chart_filename = f"{output_prefix}_{data_key}_custom.png"
                    self.create_trend_chart(
                        custom_classified,
                        f"{data_name} - 自定義級距",
                        data_name,
                        chart_filename,
                        stock_code
                    )
                    analysis_results[f"{data_key}_custom"] = custom_classified
            
            # 4. 詳細15級距圖表
            detailed_chart_filename = f"{output_prefix}_{data_key}_detailed.png"
            self.create_detailed_trend_chart(
                original_data,
                f"{data_name}",
                data_name,
                detailed_chart_filename,
                stock_code
            )
            
            # 將原始資料也加入結果
            analysis_results[f"{data_key}_original"] = original_data
        
        # 儲存三個主要 Excel 檔案
        for data_key, data_name in data_types.items():
            excel_filename = f"{output_prefix}_{data_key}_analysis.xlsx"
            
            # 收集該資料類型的所有分析結果
            relevant_data = {}
            for key, data in analysis_results.items():
                if data_key in key:
                    relevant_data[key] = data
            
            if relevant_data:
                self.save_to_excel(relevant_data, excel_filename)
        
        return analysis_results
    
    def get_custom_ranges_from_user(self):
        """從使用者獲取自定義級距"""
        print("\n請輸入自定義級距（格式：名稱,最小股數,最大股數）")
        print("範例：小散戶,1,10000")
        print("輸入 'done' 結束輸入")
        
        custom_ranges = []
        while True:
            user_input = input("級距定義: ").strip()
            
            if user_input.lower() == 'done':
                break
            
            try:
                parts = user_input.split(',')
                if len(parts) != 3:
                    print("格式錯誤，請重新輸入")
                    continue
                
                name = parts[0].strip()
                min_shares = int(parts[1].strip())
                max_shares = int(parts[2].strip()) if parts[2].strip().lower() != 'inf' else float('inf')
                
                custom_ranges.append((name, min_shares, max_shares))
                print(f"已添加級距: {name} ({min_shares} - {max_shares})")
                
            except ValueError:
                print("數值格式錯誤，請重新輸入")
        
        return custom_ranges


def main():
    """主程式"""
    parser = argparse.ArgumentParser(description='股權分佈數據分析與繪圖程式')
    parser.add_argument('excel_file', help='程式二產生的 Excel 檔案路徑')
    parser.add_argument('--stock-price', type=float, help='股價（用於金額定義分類）')
    parser.add_argument('--output-prefix', help='輸出檔案前綴')
    
    # 如果沒有提供命令列參數，使用互動模式
    if len(sys.argv) == 1:
        print("股權分佈數據分析與繪圖程式 v1.0")
        print("=" * 50)
        
        excel_file = input("請輸入 Excel 檔案路徑: ").strip()
        
        stock_price_input = input("請輸入股價（用於金額定義，按 Enter 跳過）: ").strip()
        stock_price = float(stock_price_input) if stock_price_input else None
        
        output_prefix = input("請輸入輸出檔案前綴（按 Enter 使用預設）: ").strip() or None
        
        use_custom = input("是否使用自定義級距？(y/n): ").strip().lower()
        custom_ranges = None
        
    else:
        args = parser.parse_args()
        excel_file = args.excel_file
        stock_price = args.stock_price
        output_prefix = args.output_prefix
        
        use_custom = input("是否使用自定義級距？(y/n): ").strip().lower()
        custom_ranges = None
    
    # 處理自定義級距
    if use_custom == 'y':
        analyzer = StockDataAnalyzer()
        custom_ranges = analyzer.get_custom_ranges_from_user()
    
    try:
        # 建立分析器
        analyzer = StockDataAnalyzer()
        
        # 執行分析
        results = analyzer.analyze_data(
            excel_file=excel_file,
            stock_price=stock_price,
            custom_ranges=custom_ranges,
            output_prefix=output_prefix
        )
        
        print("\n分析完成！")
        print("=" * 50)
        print(f"處理的資料類型: {len([k for k in results.keys() if 'original' in k])}")
        print(f"生成的圖表數量: {len([k for k in results.keys() if 'original' not in k])}")
        
        if stock_price:
            print(f"使用股價: ${stock_price}")
        
        if custom_ranges:
            print(f"自定義級距: {len(custom_ranges)} 個")
        
        print("\n輸出檔案:")
        # 列出生成的檔案
        output_files = []
        if output_prefix:
            base_prefix = output_prefix
        else:
            base_prefix = os.path.splitext(os.path.basename(excel_file))[0].split('_')[0]
        
        for data_type in ['people_count', 'share_count', 'percentage']:
            excel_file = f"{base_prefix}_{data_type}_analysis.xlsx"
            if os.path.exists(excel_file):
                output_files.append(excel_file)
        
        for file in output_files:
            print(f"  - {file}")
        
    except Exception as e:
        print(f"錯誤: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()