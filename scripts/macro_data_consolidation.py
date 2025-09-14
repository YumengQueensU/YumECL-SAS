#!/usr/bin/env python3
"""
加拿大宏观经济数据整理脚本
用于IFRS 9 ECL模型和压力测试
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import os

class MacroDataProcessor:
    """宏观经济数据处理器"""
    
    def __init__(self, data_path=None):
        if data_path is None:
            # 获取当前脚本所在目录的上级目录，然后构建绝对路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            self.data_path = os.path.join(parent_dir, 'data', 'sample', 'macro_data') + os.sep
        else:
            self.data_path = data_path
        self.start_date = '2020-01-01'
        self.end_date = '2024-12-31'
        
    def load_cpi_data(self):
        """加载CPI数据"""
        print("Loading CPI data...")
        df = pd.read_csv(f'{self.data_path}CPI_Monthly-1810000401-eng.csv', 
                        skiprows=9, encoding='utf-8')
        
        # 提取All-items CPI
        cpi_data = df[df['Products and product groups 3 4'] == 'All-items'].iloc[:, 1:].T
        cpi_data.columns = ['CPI']
        cpi_data.index = pd.to_datetime(cpi_data.index.str.strip(), format='%B %Y')
        cpi_data = cpi_data.loc[self.start_date:self.end_date]
        
        # 计算通胀率（年同比）
        cpi_data['CPI'] = pd.to_numeric(cpi_data['CPI'], errors='coerce')
        cpi_data['Inflation_YoY'] = cpi_data['CPI'].pct_change(12) * 100
        
        return cpi_data
    
    def load_labour_force_data(self):
        """加载劳动力市场数据"""
        print("Loading labour force data...")
        df = pd.read_csv(f'{self.data_path}Labour_Force-1410028701-eng.csv', 
                        skiprows=12, encoding='utf-8')
        
        # 提取失业率和就业率
        labour_data = pd.DataFrame()
        
        # 找到失业率行
        unemp_row = df[df['Labour force characteristics'] == 'Unemployment rate 16'].iloc[:, 6:]
        emp_row = df[df['Labour force characteristics'] == 'Employment rate 18'].iloc[:, 6:]
        
        # 转换为时间序列
        dates = pd.to_datetime([col.replace(' ', '-01-') for col in unemp_row.columns])
        
        labour_data['Unemployment_Rate'] = unemp_row.values.flatten()
        labour_data['Employment_Rate'] = emp_row.values.flatten()
        labour_data.index = dates
        
        labour_data = labour_data.loc[self.start_date:self.end_date]
        labour_data = labour_data.apply(pd.to_numeric, errors='coerce')
        
        return labour_data
    
    def load_gdp_data(self, method='forward_fill'):
        """
        加载GDP数据（年度转月度）
        
        Parameters:
        method (str): 插值方法
            - 'forward_fill': 前向填充（推荐）
            - 'linear': 线性插值
            - 'cubic': 三次样条插值
            - 'constant': 常数填充（年度值保持不变）
        """
        print("Loading GDP data...")
        df = pd.read_csv(f'{self.data_path}GDP-3610040201-eng.csv', 
                        skiprows=10, encoding='utf-8')
        
        # 提取加拿大总GDP（安大略省也可选）
        gdp_data = df[df['Geography'] == 'Ontario'][['2020', '2021', '2022', '2023', '2024']].T
        gdp_data.columns = ['GDP_Ontario']
        gdp_data.index = pd.to_datetime(gdp_data.index.astype(str) + '-01-01')
        
        # 将GDP数据转换为数值类型（移除逗号并转换为float）
        gdp_data['GDP_Ontario'] = gdp_data['GDP_Ontario'].astype(str).str.replace(',', '').astype(float)
        print("原始年度GDP数据:")
        print(gdp_data)
        
        # 创建完整的月度时间范围
        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)
        monthly_index = pd.date_range(start=start_date, end=end_date, freq='M')
        
        # 根据选择的方法处理年度数据
        if method == 'forward_fill':
            # 方法1：前向填充（推荐）
            # 将年度数据重新索引到月度，然后前向填充
            gdp_monthly = gdp_data.reindex(monthly_index, method='ffill')
            print("使用前向填充方法")
            
        elif method == 'linear':
            # 方法2：线性插值
            # 使用numpy的线性插值
            gdp_values = gdp_data['GDP_Ontario'].values
            gdp_dates = gdp_data.index.astype(np.int64) // 10**9  # 转换为秒
            monthly_dates = monthly_index.astype(np.int64) // 10**9
            
            # 使用numpy插值
            interpolated_values = np.interp(monthly_dates, gdp_dates, gdp_values)
            gdp_monthly = pd.DataFrame({'GDP_Ontario': interpolated_values}, index=monthly_index)
            print("使用线性插值方法")
            
        elif method == 'cubic':
            # 方法3：三次样条插值
            from scipy import interpolate
            
            gdp_values = gdp_data['GDP_Ontario'].values
            gdp_dates = gdp_data.index.astype(np.int64) // 10**9  # 转换为秒
            monthly_dates = monthly_index.astype(np.int64) // 10**9
            
            # 使用scipy三次样条插值
            f = interpolate.interp1d(gdp_dates, gdp_values, kind='cubic', 
                                   bounds_error=False, fill_value='extrapolate')
            interpolated_values = f(monthly_dates)
            gdp_monthly = pd.DataFrame({'GDP_Ontario': interpolated_values}, index=monthly_index)
            print("使用三次样条插值方法")
            
        elif method == 'constant':
            # 方法4：常数填充（年度值保持不变）
            gdp_monthly = gdp_data.reindex(monthly_index, method='ffill')
            print("使用常数填充方法")
            
        else:
            raise ValueError(f"未知的插值方法: {method}")
        
        # 计算GDP增长率（年同比）
        gdp_monthly['GDP_Growth_YoY'] = gdp_monthly['GDP_Ontario'].pct_change(12) * 100
        
        print("处理后的月度GDP数据（前10行）:")
        print(gdp_monthly.head(10))
        print("处理后的月度GDP数据（后10行）:")
        print(gdp_monthly.tail(10))
        
        return gdp_monthly
    
    def load_interest_rates(self):
        """加载利率数据"""
        print("Loading interest rate data...")
        
        # 政策利率（日度）
        policy_df = pd.read_csv(f'{self.data_path}Policy_Interest_Rate-V39079-sd-2020-01-01-ed-2024-12-31.csv', 
                               skiprows=8)
        policy_df['date'] = pd.to_datetime(policy_df['date'])
        policy_df = policy_df.set_index('date')
        policy_monthly = policy_df.resample('M').mean()
        policy_monthly.columns = ['Policy_Rate']
        
        # 基准利率（周度）
        prime_df = pd.read_csv(f'{self.data_path}Prime_Rate-V80691311-sd-2020-01-01-ed-2024-12-31.csv', 
                              skiprows=8)
        prime_df['date'] = pd.to_datetime(prime_df['date'])
        prime_df = prime_df.set_index('date')
        prime_monthly = prime_df.resample('M').mean()
        prime_monthly.columns = ['Prime_Rate']
        
        # 5年期抵押贷款利率（周度）
        mortgage_df = pd.read_csv(f'{self.data_path}5Year_Conventional_Mortgage-V80691335-sd-2020-01-01-ed-2024-12-31.csv', 
                                 skiprows=8)
        mortgage_df['date'] = pd.to_datetime(mortgage_df['date'])
        mortgage_df = mortgage_df.set_index('date')
        mortgage_monthly = mortgage_df.resample('M').mean()
        mortgage_monthly.columns = ['Mortgage_5Y_Rate']
        
        # 合并利率数据
        rates_data = pd.concat([policy_monthly, prime_monthly, mortgage_monthly], axis=1)
        
        # 计算利差
        rates_data['Prime_Policy_Spread'] = rates_data['Prime_Rate'] - rates_data['Policy_Rate']
        rates_data['Mortgage_Prime_Spread'] = rates_data['Mortgage_5Y_Rate'] - rates_data['Prime_Rate']
        
        return rates_data.loc[self.start_date:self.end_date]
    
    def load_fx_data(self):
        """加载汇率数据"""
        print("Loading FX data...")
        df = pd.read_csv(f'{self.data_path}FX_USD_CAD-sd-2020-01-01-ed-2024-12-31.csv', 
                        skiprows=8)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        
        # 日度转月度（取月均值）
        fx_monthly = df.resample('M').mean()
        fx_monthly.columns = ['USD_CAD']
        
        # 计算汇率变化率
        fx_monthly['FX_Change_MoM'] = fx_monthly['USD_CAD'].pct_change() * 100
        fx_monthly['FX_Change_YoY'] = fx_monthly['USD_CAD'].pct_change(12) * 100
        
        return fx_monthly.loc[self.start_date:self.end_date]
    
    def load_housing_data(self):
        """加载房价指数数据"""
        print("Loading housing price index data...")
        df = pd.read_csv(f'{self.data_path}MLS_HPI_data_August_2025.csv', 
                        skiprows=0)
        
        # 处理日期和价格
        df['Date'] = pd.to_datetime(df['Date'])
        df['HPI'] = df['Aggregate Composite MLS® HPI*'].str.replace('$', '').str.replace(',', '').astype(float)
        df = df[['Date', 'HPI']].set_index('Date')
        
        # 计算房价增长率
        df['HPI_Change_MoM'] = df['HPI'].pct_change() * 100
        df['HPI_Change_YoY'] = df['HPI'].pct_change(12) * 100
        
        return df.loc[self.start_date:self.end_date]
    
    def load_oil_prices(self):
        """加载油价数据"""
        print("Loading oil price data...")
        df = pd.read_csv(f'{self.data_path}WCS_Oil_Prices_Alberta_1757748101538.csv')
        
        # 处理日期
        df['Date'] = pd.to_datetime(df['Date'])
        
        # 分离WTI和WCS价格
        wti_df = df[df['Type'] == 'WTI'][['Date', 'Value']].set_index('Date')
        wcs_df = df[df['Type'] == 'WCS'][['Date', 'Value']].set_index('Date')
        
        oil_data = pd.DataFrame()
        oil_data['WTI_Price'] = wti_df['Value']
        oil_data['WCS_Price'] = wcs_df['Value']
        
        # 计算价差和变化率
        oil_data['WCS_WTI_Spread'] = oil_data['WCS_Price'] - oil_data['WTI_Price']
        oil_data['WTI_Change_YoY'] = oil_data['WTI_Price'].pct_change(12) * 100
        
        return oil_data.loc[self.start_date:self.end_date]
    
    def create_stress_scenarios(self, macro_data):
        """创建压力测试场景"""
        print("Creating stress testing scenarios...")
        
        # 基准场景：使用最近12个月的平均值
        baseline = macro_data.tail(12).mean()
        
        # 不利场景（Adverse）
        adverse = baseline.copy()
        adverse['Unemployment_Rate'] = baseline['Unemployment_Rate'] + 3.0  # 失业率上升3%
        adverse['GDP_Growth_YoY'] = baseline['GDP_Growth_YoY'] - 3.0  # GDP增长下降3%
        adverse['HPI_Change_YoY'] = -10.0  # 房价下跌10%
        adverse['Policy_Rate'] = baseline['Policy_Rate'] + 2.0  # 利率上升200bp
        
        # 严重不利场景（Severely Adverse）
        severe = baseline.copy()
        severe['Unemployment_Rate'] = baseline['Unemployment_Rate'] + 5.0  # 失业率上升5%
        severe['GDP_Growth_YoY'] = -5.0  # GDP负增长5%
        severe['HPI_Change_YoY'] = -20.0  # 房价下跌20%
        severe['Policy_Rate'] = baseline['Policy_Rate'] + 3.0  # 利率上升300bp
        severe['WTI_Price'] = 40.0  # 油价跌至$40
        
        scenarios = pd.DataFrame({
            'Baseline': baseline,
            'Adverse': adverse,
            'Severely_Adverse': severe
        }).T
        
        return scenarios
    
    def consolidate_all_data(self):
        """整合所有数据"""
        print("\n" + "="*50)
        print("Starting macro data consolidation...")
        print("="*50 + "\n")
        
        # 加载各数据集
        cpi_data = self.load_cpi_data()
        labour_data = self.load_labour_force_data()
        gdp_data = self.load_gdp_data()
        rates_data = self.load_interest_rates()
        fx_data = self.load_fx_data()
        housing_data = self.load_housing_data()
        oil_data = self.load_oil_prices()
        
        # 合并所有数据
        macro_data = pd.concat([
            cpi_data,
            labour_data,
            gdp_data,
            rates_data,
            fx_data,
            housing_data,
            oil_data
        ], axis=1)
        
        # 处理缺失值（前向填充）
        macro_data = macro_data.fillna(method='ffill')
        
        # 添加额外的风险指标
        self._add_risk_indicators(macro_data)
        
        # 创建压力测试场景
        scenarios = self.create_stress_scenarios(macro_data)
        
        print("\n" + "="*50)
        print("Data consolidation completed!")
        print(f"Final dataset shape: {macro_data.shape}")
        print(f"Date range: {macro_data.index[0]} to {macro_data.index[-1]}")
        print("="*50 + "\n")
        
        return macro_data, scenarios
    
    def _add_risk_indicators(self, df):
        """添加风险指标"""
        # 经济周期指标（基于失业率和GDP增长）
        df['Economic_Cycle_Score'] = (
            (df['Unemployment_Rate'] - df['Unemployment_Rate'].mean()) / df['Unemployment_Rate'].std() * (-1) +
            (df['GDP_Growth_YoY'] - df['GDP_Growth_YoY'].mean()) / df['GDP_Growth_YoY'].std()
        ) / 2
        
        # 信贷条件指标
        df['Credit_Conditions'] = (
            df['Prime_Rate'] * 0.4 + 
            df['Mortgage_5Y_Rate'] * 0.4 + 
            df['Prime_Policy_Spread'] * 0.2
        )
        
        # 房地产风险指标
        df['Housing_Risk_Score'] = (
            df['HPI_Change_YoY'] * (-0.5) +  # 房价下跌增加风险
            df['Mortgage_5Y_Rate'] * 0.3 +   # 高利率增加风险
            df['Unemployment_Rate'] * 0.2    # 高失业率增加风险
        )
        
        return df
    
    def save_data(self, macro_data, scenarios, output_path=None):
        if output_path is None:
            # 获取当前脚本所在目录的上级目录，然后构建绝对路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            output_path = os.path.join(parent_dir, 'data', 'sample') + os.sep
        """保存处理后的数据"""
        print("Saving processed data...")
        
        # 保存主数据集
        macro_data.to_csv(f'{output_path}macro_data_consolidated.csv')
        print(f"Main dataset saved to: {output_path}macro_data_consolidated.csv")
        
        # 保存压力测试场景
        scenarios.to_csv(f'{output_path}stress_test_scenarios.csv')
        print(f"Stress scenarios saved to: {output_path}stress_test_scenarios.csv")
        
        # 保存数据字典
        self._save_data_dictionary(macro_data, output_path)
        
        # 生成数据质量报告
        self._generate_data_quality_report(macro_data, output_path)
    
    def _save_data_dictionary(self, df, output_path):
        """保存数据字典"""
        data_dict = pd.DataFrame({
            'Variable': df.columns,
            'Type': df.dtypes.astype(str),
            'Non_Null_Count': df.notna().sum().values,
            'Null_Count': df.isna().sum().values,
            'Min': df.min().values,
            'Mean': df.mean().values,
            'Max': df.max().values,
            'Std': df.std().values
        })
        
        data_dict.to_csv(f'{output_path}macro_data_dictionary.csv', index=False)
        print(f"Data dictionary saved to: {output_path}macro_data_dictionary.csv")
    
    def _generate_data_quality_report(self, df, output_path):
        """生成数据质量报告"""
        report = []
        report.append("="*60)
        report.append("MACRO DATA QUALITY REPORT")
        report.append("="*60)
        report.append(f"\nGenerated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"\nDataset Overview:")
        report.append(f"- Total records: {len(df)}")
        report.append(f"- Total variables: {len(df.columns)}")
        report.append(f"- Date range: {df.index[0]} to {df.index[-1]}")
        
        report.append(f"\nMissing Data Summary:")
        missing = df.isna().sum()
        for col in missing[missing > 0].index:
            report.append(f"- {col}: {missing[col]} missing values ({missing[col]/len(df)*100:.2f}%)")
        
        report.append(f"\nKey Statistics:")
        key_vars = ['Unemployment_Rate', 'Inflation_YoY', 'GDP_Growth_YoY', 
                   'Policy_Rate', 'HPI_Change_YoY']
        for var in key_vars:
            if var in df.columns:
                report.append(f"\n{var}:")
                report.append(f"  Mean: {df[var].mean():.2f}")
                report.append(f"  Std: {df[var].std():.2f}")
                report.append(f"  Min: {df[var].min():.2f}")
                report.append(f"  Max: {df[var].max():.2f}")
        
        # 保存报告
        with open(f'{output_path}data_quality_report.txt', 'w') as f:
            f.write('\n'.join(report))
        
        print(f"Data quality report saved to: {output_path}data_quality_report.txt")

def main():
    """主函数"""
    # 初始化处理器
    processor = MacroDataProcessor()
    
    # 整合数据
    macro_data, scenarios = processor.consolidate_all_data()
    
    # 保存结果
    processor.save_data(macro_data, scenarios)
    
    # 显示结果摘要
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    print("\nFinal Dataset Columns:")
    for i, col in enumerate(macro_data.columns, 1):
        print(f"{i:2d}. {col}")
    
    print(f"\nStress Test Scenarios:")
    print(scenarios[['Unemployment_Rate', 'GDP_Growth_YoY', 'HPI_Change_YoY', 'Policy_Rate']])
    
    return macro_data, scenarios

if __name__ == "__main__":
    macro_data, scenarios = main()
