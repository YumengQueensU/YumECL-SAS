import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Get the current date to append to the file name
current_date = datetime.now().strftime("%Y%m%d")

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 从当前目录向上移动一级，进入上一级目录（例如：project_a/）
parent_dir = os.path.join(current_dir, '..')

# 构建到目标子文件夹的路径（例如：project_a/data/sample/）
target_folder = os.path.join(parent_dir, 'data', 'sample')
    
def generate_canadian_loan_data(n_loans=10000):
    """生成模拟的加拿大贷款数据"""
    
    np.random.seed(42)
    
    # 贷款主数据
    loans = pd.DataFrame({
        'loan_id': [f'L{str(i).zfill(8)}' for i in range(n_loans)],
        'customer_id': [f'C{str(i).zfill(8)}' for i in range(n_loans)],
        'origination_date': pd.date_range(end='2024-01-01', periods=n_loans, freq='H'),
        'product_type': np.random.choice(['Mortgage', 'HELOC', 'Auto', 'Credit Card'], n_loans, p=[0.4, 0.2, 0.3, 0.1]),
        'province': np.random.choice(['ON', 'BC', 'QC', 'AB', 'MB', 'SK'], n_loans, p=[0.38, 0.13, 0.23, 0.11, 0.08, 0.07]),
        'original_amount': np.random.lognormal(11, 1.5, n_loans),
        'interest_rate': np.random.uniform(0.02, 0.08, n_loans),
        'credit_score': np.random.normal(700, 80, n_loans).clip(300, 900).astype(int),
        'annual_income': np.random.lognormal(10.8, 0.6, n_loans),
        'loan_to_value': np.random.beta(5, 2, n_loans)
    })
    
    # 调整mortgage的金额
    mortgage_mask = loans['product_type'] == 'Mortgage'
    loans.loc[mortgage_mask, 'original_amount'] = np.random.lognormal(12.5, 0.8, mortgage_mask.sum())
    
    # 生成违约标记（与信用评分相关）
    default_prob = 1 / (1 + np.exp((loans['credit_score'] - 650) / 50))
    loans['default_flag'] = np.random.binomial(1, default_prob)
    
    return loans

# 生成支付历史数据
def generate_payment_history(loans_df):
    """生成支付历史数据"""
    payment_records = []
    
    for _, loan in loans_df.iterrows():
        n_payments = np.random.randint(12, 36)
        payment_dates = pd.date_range(
            start=loan['origination_date'], 
            periods=n_payments, 
            freq='M'
        )
        
        for payment_date in payment_dates:
            dpd = 0
            if loan['default_flag'] == 1 and np.random.random() < 0.3:
                dpd = np.random.choice([0, 30, 60, 90], p=[0.4, 0.3, 0.2, 0.1])
            
            payment_records.append({
                'loan_id': loan['loan_id'],
                'payment_date': payment_date,
                'scheduled_amount': loan['original_amount'] / 360,  # 简化计算
                'days_past_due': dpd
            })
            
    payment_history = pd.DataFrame(payment_records)
    
    return payment_history

# 主程序执行
if __name__ == "__main__":
    # 生成贷款数据
    loans = generate_canadian_loan_data()
    
    # 保存贷款数据
    loans_file_name = os.path.join(target_folder, f"loans_{current_date}.csv")
    loans.to_csv(loans_file_name, index=False)
    print(f"贷款数据已保存为 {loans_file_name}")
    
    # 生成支付历史数据
    payment_history = generate_payment_history(loans)
    
    # 保存支付历史数据
    payment_history_name = os.path.join(target_folder, f"payment_history_{current_date}.csv")
    payment_history.to_csv(payment_history_name, index=False)
    print(f"支付历史数据已保存为 {payment_history_name}")