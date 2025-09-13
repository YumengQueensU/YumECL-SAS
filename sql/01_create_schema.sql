-- 1. 贷款主表（模拟加拿大零售贷款组合）
CREATE TABLE loan_portfolio (
    loan_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    origination_date DATE,
    maturity_date DATE,
    original_amount DECIMAL(15,2),
    outstanding_balance DECIMAL(15,2),
    interest_rate DECIMAL(5,4),
    product_type VARCHAR(50), -- 'Mortgage', 'HELOC', 'Auto', 'Credit Card'
    province VARCHAR(2), -- ON, BC, QC, AB等
    fsa VARCHAR(3), -- Forward Sortation Area (邮编前3位)
    employment_status VARCHAR(20),
    annual_income DECIMAL(12,2),
    credit_score INT, -- Equifax或TransUnion评分
    loan_to_value DECIMAL(5,4),
    payment_frequency VARCHAR(20),
    snapshot_date DATE
);

-- 2. 支付历史表（用于行为评分）
CREATE TABLE payment_history (
    loan_id VARCHAR(20),
    payment_date DATE,
    scheduled_amount DECIMAL(12,2),
    actual_amount DECIMAL(12,2),
    days_past_due INT,
    delinquency_status VARCHAR(10), -- Current, 30DPD, 60DPD, 90DPD
    snapshot_date DATE,
    PRIMARY KEY (loan_id, payment_date)
);

-- 3. 宏观经济变量表（加拿大特定指标）
CREATE TABLE macro_economic_factors (
    scenario_id VARCHAR(20),
    forecast_date DATE,
    gdp_growth DECIMAL(5,4),
    unemployment_rate DECIMAL(5,4),
    boc_policy_rate DECIMAL(5,4), -- Bank of Canada政策利率
    housing_price_index DECIMAL(10,2),
    cpi_inflation DECIMAL(5,4),
    oil_price_wcs DECIMAL(10,2), -- Western Canadian Select油价
    province VARCHAR(2),
    PRIMARY KEY (scenario_id, forecast_date, province)
);

-- 4. IFRS 9阶段迁移表
CREATE TABLE stage_migration (
    loan_id VARCHAR(20),
    assessment_date DATE,
    previous_stage INT,
    current_stage INT, -- 1: Stage 1, 2: Stage 2, 3: Stage 3
    sicr_flag BOOLEAN, -- Significant Increase in Credit Risk
    sicr_trigger VARCHAR(100),
    pd_12m DECIMAL(6,5),
    pd_lifetime DECIMAL(6,5),
    lgd DECIMAL(6,5),
    ead DECIMAL(15,2),
    ecl_12m DECIMAL(12,2),
    ecl_lifetime DECIMAL(12,2),
    PRIMARY KEY (loan_id, assessment_date)
);