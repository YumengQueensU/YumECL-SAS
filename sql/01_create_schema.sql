/**********************************************************************
 * Script: 01_create_schema.sql
 * Purpose: Create database schema for IFRS 9 ECL Model
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This script creates all necessary tables, views, and indexes
 * for the Canadian retail credit ECL model
 **********************************************************************/

-- =====================================================================
-- PART 1: DATABASE AND SCHEMA SETUP
-- =====================================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS ecl_model;
USE ecl_model;

-- =====================================================================
-- PART 2: BASE TABLES - RAW DATA STORAGE
-- =====================================================================

-- 2.1 Loan Master Table
DROP TABLE IF EXISTS loans;
CREATE TABLE loans (
    loan_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    origination_date DATETIME NOT NULL,
    product_type VARCHAR(50) NOT NULL,
    province CHAR(2) NOT NULL,
    original_amount DECIMAL(18,2) NOT NULL,
    interest_rate DECIMAL(10,6) NOT NULL,
    credit_score INT,
    annual_income DECIMAL(18,2),
    loan_to_value DECIMAL(10,6),
    default_flag INT DEFAULT 0,
    
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes for performance
    INDEX idx_customer (customer_id),
    INDEX idx_product (product_type),
    INDEX idx_origination (origination_date),
    INDEX idx_province (province),
    INDEX idx_default (default_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.2 Payment History Table
DROP TABLE IF EXISTS payment_history;
CREATE TABLE payment_history (
    payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    loan_id VARCHAR(20) NOT NULL,
    payment_date DATETIME NOT NULL,
    scheduled_amount DECIMAL(18,2) NOT NULL,
    actual_amount DECIMAL(18,2),
    days_past_due INT DEFAULT 0,
    
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key and indexes
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
    INDEX idx_loan_payment (loan_id, payment_date),
    INDEX idx_dpd (days_past_due),
    INDEX idx_payment_date (payment_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.3 Macro Economic Data Table
DROP TABLE IF EXISTS macro_data;
CREATE TABLE macro_data (
    data_date DATE PRIMARY KEY,
    
    -- Price indices
    cpi DECIMAL(10,4),
    inflation_yoy DECIMAL(10,4),
    
    -- Labor market
    unemployment_rate DECIMAL(10,4),
    employment_rate DECIMAL(10,4),
    
    -- GDP
    gdp_ontario DECIMAL(18,2),
    gdp_growth_yoy DECIMAL(10,4),
    
    -- Interest rates
    policy_rate DECIMAL(10,6),
    prime_rate DECIMAL(10,6),
    mortgage_5y_rate DECIMAL(10,6),
    prime_policy_spread DECIMAL(10,6),
    mortgage_prime_spread DECIMAL(10,6),
    
    -- Exchange rates
    usd_cad DECIMAL(10,6),
    fx_change_mom DECIMAL(10,4),
    fx_change_yoy DECIMAL(10,4),
    
    -- Housing
    hpi DECIMAL(18,2),
    hpi_change_mom DECIMAL(10,4),
    hpi_change_yoy DECIMAL(10,4),
    
    -- Oil prices
    wti_price DECIMAL(10,2),
    wcs_price DECIMAL(10,2),
    wcs_wti_spread DECIMAL(10,2),
    wti_change_yoy DECIMAL(10,4),
    
    -- Risk indicators
    economic_cycle_score DECIMAL(10,6),
    credit_conditions DECIMAL(10,6),
    housing_risk_score DECIMAL(10,6),
    
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_year_month (data_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.4 Stress Test Scenarios Table
DROP TABLE IF EXISTS stress_scenarios;
CREATE TABLE stress_scenarios (
    scenario_name VARCHAR(50) PRIMARY KEY,
    
    -- All macro variables for scenarios
    cpi DECIMAL(10,4),
    inflation_yoy DECIMAL(10,4),
    unemployment_rate DECIMAL(10,4),
    employment_rate DECIMAL(10,4),
    gdp_ontario DECIMAL(18,2),
    gdp_growth_yoy DECIMAL(10,4),
    policy_rate DECIMAL(10,6),
    prime_rate DECIMAL(10,6),
    mortgage_5y_rate DECIMAL(10,6),
    prime_policy_spread DECIMAL(10,6),
    mortgage_prime_spread DECIMAL(10,6),
    usd_cad DECIMAL(10,6),
    fx_change_mom DECIMAL(10,4),
    fx_change_yoy DECIMAL(10,4),
    hpi DECIMAL(18,2),
    hpi_change_mom DECIMAL(10,4),
    hpi_change_yoy DECIMAL(10,4),
    wti_price DECIMAL(10,2),
    wcs_price DECIMAL(10,2),
    wcs_wti_spread DECIMAL(10,2),
    wti_change_yoy DECIMAL(10,4),
    economic_cycle_score DECIMAL(10,6),
    credit_conditions DECIMAL(10,6),
    housing_risk_score DECIMAL(10,6),
    
    -- Metadata
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- PART 3: DERIVED TABLES - MODEL FEATURES AND RESULTS
-- =====================================================================

-- 3.1 Loan Features Table (for modeling)
DROP TABLE IF EXISTS loan_features;
CREATE TABLE loan_features (
    loan_id VARCHAR(20) PRIMARY KEY,
    
    -- Original loan characteristics
    origination_date DATE,
    product_type VARCHAR(50),
    province CHAR(2),
    original_amount DECIMAL(18,2),
    current_balance DECIMAL(18,2),
    interest_rate DECIMAL(10,6),
    credit_score INT,
    annual_income DECIMAL(18,2),
    loan_to_value DECIMAL(10,6),
    
    -- Behavioral features
    months_on_book INT,
    max_dpd_3m INT,
    max_dpd_6m INT,
    max_dpd_12m INT,
    current_dpd INT,
    num_payments_30dpd INT,
    num_payments_60dpd INT,
    num_payments_90dpd INT,
    
    -- Macro features at origination
    unemployment_rate_orig DECIMAL(10,4),
    gdp_growth_orig DECIMAL(10,4),
    policy_rate_orig DECIMAL(10,6),
    hpi_change_orig DECIMAL(10,4),
    
    -- Current macro features
    unemployment_rate_current DECIMAL(10,4),
    gdp_growth_current DECIMAL(10,4),
    policy_rate_current DECIMAL(10,6),
    hpi_change_current DECIMAL(10,4),
    
    -- Model segments
    risk_segment VARCHAR(20),
    stage_ifrs9 INT DEFAULT 1,
    
    -- Audit
    feature_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
    INDEX idx_stage (stage_ifrs9),
    INDEX idx_segment (risk_segment),
    INDEX idx_feature_date (feature_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.2 PD Model Results Table
DROP TABLE IF EXISTS pd_results;
CREATE TABLE pd_results (
    loan_id VARCHAR(20) NOT NULL,
    scenario_name VARCHAR(50) NOT NULL,
    
    -- PD estimates by term
    pd_12m DECIMAL(10,8),
    pd_lifetime DECIMAL(10,8),
    pd_marginal_y1 DECIMAL(10,8),
    pd_marginal_y2 DECIMAL(10,8),
    pd_marginal_y3 DECIMAL(10,8),
    pd_marginal_y4 DECIMAL(10,8),
    pd_marginal_y5 DECIMAL(10,8),
    
    -- Model metadata
    model_version VARCHAR(20),
    calculation_date DATE,
    
    -- Audit
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key and indexes
    PRIMARY KEY (loan_id, scenario_name),
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
    FOREIGN KEY (scenario_name) REFERENCES stress_scenarios(scenario_name),
    INDEX idx_calc_date (calculation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.3 LGD Model Results Table
DROP TABLE IF EXISTS lgd_results;
CREATE TABLE lgd_results (
    loan_id VARCHAR(20) NOT NULL,
    scenario_name VARCHAR(50) NOT NULL,
    
    -- LGD estimates
    lgd_downturn DECIMAL(10,8),
    lgd_expected DECIMAL(10,8),
    lgd_best_estimate DECIMAL(10,8),
    
    -- Components
    cure_rate DECIMAL(10,8),
    recovery_rate DECIMAL(10,8),
    collateral_value DECIMAL(18,2),
    
    -- Model metadata
    model_version VARCHAR(20),
    calculation_date DATE,
    
    -- Audit
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key and indexes
    PRIMARY KEY (loan_id, scenario_name),
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
    FOREIGN KEY (scenario_name) REFERENCES stress_scenarios(scenario_name),
    INDEX idx_calc_date (calculation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.4 ECL Results Table
DROP TABLE IF EXISTS ecl_results;
CREATE TABLE ecl_results (
    loan_id VARCHAR(20) NOT NULL,
    scenario_name VARCHAR(50) NOT NULL,
    
    -- ECL components
    ead DECIMAL(18,2),
    pd_12m DECIMAL(10,8),
    pd_lifetime DECIMAL(10,8),
    lgd DECIMAL(10,8),
    
    -- ECL amounts
    ecl_12m DECIMAL(18,2),
    ecl_lifetime DECIMAL(18,2),
    ecl_final DECIMAL(18,2),
    
    -- IFRS 9 staging
    stage_ifrs9 INT,
    stage_transition VARCHAR(20),
    
    -- Scenario weights (for probability-weighted ECL)
    scenario_weight DECIMAL(10,4),
    
    -- Model metadata
    calculation_date DATE,
    model_run_id VARCHAR(50),
    
    -- Audit
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key and indexes
    PRIMARY KEY (loan_id, scenario_name, calculation_date),
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id),
    FOREIGN KEY (scenario_name) REFERENCES stress_scenarios(scenario_name),
    INDEX idx_calc_date (calculation_date),
    INDEX idx_stage (stage_ifrs9),
    INDEX idx_run_id (model_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- PART 4: MONITORING AND VALIDATION TABLES
-- =====================================================================

-- 4.1 Model Performance Monitoring
DROP TABLE IF EXISTS model_monitoring;
CREATE TABLE model_monitoring (
    monitoring_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    model_type VARCHAR(20) NOT NULL, -- 'PD', 'LGD', 'ECL'
    monitoring_date DATE NOT NULL,
    
    -- Population Stability Index (PSI)
    psi_score DECIMAL(10,6),
    psi_status VARCHAR(20),
    
    -- Model accuracy metrics
    auc_score DECIMAL(10,6),
    gini_coefficient DECIMAL(10,6),
    ks_statistic DECIMAL(10,6),
    
    -- Backtesting results
    actual_default_rate DECIMAL(10,6),
    predicted_default_rate DECIMAL(10,6),
    variance_ratio DECIMAL(10,6),
    
    -- Sample sizes
    total_accounts INT,
    defaulted_accounts INT,
    
    -- Metadata
    model_version VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_model_date (model_type, monitoring_date),
    INDEX idx_monitoring_date (monitoring_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4.2 Data Quality Metrics Table
DROP TABLE IF EXISTS data_quality_metrics;
CREATE TABLE data_quality_metrics (
    check_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_date DATE NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    
    -- Quality metrics
    total_records INT,
    null_count INT,
    duplicate_count INT,
    outlier_count INT,
    
    -- Specific checks
    check_type VARCHAR(50),
    check_result VARCHAR(20),
    check_details TEXT,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_check_date (check_date),
    INDEX idx_table (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================================
-- PART 5: VIEWS FOR REPORTING
-- =====================================================================

-- 5.1 Current Portfolio View
CREATE OR REPLACE VIEW v_portfolio_current AS
SELECT 
    l.loan_id,
    l.customer_id,
    l.product_type,
    l.province,
    l.original_amount,
    l.interest_rate,
    l.credit_score,
    l.default_flag,
    lf.current_balance,
    lf.months_on_book,
    lf.current_dpd,
    lf.stage_ifrs9,
    lf.risk_segment,
    e.ecl_12m,
    e.ecl_lifetime,
    e.ecl_final
FROM loans l
LEFT JOIN loan_features lf ON l.loan_id = lf.loan_id
LEFT JOIN ecl_results e ON l.loan_id = e.loan_id 
    AND e.scenario_name = 'Baseline'
    AND e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results);

-- 5.2 ECL Summary by Product View
CREATE OR REPLACE VIEW v_ecl_summary_product AS
SELECT 
    l.product_type,
    COUNT(DISTINCT l.loan_id) as loan_count,
    SUM(lf.current_balance) as total_exposure,
    AVG(p.pd_12m) as avg_pd_12m,
    AVG(lg.lgd_expected) as avg_lgd,
    SUM(e.ecl_12m) as total_ecl_12m,
    SUM(e.ecl_lifetime) as total_ecl_lifetime,
    SUM(e.ecl_12m) / NULLIF(SUM(lf.current_balance), 0) as ecl_coverage_12m,
    SUM(e.ecl_lifetime) / NULLIF(SUM(lf.current_balance), 0) as ecl_coverage_lifetime
FROM loans l
LEFT JOIN loan_features lf ON l.loan_id = lf.loan_id
LEFT JOIN pd_results p ON l.loan_id = p.loan_id AND p.scenario_name = 'Baseline'
LEFT JOIN lgd_results lg ON l.loan_id = lg.loan_id AND lg.scenario_name = 'Baseline'
LEFT JOIN ecl_results e ON l.loan_id = e.loan_id 
    AND e.scenario_name = 'Baseline'
    AND e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY l.product_type;

-- 5.3 IFRS 9 Stage Migration View
CREATE OR REPLACE VIEW v_stage_migration AS
SELECT 
    prev.stage_ifrs9 as stage_from,
    curr.stage_ifrs9 as stage_to,
    COUNT(*) as loan_count,
    SUM(curr.current_balance) as total_balance
FROM loan_features curr
LEFT JOIN loan_features prev ON curr.loan_id = prev.loan_id
    AND prev.feature_date = DATE_SUB(curr.feature_date, INTERVAL 1 MONTH)
WHERE curr.feature_date = (SELECT MAX(feature_date) FROM loan_features)
GROUP BY prev.stage_ifrs9, curr.stage_ifrs9;

-- 5.4 Stress Test Results Comparison View
CREATE OR REPLACE VIEW v_stress_test_comparison AS
SELECT 
    scenario_name,
    COUNT(DISTINCT loan_id) as loan_count,
    SUM(ead) as total_ead,
    AVG(pd_12m) as avg_pd_12m,
    AVG(lgd) as avg_lgd,
    SUM(ecl_12m) as total_ecl_12m,
    SUM(ecl_lifetime) as total_ecl_lifetime,
    SUM(ecl_12m) / NULLIF(SUM(ead), 0) as ecl_rate_12m,
    SUM(ecl_lifetime) / NULLIF(SUM(ead), 0) as ecl_rate_lifetime
FROM ecl_results
WHERE calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY scenario_name;

-- =====================================================================
-- PART 6: STORED PROCEDURES FOR DATA MANAGEMENT
-- =====================================================================

-- 6.1 Procedure to refresh loan features
DELIMITER //
CREATE PROCEDURE sp_refresh_loan_features(IN p_feature_date DATE)
BEGIN
    -- This procedure will be implemented in feature_engineering.sql
    -- Placeholder for now
    SELECT 'Loan features refresh procedure - to be implemented' AS message;
END //
DELIMITER ;

-- 6.2 Procedure to calculate ECL
DELIMITER //
CREATE PROCEDURE sp_calculate_ecl(
    IN p_scenario_name VARCHAR(50),
    IN p_calculation_date DATE
)
BEGIN
    -- This procedure will be implemented after model development
    -- Placeholder for now
    SELECT 'ECL calculation procedure - to be implemented' AS message;
END //
DELIMITER ;

-- =====================================================================
-- PART 7: GRANTS AND PERMISSIONS (adjust as needed)
-- =====================================================================

-- Example grants (uncomment and modify as needed)
-- GRANT SELECT ON ecl_model.* TO 'read_user'@'%';
-- GRANT SELECT, INSERT, UPDATE ON ecl_model.* TO 'write_user'@'%';
-- GRANT ALL PRIVILEGES ON ecl_model.* TO 'admin_user'@'%';

-- =====================================================================
-- END OF SCHEMA CREATION SCRIPT
-- =====================================================================