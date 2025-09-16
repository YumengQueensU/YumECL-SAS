/**********************************************************************
 * Script: 03_reporting_queries.sql
 * Purpose: SQL queries for ECL reporting and monitoring
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This script contains queries for:
 * - ECL summary reports
 * - Stage migration analysis  
 * - Portfolio risk metrics
 * - Regulatory reporting
 * - Model performance monitoring
 **********************************************************************/

USE ecl_model;

-- =====================================================================
-- PART 1: ECL SUMMARY REPORTS
-- =====================================================================

-- Overall ECL Summary
CREATE OR REPLACE VIEW v_ecl_summary AS
SELECT 
    e.calculation_date,
    e.product_type,
    e.stage,
    COUNT(*) as account_count,
    SUM(e.exposure_amount) as total_exposure,
    SUM(e.ecl_amount) as total_ecl,
    AVG(e.pd_12m) as avg_pd_12m,
    AVG(e.lgd) as avg_lgd,
    AVG(e.coverage_ratio) as avg_coverage,
    SUM(CASE WHEN e.stage = 1 THEN e.ecl_amount ELSE 0 END) as stage1_ecl,
    SUM(CASE WHEN e.stage = 2 THEN e.ecl_amount ELSE 0 END) as stage2_ecl,
    SUM(CASE WHEN e.stage = 3 THEN e.ecl_amount ELSE 0 END) as stage3_ecl
FROM ecl_results e
WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY e.calculation_date, e.product_type, e.stage
WITH ROLLUP;

-- ECL by Risk Segments
SELECT 
    lf.risk_segment,
    lf.product_type,
    COUNT(DISTINCT e.loan_id) as loan_count,
    SUM(e.exposure_amount) as total_exposure,
    SUM(e.ecl_amount) as total_ecl,
    SUM(e.ecl_amount) / NULLIF(SUM(e.exposure_amount), 0) as coverage_ratio,
    AVG(e.pd_12m) as avg_pd,
    AVG(e.lgd) as avg_lgd
FROM ecl_results e
JOIN loan_features lf ON e.loan_id = lf.loan_id
WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY lf.risk_segment, lf.product_type
ORDER BY total_ecl DESC;

-- =====================================================================
-- PART 2: STAGE MIGRATION ANALYSIS
-- =====================================================================

-- Stage Migration Matrix (Month-over-Month)
WITH current_month AS (
    SELECT 
        loan_id,
        stage,
        ecl_amount,
        calculation_date
    FROM ecl_results
    WHERE calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
),
prior_month AS (
    SELECT 
        loan_id,
        stage,
        ecl_amount,
        calculation_date
    FROM ecl_results
    WHERE calculation_date = DATE_SUB(
        (SELECT MAX(calculation_date) FROM ecl_results), 
        INTERVAL 1 MONTH
    )
)
SELECT 
    COALESCE(p.stage, 0) as from_stage,
    COALESCE(c.stage, 0) as to_stage,
    COUNT(*) as loan_count,
    SUM(c.ecl_amount - COALESCE(p.ecl_amount, 0)) as ecl_change,
    CASE 
        WHEN p.stage IS NULL THEN 'New Origination'
        WHEN c.stage IS NULL THEN 'Paid Off/Written Off'
        WHEN c.stage > p.stage THEN 'Deterioration'
        WHEN c.stage < p.stage THEN 'Improvement'
        ELSE 'No Change'
    END as migration_type
FROM current_month c
FULL OUTER JOIN prior_month p ON c.loan_id = p.loan_id
GROUP BY from_stage, to_stage, migration_type;

-- Detailed Stage Migration Report
CREATE OR REPLACE VIEW v_stage_migration AS
SELECT 
    DATE_FORMAT(curr.calculation_date, '%Y-%m') as reporting_month,
    curr.product_type,
    prev.stage as prev_stage,
    curr.stage as curr_stage,
    COUNT(*) as account_count,
    SUM(curr.exposure_amount) as exposure_amount,
    SUM(curr.ecl_amount) as current_ecl,
    SUM(prev.ecl_amount) as previous_ecl,
    SUM(curr.ecl_amount - COALESCE(prev.ecl_amount, 0)) as ecl_change
FROM ecl_results curr
LEFT JOIN ecl_results prev 
    ON curr.loan_id = prev.loan_id 
    AND prev.calculation_date = DATE_SUB(curr.calculation_date, INTERVAL 1 MONTH)
GROUP BY reporting_month, curr.product_type, prev.stage, curr.stage;

-- =====================================================================
-- PART 3: PORTFOLIO RISK METRICS
-- =====================================================================

-- Key Risk Indicators (KRIs)
CREATE OR REPLACE VIEW v_key_risk_indicators AS
SELECT 
    calculation_date,
    -- Portfolio Quality Metrics
    SUM(CASE WHEN stage = 3 THEN exposure_amount ELSE 0 END) / 
        SUM(exposure_amount) as npl_ratio,
    
    SUM(CASE WHEN stage = 2 THEN exposure_amount ELSE 0 END) / 
        SUM(exposure_amount) as stage2_ratio,
    
    -- Coverage Metrics
    SUM(ecl_amount) / SUM(exposure_amount) as total_coverage,
    
    SUM(CASE WHEN stage = 3 THEN ecl_amount ELSE 0 END) /
        NULLIF(SUM(CASE WHEN stage = 3 THEN exposure_amount ELSE 0 END), 0) as npl_coverage,
    
    -- Average Risk Parameters
    AVG(pd_12m) as avg_pd_12m,
    AVG(CASE WHEN stage = 1 THEN pd_12m ELSE NULL END) as avg_pd_stage1,
    AVG(CASE WHEN stage = 2 THEN pd_12m ELSE NULL END) as avg_pd_stage2,
    AVG(lgd) as avg_lgd,
    
    -- Concentration Metrics
    MAX(exposure_amount) / SUM(exposure_amount) as largest_exposure_concentration,
    COUNT(DISTINCT product_type) as product_diversification,
    
    -- ECL Volatility
    STDDEV(ecl_amount) / AVG(ecl_amount) as ecl_coefficient_variation
    
FROM ecl_results
GROUP BY calculation_date
ORDER BY calculation_date DESC;

-- Vintage Analysis
SELECT 
    DATE_FORMAT(l.origination_date, '%Y-Q%q') as origination_quarter,
    l.product_type,
    TIMESTAMPDIFF(MONTH, l.origination_date, e.calculation_date) as months_on_book,
    COUNT(*) as loan_count,
    SUM(l.original_amount) as original_amount,
    SUM(e.exposure_amount) as current_exposure,
    AVG(e.pd_12m) as avg_pd,
    SUM(CASE WHEN e.stage = 3 THEN 1 ELSE 0 END) / COUNT(*) as default_rate,
    SUM(e.ecl_amount) / SUM(e.exposure_amount) as coverage_ratio
FROM loans l
JOIN ecl_results e ON l.loan_id = e.loan_id
WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY origination_quarter, l.product_type, months_on_book
HAVING months_on_book <= 60
ORDER BY origination_quarter DESC, months_on_book;

-- =====================================================================
-- PART 4: REGULATORY REPORTING (OSFI FORMAT)
-- =====================================================================

-- IFRS 9 Disclosure - ECL by Stage and Product
CREATE OR REPLACE VIEW v_ifrs9_disclosure AS
SELECT 
    'As at ' || DATE_FORMAT(calculation_date, '%Y-%m-%d') as reporting_date,
    product_type,
    stage,
    COUNT(*) as number_of_accounts,
    SUM(exposure_amount) as gross_carrying_amount,
    SUM(ecl_amount) as allowance_for_ecl,
    SUM(exposure_amount - ecl_amount) as net_carrying_amount,
    AVG(pd_12m) * 100 as avg_pd_percent,
    AVG(lgd) * 100 as avg_lgd_percent,
    SUM(ecl_amount) / NULLIF(SUM(exposure_amount), 0) * 100 as coverage_percent
FROM ecl_results
WHERE calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY calculation_date, product_type, stage

UNION ALL

SELECT 
    'As at ' || DATE_FORMAT(calculation_date, '%Y-%m-%d') as reporting_date,
    'TOTAL' as product_type,
    NULL as stage,
    COUNT(*) as number_of_accounts,
    SUM(exposure_amount) as gross_carrying_amount,
    SUM(ecl_amount) as allowance_for_ecl,
    SUM(exposure_amount - ecl_amount) as net_carrying_amount,
    AVG(pd_12m) * 100 as avg_pd_percent,
    AVG(lgd) * 100 as avg_lgd_percent,
    SUM(ecl_amount) / NULLIF(SUM(exposure_amount), 0) * 100 as coverage_percent
FROM ecl_results
WHERE calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY calculation_date;

-- Movement in Allowances (Roll-forward)
WITH ecl_movement AS (
    SELECT 
        e1.product_type,
        e1.stage,
        SUM(e0.ecl_amount) as opening_balance,
        SUM(CASE WHEN e1.loan_id NOT IN (SELECT loan_id FROM ecl_results WHERE calculation_date = DATE_SUB(e1.calculation_date, INTERVAL 1 MONTH))
                THEN e1.ecl_amount ELSE 0 END) as new_originations,
        SUM(CASE WHEN e0.loan_id NOT IN (SELECT loan_id FROM ecl_results WHERE calculation_date = e1.calculation_date)
                THEN -e0.ecl_amount ELSE 0 END) as derecognitions,
        SUM(CASE WHEN e1.stage > COALESCE(e0.stage, 0) THEN e1.ecl_amount - COALESCE(e0.ecl_amount, 0) ELSE 0 END) as transfers_deterioration,
        SUM(CASE WHEN e1.stage < COALESCE(e0.stage, 999) THEN e1.ecl_amount - COALESCE(e0.ecl_amount, 0) ELSE 0 END) as transfers_improvement,
        SUM(CASE WHEN e1.loan_id = e0.loan_id AND e1.stage = e0.stage 
                THEN e1.ecl_amount - e0.ecl_amount ELSE 0 END) as remeasurement,
        SUM(e1.ecl_amount) as closing_balance
    FROM ecl_results e1
    LEFT JOIN ecl_results e0 
        ON e1.loan_id = e0.loan_id 
        AND e0.calculation_date = DATE_SUB(e1.calculation_date, INTERVAL 1 MONTH)
    WHERE e1.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
    GROUP BY e1.product_type, e1.stage
)
SELECT * FROM ecl_movement
ORDER BY product_type, stage;

-- =====================================================================
-- PART 5: MODEL PERFORMANCE MONITORING
-- =====================================================================

-- Back-testing: Predicted vs Actual Defaults
CREATE OR REPLACE VIEW v_model_backtest AS
SELECT 
    DATE_FORMAT(p.prediction_date, '%Y-%m') as cohort_month,
    p.product_type,
    p.pd_bucket,
    COUNT(*) as loan_count,
    AVG(p.pd_12m) as avg_predicted_pd,
    SUM(CASE WHEN l.default_flag = 1 THEN 1 ELSE 0 END) / COUNT(*) as actual_default_rate,
    AVG(p.pd_12m) - (SUM(CASE WHEN l.default_flag = 1 THEN 1 ELSE 0 END) / COUNT(*)) as prediction_error,
    ABS(AVG(p.pd_12m) - (SUM(CASE WHEN l.default_flag = 1 THEN 1 ELSE 0 END) / COUNT(*))) as absolute_error
FROM pd_results p
JOIN loans l ON p.loan_id = l.loan_id
WHERE p.prediction_date <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY cohort_month, p.product_type, p.pd_bucket
ORDER BY cohort_month DESC, p.product_type, p.pd_bucket;

-- Population Stability Index (PSI) Monitoring
CREATE OR REPLACE VIEW v_psi_monitoring AS
WITH current_distribution AS (
    SELECT 
        pd_bucket,
        COUNT(*) as current_count,
        COUNT(*) / SUM(COUNT(*)) OVER() as current_pct
    FROM pd_results
    WHERE prediction_date = (SELECT MAX(prediction_date) FROM pd_results)
    GROUP BY pd_bucket
),
baseline_distribution AS (
    SELECT 
        pd_bucket,
        COUNT(*) as baseline_count,
        COUNT(*) / SUM(COUNT(*)) OVER() as baseline_pct
    FROM pd_results
    WHERE prediction_date = (SELECT MIN(prediction_date) FROM pd_results WHERE prediction_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH))
    GROUP BY pd_bucket
)
SELECT 
    c.pd_bucket,
    c.current_count,
    c.current_pct * 100 as current_percent,
    b.baseline_count,
    b.baseline_pct * 100 as baseline_percent,
    (c.current_pct - b.baseline_pct) * LN(c.current_pct / NULLIF(b.baseline_pct, 0)) as psi_contribution,
    CASE 
        WHEN (c.current_pct - b.baseline_pct) * LN(c.current_pct / NULLIF(b.baseline_pct, 0)) < 0.1 THEN 'Stable'
        WHEN (c.current_pct - b.baseline_pct) * LN(c.current_pct / NULLIF(b.baseline_pct, 0)) < 0.25 THEN 'Slight Shift'
        ELSE 'Significant Shift'
    END as stability_flag
FROM current_distribution c
JOIN baseline_distribution b ON c.pd_bucket = b.pd_bucket;

-- Model Performance Metrics Over Time
CREATE OR REPLACE VIEW v_model_performance_trend AS
SELECT 
    DATE_FORMAT(calculation_date, '%Y-%m') as month,
    AVG(CASE WHEN stage = 1 THEN pd_12m ELSE NULL END) as avg_pd_stage1,
    AVG(CASE WHEN stage = 2 THEN pd_12m ELSE NULL END) as avg_pd_stage2,
    AVG(CASE WHEN stage = 3 THEN pd_12m ELSE NULL END) as avg_pd_stage3,
    AVG(lgd) as avg_lgd,
    SUM(ecl_amount) as total_ecl,
    SUM(ecl_amount) / SUM(exposure_amount) as coverage_ratio,
    COUNT(*) as total_accounts,
    SUM(CASE WHEN stage = 3 THEN 1 ELSE 0 END) as default_accounts
FROM ecl_results
GROUP BY month
ORDER BY month DESC
LIMIT 24;

-- =====================================================================
-- PART 6: STRESS TESTING REPORTS
-- =====================================================================

-- Stress Test Scenario Comparison
CREATE OR REPLACE VIEW v_stress_test_comparison AS
SELECT 
    s.scenario_name,
    e.product_type,
    COUNT(*) as loan_count,
    SUM(e.exposure_amount) as total_exposure,
    SUM(e.ecl_baseline) as ecl_baseline,
    SUM(e.ecl_adverse) as ecl_adverse,
    SUM(e.ecl_severe) as ecl_severe,
    (SUM(e.ecl_adverse) - SUM(e.ecl_baseline)) / SUM(e.ecl_baseline) * 100 as adverse_impact_pct,
    (SUM(e.ecl_severe) - SUM(e.ecl_baseline)) / SUM(e.ecl_baseline) * 100 as severe_impact_pct
FROM ecl_results e
CROSS JOIN stress_test_scenarios s
WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
GROUP BY s.scenario_name, e.product_type;

-- Capital Impact Under Stress
SELECT 
    scenario_type,
    SUM(CASE WHEN scenario_type = 'baseline' THEN total_ecl ELSE 0 END) as baseline_ecl,
    SUM(CASE WHEN scenario_type = 'adverse' THEN total_ecl * 1.5 ELSE 0 END) as adverse_ecl,
    SUM(CASE WHEN scenario_type = 'severely_adverse' THEN total_ecl * 2.0 ELSE 0 END) as severe_ecl,
    SUM(CASE WHEN scenario_type = 'adverse' THEN total_ecl * 1.5 ELSE 0 END) - 
        SUM(CASE WHEN scenario_type = 'baseline' THEN total_ecl ELSE 0 END) as additional_capital_adverse,
    SUM(CASE WHEN scenario_type = 'severely_adverse' THEN total_ecl * 2.0 ELSE 0 END) - 
        SUM(CASE WHEN scenario_type = 'baseline' THEN total_ecl ELSE 0 END) as additional_capital_severe
FROM (
    SELECT 
        'baseline' as scenario_type,
        SUM(ecl_amount) as total_ecl
    FROM ecl_results
    WHERE calculation_date = (SELECT MAX(calculation_date) FROM ecl_results)
) t
GROUP BY scenario_type;

-- =====================================================================
-- PART 7: DATA QUALITY MONITORING
-- =====================================================================

-- Data Quality Dashboard
CREATE OR REPLACE VIEW v_data_quality_dashboard AS
SELECT 
    check_date,
    table_name,
    check_type,
    check_result,
    metric_value,
    CASE 
        WHEN check_result = 'PASS' THEN 'Green'
        WHEN check_result = 'WARNING' THEN 'Yellow'
        WHEN check_result = 'FAIL' THEN 'Red'
        ELSE 'Unknown'
    END as status_color,
    created_at
FROM data_quality_metrics
WHERE check_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
ORDER BY check_date DESC, table_name, check_type;

-- Missing Data Summary
SELECT 
    'loans' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN credit_score IS NULL THEN 1 ELSE 0 END) as missing_credit_score,
    SUM(CASE WHEN annual_income IS NULL THEN 1 ELSE 0 END) as missing_income,
    SUM(CASE WHEN loan_to_value IS NULL THEN 1 ELSE 0 END) as missing_ltv,
    SUM(CASE WHEN province IS NULL OR province = 'UNKNOWN' THEN 1 ELSE 0 END) as missing_province
FROM loans
UNION ALL
SELECT 
    'payment_history' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN days_past_due IS NULL THEN 1 ELSE 0 END) as missing_dpd,
    SUM(CASE WHEN actual_amount IS NULL THEN 1 ELSE 0 END) as missing_payment,
    NULL as missing_ltv,
    NULL as missing_province
FROM payment_history;

-- =====================================================================
-- PART 8: EXECUTIVE DASHBOARD QUERIES
-- =====================================================================

-- Executive Summary Dashboard
CREATE OR REPLACE VIEW v_executive_dashboard AS
SELECT 
    -- Portfolio Overview
    (SELECT COUNT(DISTINCT loan_id) FROM ecl_results WHERE calculation_date = CURDATE()) as total_accounts,
    (SELECT SUM(exposure_amount) FROM ecl_results WHERE calculation_date = CURDATE()) as total_exposure,
    (SELECT SUM(ecl_amount) FROM ecl_results WHERE calculation_date = CURDATE()) as total_provisions,
    
    -- Risk Metrics
    (SELECT SUM(ecl_amount) / SUM(exposure_amount) * 100 
     FROM ecl_results WHERE calculation_date = CURDATE()) as overall_coverage_pct,
    
    (SELECT SUM(CASE WHEN stage = 3 THEN exposure_amount ELSE 0 END) / SUM(exposure_amount) * 100
     FROM ecl_results WHERE calculation_date = CURDATE()) as npl_ratio_pct,
    
    -- Month-over-Month Changes
    (SELECT 
        (SUM(CASE WHEN calculation_date = CURDATE() THEN ecl_amount ELSE 0 END) -
         SUM(CASE WHEN calculation_date = DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN ecl_amount ELSE 0 END)) /
         SUM(CASE WHEN calculation_date = DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN ecl_amount ELSE 0 END) * 100
     FROM ecl_results
     WHERE calculation_date IN (CURDATE(), DATE_SUB(CURDATE(), INTERVAL 1 MONTH))) as ecl_mom_change_pct,
    
    -- Top Risk Segments
    (SELECT product_type 
     FROM ecl_results 
     WHERE calculation_date = CURDATE()
     GROUP BY product_type 
     ORDER BY SUM(ecl_amount) DESC 
     LIMIT 1) as highest_risk_product,
    
    -- Model Performance
    (SELECT AVG(ABS(predicted_pd - actual_default_rate)) * 100
     FROM v_model_backtest
     WHERE cohort_month >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 12 MONTH), '%Y-%m')) as avg_pd_error_pct;

-- Trend Analysis for Executive Reporting
CREATE OR REPLACE VIEW v_executive_trends AS
SELECT 
    DATE_FORMAT(calculation_date, '%Y-%m') as month,
    SUM(exposure_amount) / 1000000 as exposure_millions,
    SUM(ecl_amount) / 1000000 as ecl_millions,
    SUM(ecl_amount) / SUM(exposure_amount) * 100 as coverage_pct,
    SUM(CASE WHEN stage = 1 THEN exposure_amount ELSE 0 END) / SUM(exposure_amount) * 100 as stage1_pct,
    SUM(CASE WHEN stage = 2 THEN exposure_amount ELSE 0 END) / SUM(exposure_amount) * 100 as stage2_pct,
    SUM(CASE WHEN stage = 3 THEN exposure_amount ELSE 0 END) / SUM(exposure_amount) * 100 as stage3_pct
FROM ecl_results
WHERE calculation_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY month
ORDER BY month;

-- =====================================================================
-- PART 9: STORED PROCEDURES FOR REPORTING
-- =====================================================================

-- Procedure to generate monthly ECL report
DELIMITER //
CREATE PROCEDURE sp_generate_monthly_report(IN p_report_date DATE)
BEGIN
    DECLARE v_report_month VARCHAR(7);
    SET v_report_month = DATE_FORMAT(p_report_date, '%Y-%m');
    
    -- Create temporary summary table
    DROP TEMPORARY TABLE IF EXISTS temp_monthly_summary;
    CREATE TEMPORARY TABLE temp_monthly_summary AS
    SELECT 
        v_report_month as report_month,
        product_type,
        stage,
        COUNT(*) as account_count,
        SUM(exposure_amount) as exposure,
        SUM(ecl_amount) as ecl,
        AVG(pd_12m) as avg_pd,
        AVG(lgd) as avg_lgd
    FROM ecl_results
    WHERE DATE_FORMAT(calculation_date, '%Y-%m') = v_report_month
    GROUP BY product_type, stage;
    
    -- Insert into reporting history
    INSERT INTO model_monitoring (
        monitoring_date,
        metric_name,
        metric_value,
        product_type,
        stage
    )
    SELECT 
        p_report_date,
        'MONTHLY_ECL',
        ecl,
        product_type,
        stage
    FROM temp_monthly_summary;
    
    -- Return summary
    SELECT * FROM temp_monthly_summary;
    
END //
DELIMITER ;

-- Procedure to calculate quarter-end provisions
DELIMITER //
CREATE PROCEDURE sp_calculate_quarterly_provisions(IN p_quarter_end DATE)
BEGIN
    DECLARE v_quarter VARCHAR(6);
    SET v_quarter = CONCAT(YEAR(p_quarter_end), '-Q', QUARTER(p_quarter_end));
    
    SELECT 
        v_quarter as reporting_quarter,
        product_type,
        SUM(CASE WHEN stage = 1 THEN ecl_amount ELSE 0 END) as stage1_provisions,
        SUM(CASE WHEN stage = 2 THEN ecl_amount ELSE 0 END) as stage2_provisions,
        SUM(CASE WHEN stage = 3 THEN ecl_amount ELSE 0 END) as stage3_provisions,
        SUM(ecl_amount) as total_provisions,
        SUM(exposure_amount) as total_exposure,
        SUM(ecl_amount) / SUM(exposure_amount) * 100 as coverage_ratio
    FROM ecl_results
    WHERE calculation_date = p_quarter_end
    GROUP BY product_type WITH ROLLUP;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 10: AUDIT TRAIL QUERIES
-- =====================================================================

-- Model Change Audit Log
CREATE OR REPLACE VIEW v_model_audit_log AS
SELECT 
    monitoring_date,
    metric_name,
    old_value,
    new_value,
    change_reason,
    approved_by,
    created_at
FROM model_monitoring
WHERE metric_name IN ('MODEL_UPDATE', 'PARAMETER_CHANGE', 'OVERLAY_ADJUSTMENT')
ORDER BY created_at DESC;

-- ECL Calculation Audit Trail
SELECT 
    calculation_date,
    loan_id,
    'PD' as component,
    pd_12m as value,
    'Model Output' as source
FROM ecl_results
WHERE calculation_date = CURDATE()
    AND loan_id IN (SELECT loan_id FROM loans LIMIT 10)
UNION ALL
SELECT 
    calculation_date,
    loan_id,
    'LGD' as component,
    lgd as value,
    'Model Output' as source
FROM ecl_results
WHERE calculation_date = CURDATE()
    AND loan_id IN (SELECT loan_id FROM loans LIMIT 10)
UNION ALL
SELECT 
    calculation_date,
    loan_id,
    'EAD' as component,
    exposure_amount as value,
    'Calculated' as source
FROM ecl_results
WHERE calculation_date = CURDATE()
    AND loan_id IN (SELECT loan_id FROM loans LIMIT 10)
ORDER BY loan_id, component;

-- =====================================================================
-- PART 11: REGULATORY COMPLIANCE CHECKS
-- =====================================================================

-- IFRS 9 Compliance Checklist
CREATE OR REPLACE VIEW v_ifrs9_compliance AS
SELECT 
    'Stage Assignment' as requirement,
    CASE WHEN COUNT(*) = SUM(CASE WHEN stage IN (1,2,3) THEN 1 ELSE 0 END) 
         THEN 'Compliant' ELSE 'Non-Compliant' END as status,
    COUNT(*) as total_records,
    SUM(CASE WHEN stage IS NULL THEN 1 ELSE 0 END) as exceptions
FROM ecl_results
WHERE calculation_date = CURDATE()
UNION ALL
SELECT 
    'ECL Calculation' as requirement,
    CASE WHEN COUNT(*) = SUM(CASE WHEN ecl_amount >= 0 THEN 1 ELSE 0 END)
         THEN 'Compliant' ELSE 'Non-Compliant' END as status,
    COUNT(*) as total_records,
    SUM(CASE WHEN ecl_amount < 0 OR ecl_amount IS NULL THEN 1 ELSE 0 END) as exceptions
FROM ecl_results
WHERE calculation_date = CURDATE()
UNION ALL
SELECT 
    'PD Boundaries' as requirement,
    CASE WHEN COUNT(*) = SUM(CASE WHEN pd_12m BETWEEN 0 AND 1 THEN 1 ELSE 0 END)
         THEN 'Compliant' ELSE 'Non-Compliant' END as status,
    COUNT(*) as total_records,
    SUM(CASE WHEN pd_12m < 0 OR pd_12m > 1 OR pd_12m IS NULL THEN 1 ELSE 0 END) as exceptions
FROM ecl_results
WHERE calculation_date = CURDATE();

-- =====================================================================
-- END OF REPORTING QUERIES SCRIPT
-- =====================================================================