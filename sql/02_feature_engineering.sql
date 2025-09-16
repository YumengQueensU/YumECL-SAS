/**********************************************************************
 * Script: 02_feature_engineering.sql
 * Purpose: Feature engineering and data preparation for IFRS 9 ECL Model
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This script creates features from raw data for model development
 **********************************************************************/

USE ecl_model;

-- =====================================================================
-- PART 1: DATA LOADING PROCEDURES
-- =====================================================================

-- Procedure to load loans data from staging
DELIMITER //
CREATE PROCEDURE sp_load_loans_data(IN p_file_date VARCHAR(8))
BEGIN
    DECLARE v_record_count INT;
    DECLARE v_error_msg VARCHAR(255);
    
    -- Log start
    INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result)
    VALUES (CURDATE(), 'loans', 'LOAD_START', 'Started');
    
    -- Create staging table
    DROP TEMPORARY TABLE IF EXISTS temp_loans_staging;
    CREATE TEMPORARY TABLE temp_loans_staging (
        loan_id VARCHAR(20),
        customer_id VARCHAR(20),
        origination_date VARCHAR(50),
        product_type VARCHAR(50),
        province CHAR(2),
        original_amount DECIMAL(18,2),
        interest_rate DECIMAL(10,6),
        credit_score INT,
        annual_income DECIMAL(18,2),
        loan_to_value DECIMAL(10,6),
        default_flag INT
    );
    
    -- Note: In production, use LOAD DATA INFILE or external ETL tool
    -- This is a placeholder for the actual data loading mechanism
    SET @sql = CONCAT('LOAD DATA LOCAL INFILE ''', 
                     'C:/YumECL-SAS/data/sample/loans_', p_file_date, '.csv''',
                     ' INTO TABLE temp_loans_staging',
                     ' FIELDS TERMINATED BY '',''',
                     ' ENCLOSED BY ''"''',
                     ' LINES TERMINATED BY ''\\n''',
                     ' IGNORE 1 LINES');
    
    -- PREPARE stmt FROM @sql;
    -- EXECUTE stmt;
    -- DEALLOCATE PREPARE stmt;
    
    -- Data quality checks
    SELECT COUNT(*) INTO v_record_count FROM temp_loans_staging;
    
    IF v_record_count > 0 THEN
        -- Insert/Update main table
        INSERT INTO loans (
            loan_id, customer_id, origination_date, product_type,
            province, original_amount, interest_rate, credit_score,
            annual_income, loan_to_value, default_flag
        )
        SELECT 
            loan_id, 
            customer_id, 
            STR_TO_DATE(origination_date, '%Y-%m-%d %H:%i:%s'),
            product_type,
            province, 
            original_amount, 
            interest_rate, 
            credit_score,
            annual_income, 
            loan_to_value, 
            default_flag
        FROM temp_loans_staging
        ON DUPLICATE KEY UPDATE
            product_type = VALUES(product_type),
            default_flag = VALUES(default_flag),
            updated_date = CURRENT_TIMESTAMP;
        
        -- Log success
        INSERT INTO data_quality_metrics (check_date, table_name, total_records, check_type, check_result)
        VALUES (CURDATE(), 'loans', v_record_count, 'LOAD_COMPLETE', 'Success');
    ELSE
        -- Log failure
        INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result, check_details)
        VALUES (CURDATE(), 'loans', 'LOAD_FAILED', 'Failed', 'No records found in staging');
    END IF;
    
    DROP TEMPORARY TABLE IF EXISTS temp_loans_staging;
    
END //
DELIMITER ;

-- Procedure to load payment history data
DELIMITER //
CREATE PROCEDURE sp_load_payment_history(IN p_file_date VARCHAR(8))
BEGIN
    DECLARE v_record_count INT;
    
    -- Create staging table
    DROP TEMPORARY TABLE IF EXISTS temp_payment_staging;
    CREATE TEMPORARY TABLE temp_payment_staging (
        loan_id VARCHAR(20),
        payment_date VARCHAR(50),
        scheduled_amount DECIMAL(18,2),
        days_past_due INT
    );
    
    -- Load data (placeholder - implement actual loading mechanism)
    -- LOAD DATA INFILE...
    
    -- Insert into main table
    INSERT INTO payment_history (loan_id, payment_date, scheduled_amount, days_past_due)
    SELECT 
        loan_id,
        STR_TO_DATE(payment_date, '%Y-%m-%d %H:%i:%s'),
        scheduled_amount,
        days_past_due
    FROM temp_payment_staging;
    
    SELECT COUNT(*) INTO v_record_count FROM temp_payment_staging;
    
    -- Log results
    INSERT INTO data_quality_metrics (check_date, table_name, total_records, check_type, check_result)
    VALUES (CURDATE(), 'payment_history', v_record_count, 'LOAD_COMPLETE', 'Success');
    
    DROP TEMPORARY TABLE IF EXISTS temp_payment_staging;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 2: BEHAVIORAL FEATURE CALCULATION
-- =====================================================================

-- Procedure to calculate payment behavior features
DELIMITER //
CREATE PROCEDURE sp_calculate_payment_features(IN p_observation_date DATE)
BEGIN
    DECLARE v_start_time DATETIME;
    DECLARE v_end_time DATETIME;
    
    SET v_start_time = NOW();
    
    -- Create temporary table for payment features
    DROP TEMPORARY TABLE IF EXISTS temp_payment_features;
    CREATE TEMPORARY TABLE temp_payment_features (
        loan_id VARCHAR(20) PRIMARY KEY,
        observation_date DATE,
        max_dpd_3m INT,
        max_dpd_6m INT,
        max_dpd_12m INT,
        current_dpd INT,
        num_payments_30dpd INT,
        num_payments_60dpd INT,
        num_payments_90dpd INT,
        avg_dpd_3m DECIMAL(10,2),
        avg_dpd_6m DECIMAL(10,2),
        avg_dpd_12m DECIMAL(10,2),
        payment_regularity_score DECIMAL(10,4),
        months_since_last_payment INT,
        total_payments_made INT
    );
    
    -- Calculate payment behavior metrics
    INSERT INTO temp_payment_features
    SELECT 
        ph.loan_id,
        p_observation_date as observation_date,
        
        -- Maximum DPD in different windows
        MAX(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 3 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as max_dpd_3m,
        MAX(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 6 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as max_dpd_6m,
        MAX(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 12 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as max_dpd_12m,
        
        -- Current DPD (most recent payment)
        (SELECT days_past_due FROM payment_history ph2 
         WHERE ph2.loan_id = ph.loan_id 
         AND ph2.payment_date <= p_observation_date
         ORDER BY ph2.payment_date DESC LIMIT 1) as current_dpd,
        
        -- Count of delinquent payments
        SUM(CASE WHEN ph.days_past_due >= 30 THEN 1 ELSE 0 END) as num_payments_30dpd,
        SUM(CASE WHEN ph.days_past_due >= 60 THEN 1 ELSE 0 END) as num_payments_60dpd,
        SUM(CASE WHEN ph.days_past_due >= 90 THEN 1 ELSE 0 END) as num_payments_90dpd,
        
        -- Average DPD in different windows
        AVG(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 3 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as avg_dpd_3m,
        AVG(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 6 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as avg_dpd_6m,
        AVG(CASE WHEN ph.payment_date >= DATE_SUB(p_observation_date, INTERVAL 12 MONTH) 
                 THEN ph.days_past_due ELSE NULL END) as avg_dpd_12m,
        
        -- Payment regularity score (lower variance = more regular)
        1 / (1 + COALESCE(STDDEV(ph.days_past_due), 0)) as payment_regularity_score,
        
        -- Months since last payment
        TIMESTAMPDIFF(MONTH, MAX(ph.payment_date), p_observation_date) as months_since_last_payment,
        
        -- Total payments made
        COUNT(*) as total_payments_made
        
    FROM payment_history ph
    WHERE ph.payment_date <= p_observation_date
    GROUP BY ph.loan_id;
    
    -- Update loan_features table
    INSERT INTO loan_features (
        loan_id, feature_date,
        max_dpd_3m, max_dpd_6m, max_dpd_12m, current_dpd,
        num_payments_30dpd, num_payments_60dpd, num_payments_90dpd,
        updated_date
    )
    SELECT 
        loan_id, observation_date,
        COALESCE(max_dpd_3m, 0),
        COALESCE(max_dpd_6m, 0),
        COALESCE(max_dpd_12m, 0),
        COALESCE(current_dpd, 0),
        COALESCE(num_payments_30dpd, 0),
        COALESCE(num_payments_60dpd, 0),
        COALESCE(num_payments_90dpd, 0),
        NOW()
    FROM temp_payment_features
    ON DUPLICATE KEY UPDATE
        max_dpd_3m = VALUES(max_dpd_3m),
        max_dpd_6m = VALUES(max_dpd_6m),
        max_dpd_12m = VALUES(max_dpd_12m),
        current_dpd = VALUES(current_dpd),
        num_payments_30dpd = VALUES(num_payments_30dpd),
        num_payments_60dpd = VALUES(num_payments_60dpd),
        num_payments_90dpd = VALUES(num_payments_90dpd),
        updated_date = NOW();
    
    SET v_end_time = NOW();
    
    -- Log execution time
    INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result, check_details)
    VALUES (CURDATE(), 'payment_features', 'CALCULATION_TIME', 'Success',
            CONCAT('Execution time: ', TIMEDIFF(v_end_time, v_start_time)));
    
    DROP TEMPORARY TABLE IF EXISTS temp_payment_features;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 3: MACRO ECONOMIC FEATURE MATCHING
-- =====================================================================

-- Procedure to match macro features to loans
DELIMITER //
CREATE PROCEDURE sp_match_macro_features(IN p_observation_date DATE)
BEGIN
    
    -- Create temporary table for macro matching
    DROP TEMPORARY TABLE IF EXISTS temp_macro_features;
    CREATE TEMPORARY TABLE temp_macro_features (
        loan_id VARCHAR(20) PRIMARY KEY,
        origination_date DATE,
        observation_date DATE,
        -- Macro at origination
        unemployment_rate_orig DECIMAL(10,4),
        gdp_growth_orig DECIMAL(10,4),
        policy_rate_orig DECIMAL(10,6),
        hpi_change_orig DECIMAL(10,4),
        credit_conditions_orig DECIMAL(10,6),
        -- Current macro
        unemployment_rate_current DECIMAL(10,4),
        gdp_growth_current DECIMAL(10,4),
        policy_rate_current DECIMAL(10,6),
        hpi_change_current DECIMAL(10,4),
        credit_conditions_current DECIMAL(10,6),
        -- Changes
        unemployment_rate_change DECIMAL(10,4),
        policy_rate_change DECIMAL(10,6),
        hpi_change_delta DECIMAL(10,4)
    );
    
    -- Match macro features
    INSERT INTO temp_macro_features
    SELECT 
        l.loan_id,
        DATE(l.origination_date) as origination_date,
        p_observation_date as observation_date,
        
        -- Macro at origination
        m_orig.unemployment_rate as unemployment_rate_orig,
        m_orig.gdp_growth_yoy as gdp_growth_orig,
        m_orig.policy_rate as policy_rate_orig,
        m_orig.hpi_change_yoy as hpi_change_orig,
        m_orig.credit_conditions as credit_conditions_orig,
        
        -- Current macro
        m_curr.unemployment_rate as unemployment_rate_current,
        m_curr.gdp_growth_yoy as gdp_growth_current,
        m_curr.policy_rate as policy_rate_current,
        m_curr.hpi_change_yoy as hpi_change_current,
        m_curr.credit_conditions as credit_conditions_current,
        
        -- Changes
        m_curr.unemployment_rate - m_orig.unemployment_rate as unemployment_rate_change,
        m_curr.policy_rate - m_orig.policy_rate as policy_rate_change,
        m_curr.hpi_change_yoy - m_orig.hpi_change_yoy as hpi_change_delta
        
    FROM loans l
    LEFT JOIN macro_data m_orig ON DATE(m_orig.data_date) = 
        LAST_DAY(DATE_SUB(DATE(l.origination_date), INTERVAL DAY(DATE(l.origination_date))-1 DAY))
    LEFT JOIN macro_data m_curr ON DATE(m_curr.data_date) = 
        LAST_DAY(DATE_SUB(p_observation_date, INTERVAL DAY(p_observation_date)-1 DAY));
    
    -- Update loan_features with macro data
    UPDATE loan_features lf
    INNER JOIN temp_macro_features tmf ON lf.loan_id = tmf.loan_id
    SET 
        lf.unemployment_rate_orig = tmf.unemployment_rate_orig,
        lf.gdp_growth_orig = tmf.gdp_growth_orig,
        lf.policy_rate_orig = tmf.policy_rate_orig,
        lf.hpi_change_orig = tmf.hpi_change_orig,
        lf.unemployment_rate_current = tmf.unemployment_rate_current,
        lf.gdp_growth_current = tmf.gdp_growth_current,
        lf.policy_rate_current = tmf.policy_rate_current,
        lf.hpi_change_current = tmf.hpi_change_current,
        lf.updated_date = NOW()
    WHERE lf.feature_date = p_observation_date;
    
    DROP TEMPORARY TABLE IF EXISTS temp_macro_features;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 4: IFRS 9 STAGING LOGIC
-- =====================================================================

-- Function to determine IFRS 9 stage
DELIMITER //
CREATE FUNCTION fn_get_ifrs9_stage(
    p_current_dpd INT,
    p_max_dpd_12m INT,
    p_pd_current DECIMAL(10,8),
    p_pd_origination DECIMAL(10,8)
) RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE v_stage INT DEFAULT 1;
    DECLARE v_sicr_threshold DECIMAL(10,4) DEFAULT 2.0;
    
    -- Stage 3: Non-performing (90+ DPD)
    IF p_current_dpd >= 90 OR p_max_dpd_12m >= 90 THEN
        SET v_stage = 3;
    -- Stage 2: Significant increase in credit risk
    ELSEIF p_current_dpd >= 30 
        OR (p_pd_current IS NOT NULL AND p_pd_origination IS NOT NULL 
            AND p_pd_current > p_pd_origination * v_sicr_threshold) THEN
        SET v_stage = 2;
    -- Stage 1: Performing
    ELSE
        SET v_stage = 1;
    END IF;
    
    RETURN v_stage;
END //
DELIMITER ;

-- Procedure to calculate IFRS 9 staging
DELIMITER //
CREATE PROCEDURE sp_calculate_ifrs9_staging(IN p_observation_date DATE)
BEGIN
    
    -- Update staging in loan_features
    UPDATE loan_features lf
    SET 
        lf.stage_ifrs9 = fn_get_ifrs9_stage(
            lf.current_dpd,
            lf.max_dpd_12m,
            NULL,  -- PD will be added after model development
            NULL   -- PD at origination will be added after model development
        ),
        lf.updated_date = NOW()
    WHERE lf.feature_date = p_observation_date;
    
    -- Log staging distribution
    INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result, check_details)
    SELECT 
        CURDATE(),
        'loan_features',
        'STAGING_DISTRIBUTION',
        'Success',
        CONCAT('Stage 1: ', COUNT(CASE WHEN stage_ifrs9 = 1 THEN 1 END),
               ', Stage 2: ', COUNT(CASE WHEN stage_ifrs9 = 2 THEN 1 END),
               ', Stage 3: ', COUNT(CASE WHEN stage_ifrs9 = 3 THEN 1 END))
    FROM loan_features
    WHERE feature_date = p_observation_date;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 5: RISK SEGMENTATION
-- =====================================================================

-- Procedure to assign risk segments
DELIMITER //
CREATE PROCEDURE sp_assign_risk_segments(IN p_observation_date DATE)
BEGIN
    
    -- Update risk segments based on credit score and DPD history
    UPDATE loan_features lf
    INNER JOIN loans l ON lf.loan_id = l.loan_id
    SET lf.risk_segment = 
        CASE 
            WHEN lf.stage_ifrs9 = 3 THEN 'HIGH'
            WHEN lf.stage_ifrs9 = 2 THEN 'MEDIUM'
            WHEN l.credit_score >= 750 AND lf.max_dpd_12m = 0 THEN 'LOW'
            WHEN l.credit_score >= 700 AND lf.max_dpd_12m <= 30 THEN 'LOW'
            WHEN l.credit_score >= 650 AND lf.max_dpd_12m <= 60 THEN 'MEDIUM'
            WHEN l.credit_score < 650 OR lf.max_dpd_12m > 60 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        lf.updated_date = NOW()
    WHERE lf.feature_date = p_observation_date;
    
    -- Log risk segment distribution
    INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result, check_details)
    SELECT 
        CURDATE(),
        'loan_features',
        'RISK_SEGMENT_DISTRIBUTION',
        'Success',
        CONCAT('LOW: ', COUNT(CASE WHEN risk_segment = 'LOW' THEN 1 END),
               ', MEDIUM: ', COUNT(CASE WHEN risk_segment = 'MEDIUM' THEN 1 END),
               ', HIGH: ', COUNT(CASE WHEN risk_segment = 'HIGH' THEN 1 END))
    FROM loan_features
    WHERE feature_date = p_observation_date;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 6: CURRENT BALANCE CALCULATION
-- =====================================================================

-- Procedure to calculate current balance
DELIMITER //
CREATE PROCEDURE sp_calculate_current_balance(IN p_observation_date DATE)
BEGIN
    
    -- Simple amortization calculation (placeholder for actual logic)
    UPDATE loan_features lf
    INNER JOIN loans l ON lf.loan_id = l.loan_id
    INNER JOIN (
        SELECT 
            loan_id,
            COUNT(*) as payments_made,
            SUM(scheduled_amount) as total_paid
        FROM payment_history
        WHERE payment_date <= p_observation_date
        GROUP BY loan_id
    ) ph ON lf.loan_id = ph.loan_id
    SET 
        lf.current_balance = GREATEST(
            0,
            l.original_amount - ph.total_paid
        ),
        lf.months_on_book = TIMESTAMPDIFF(MONTH, l.origination_date, p_observation_date),
        lf.updated_date = NOW()
    WHERE lf.feature_date = p_observation_date;
    
END //
DELIMITER ;

-- =====================================================================
-- PART 7: MASTER FEATURE ENGINEERING PROCEDURE
-- =====================================================================

-- Master procedure to run all feature engineering steps
DELIMITER //
CREATE PROCEDURE sp_run_feature_engineering(
    IN p_observation_date DATE,
    IN p_file_date VARCHAR(8)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result, check_details)
        VALUES (CURDATE(), 'feature_engineering', 'ERROR', 'Failed', 'SQL Exception occurred');
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Step 1: Load data if file_date provided
    IF p_file_date IS NOT NULL THEN
        CALL sp_load_loans_data(p_file_date);
        CALL sp_load_payment_history(p_file_date);
    END IF;
    
    -- Step 2: Calculate payment features
    CALL sp_calculate_payment_features(p_observation_date);
    
    -- Step 3: Match macro features
    CALL sp_match_macro_features(p_observation_date);
    
    -- Step 4: Calculate IFRS 9 staging
    CALL sp_calculate_ifrs9_staging(p_observation_date);
    
    -- Step 5: Assign risk segments
    CALL sp_assign_risk_segments(p_observation_date);
    
    -- Step 6: Calculate current balance
    CALL sp_calculate_current_balance(p_observation_date);
    
    -- Step 7: Copy loan characteristics to features table
    INSERT INTO loan_features (
        loan_id, origination_date, product_type, province,
        original_amount, interest_rate, credit_score, 
        annual_income, loan_to_value, feature_date
    )
    SELECT 
        loan_id, DATE(origination_date), product_type, province,
        original_amount, interest_rate, credit_score,
        annual_income, loan_to_value, p_observation_date
    FROM loans
    ON DUPLICATE KEY UPDATE
        product_type = VALUES(product_type),
        updated_date = NOW();
    
    COMMIT;
    
    -- Log success
    INSERT INTO data_quality_metrics (check_date, table_name, check_type, check_result)
    VALUES (CURDATE(), 'feature_engineering', 'COMPLETE', 'Success');
    
END //
DELIMITER ;

-- =====================================================================
-- PART 8: DATA QUALITY VIEWS
-- =====================================================================

-- View for feature completeness
CREATE OR REPLACE VIEW v_feature_completeness AS
SELECT 
    feature_date,
    COUNT(*) as total_loans,
    SUM(CASE WHEN current_balance IS NOT NULL THEN 1 ELSE 0 END) as with_balance,
    SUM(CASE WHEN stage_ifrs9 IS NOT NULL THEN 1 ELSE 0 END) as with_stage,
    SUM(CASE WHEN risk_segment IS NOT NULL THEN 1 ELSE 0 END) as with_segment,
    SUM(CASE WHEN unemployment_rate_current IS NOT NULL THEN 1 ELSE 0 END) as with_macro,
    MIN(created_date) as first_created,
    MAX(updated_date) as last_updated
FROM loan_features
GROUP BY feature_date
ORDER BY feature_date DESC;

-- View for staging migration
CREATE OR REPLACE VIEW v_stage_transitions AS
SELECT 
    curr.feature_date,
    prev.stage_ifrs9 as prev_stage,
    curr.stage_ifrs9 as curr_stage,
    COUNT(*) as loan_count,
    SUM(curr.current_balance) as total_balance,
    AVG(curr.current_balance) as avg_balance
FROM loan_features curr
LEFT JOIN loan_features prev 
    ON curr.loan_id = prev.loan_id 
    AND prev.feature_date = DATE_SUB(curr.feature_date, INTERVAL 1 MONTH)
GROUP BY curr.feature_date, prev.stage_ifrs9, curr.stage_ifrs9;

-- View for portfolio summary
CREATE OR REPLACE VIEW v_portfolio_summary AS
SELECT 
    lf.feature_date,
    lf.product_type,
    lf.stage_ifrs9,
    lf.risk_segment,
    COUNT(*) as loan_count,
    SUM(lf.current_balance) as total_exposure,
    AVG(lf.current_balance) as avg_balance,
    AVG(l.credit_score) as avg_credit_score,
    AVG(lf.current_dpd) as avg_dpd,
    MAX(lf.max_dpd_12m) as max_dpd_portfolio
FROM loan_features lf
INNER JOIN loans l ON lf.loan_id = l.loan_id
GROUP BY lf.feature_date, lf.product_type, lf.stage_ifrs9, lf.risk_segment;

-- =====================================================================
-- PART 9: EXECUTION EXAMPLE
-- =====================================================================

-- Example: Run feature engineering for current date
-- CALL sp_run_feature_engineering(CURDATE(), '20241231');

-- Example: Check feature completeness
-- SELECT * FROM v_feature_completeness;

-- Example: Check portfolio summary
-- SELECT * FROM v_portfolio_summary WHERE feature_date = CURDATE();

-- =====================================================================
-- END OF FEATURE ENGINEERING SCRIPT
-- =====================================================================