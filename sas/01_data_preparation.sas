/*****************************************************************************
 * Program: 01_data_preparation.sas
 * Purpose: Data preparation and feature engineering for IFRS 9 ECL Model
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Imports raw data from CSV files
 * 2. Performs data quality checks and cleansing
 * 3. Creates derived features for modeling
 * 4. Implements IFRS 9 staging logic
 * 5. Splits data into training/validation/test sets
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

/* Include setup program */
%include "&project_path/sas/00_setup.sas";

/* Set run parameters */
%let run_date = %sysfunc(today(), yymmddn8.);
%let snapshot_date = 20241231;  /* Portfolio snapshot date */
%let train_end_date = '31DEC2023'd;
%let valid_end_date = '30JUN2024'd;

/* =========================================================================
   PART 2: DATA IMPORT
   ========================================================================= */

/* Import loans data */
proc import datafile="&raw_path/loans_&snapshot_date..csv"
    out=rawdata.loans
    dbms=csv
    replace;
    getnames=yes;
    datarow=2;
run;

/* Import payment history */
proc import datafile="&raw_path/payment_history_&snapshot_date..csv"
    out=rawdata.payment_history
    dbms=csv
    replace;
    getnames=yes;
    datarow=2;
run;

/* Add calculated actual_amount since it's not in the raw data */
data rawdata.payment_history;
    set rawdata.payment_history;
    /* Assume actual payment = scheduled unless past due */
    if days_past_due = 0 then actual_amount = scheduled_amount;
    else if days_past_due <= 30 then actual_amount = scheduled_amount * 0.5;
    else if days_past_due <= 60 then actual_amount = scheduled_amount * 0.2;
    else actual_amount = 0;
run;

/* Import macro data */
proc import datafile="&raw_path/macro_data_&snapshot_date..csv"
    out=rawdata.macro_data_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=2;
run;

/* Standardize macro data field names */
data rawdata.macro_data;
    set rawdata.macro_data_raw;
    
    /* Rename fields to match our convention */
    rename 
        Unemployment_Rate = unemployment_rate
        Employment_Rate = employment_rate
        GDP_Ontario = gdp_ontario
        GDP_Growth_YoY = gdp_growth
        Policy_Rate = policy_rate
        Prime_Rate = prime_rate
        Mortgage_5Y_Rate = mortgage_rate
        USD_CAD = fx_rate
        HPI = house_price_index
        HPI_Change_YoY = hpi_change_yoy
        WTI_Price = oil_price_wti
        WCS_Price = oil_price_wcs
        Economic_Cycle_Score = economic_cycle_score
        Credit_Conditions = credit_conditions
        Housing_Risk_Score = housing_risk_score;
    
    /* Create forecast_date from index */
    if _N_ = 1 then forecast_date = input(VAR1, yymmdd10.);
    else forecast_date = intnx('month', input(VAR1, yymmdd10.), 0);
    format forecast_date date9.;
    
    /* Add scenario_type - all historical data is baseline */
    scenario_type = 'baseline';
    
    drop VAR1;
run;

/* Import stress test scenarios */
proc import datafile="&raw_path/stress_test_scenarios.csv"
    out=rawdata.stress_scenarios_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=2;
run;

/* Transform stress scenarios to long format */
data rawdata.stress_scenarios;
    set rawdata.stress_scenarios_raw;
    
    /* Standardize scenario names */
    length scenario_type $20;
    if _N_ = 1 then scenario_type = 'baseline';
    else if _N_ = 2 then scenario_type = 'adverse';
    else if _N_ = 3 then scenario_type = 'severely_adverse';
    
    /* Rename fields to match our convention */
    rename 
        Unemployment_Rate = unemployment_rate
        GDP_Growth_YoY = gdp_growth_stress
        Policy_Rate = policy_rate_stress
        HPI_Change_YoY = hpi_change_stress
        WTI_Price = oil_price_stress;
    
    /* Create scenario weights */
    if scenario_type = 'baseline' then scenario_weight = 0.60;
    else if scenario_type = 'adverse' then scenario_weight = 0.30;
    else scenario_weight = 0.10;
run;

/* =========================================================================
   PART 3: DATA QUALITY CHECKS
   ========================================================================= */

/* Check loans data quality */
%data_quality_check(
    data=rawdata.loans,
    key_var=loan_id,
    numeric_vars=original_amount interest_rate credit_score annual_income loan_to_value,
    char_vars=customer_id product_type province,
    date_vars=origination_date
);

/* Check payment history data quality */
%data_quality_check(
    data=rawdata.payment_history,
    key_var=loan_id payment_date,
    numeric_vars=scheduled_amount actual_amount days_past_due,
    date_vars=payment_date
);

/* Check for missing values in critical fields */
proc freq data=rawdata.loans;
    title "Missing Value Analysis - Loans";
    tables _numeric_ / missing;
    tables _character_ / missing;
run;

/* =========================================================================
   PART 4: DATA CLEANING AND STANDARDIZATION
   ========================================================================= */

data work.loans_clean;
    set rawdata.loans;
    
    /* Convert date variables */
    if not missing(origination_date) then do;
        orig_date = input(origination_date, yymmdd10.);
    end;
    format orig_date date9.;
    
    /* Standardize product types */
    product_type_std = propcase(compress(product_type));
    select(upcase(product_type_std));
        when('MORTGAGE','MORT','MTG') product_type_std = 'MORTGAGE';
        when('CREDITCARD','CREDIT CARD','CC','CARD') product_type_std = 'CREDIT_CARD';
        when('AUTO','AUTOLOAN','AUTO LOAN','CAR','VEHICLE') product_type_std = 'AUTO_LOAN';
        when('PERSONAL','PERSONALLOAN','PERSONAL LOAN','PERS','PL') product_type_std = 'PERSONAL_LOAN';
        when('HELOC','LOC','LINEOFCREDIT') product_type_std = 'HELOC';
        otherwise product_type_std = 'OTHER';
    end;
    
    /* Standardize provinces */
    province_std = upcase(province);
    if province_std not in ('AB','BC','MB','NB','NL','NS','NT','NU','ON','PE','QC','SK','YT') 
        then province_std = 'UNKNOWN';
    
    /* Handle missing values */
    if missing(credit_score) then credit_score = 650; /* Population median */
    if missing(annual_income) then annual_income = 65000; /* Population median */
    if missing(loan_to_value) then do;
        if product_type_std = 'MORTGAGE' then loan_to_value = 0.75;
        else loan_to_value = 0.5;
    end;
    
    /* Cap and floor extreme values */
    if credit_score < 300 then credit_score = 300;
    if credit_score > 900 then credit_score = 900;
    if loan_to_value < 0 then loan_to_value = 0;
    if loan_to_value > 1 then loan_to_value = 1;
    if interest_rate < 0 then interest_rate = 0.01;
    if interest_rate > 0.5 then interest_rate = 0.5; /* 50% cap */
    
    /* Create age variables */
    months_on_book = intck('month', orig_date, "&snapshot_date"d);
    years_on_book = months_on_book / 12;
    
    /* Create income bands */
    if annual_income < 30000 then income_band = '1_LOW';
    else if annual_income < 60000 then income_band = '2_MED_LOW';
    else if annual_income < 100000 then income_band = '3_MEDIUM';
    else if annual_income < 150000 then income_band = '4_MED_HIGH';
    else income_band = '5_HIGH';
    
    /* Create credit score bands */
    if credit_score < 600 then credit_band = '1_POOR';
    else if credit_score < 650 then credit_band = '2_FAIR';
    else if credit_score < 700 then credit_band = '3_GOOD';
    else if credit_score < 750 then credit_band = '4_VERY_GOOD';
    else credit_band = '5_EXCELLENT';
    
    /* Create LTV bands */
    if loan_to_value <= 0.6 then ltv_band = '1_LOW';
    else if loan_to_value <= 0.8 then ltv_band = '2_MEDIUM';
    else if loan_to_value <= 0.95 then ltv_band = '3_HIGH';
    else ltv_band = '4_VERY_HIGH';
    
    drop origination_date product_type province;
    rename product_type_std = product_type
           province_std = province;
run;

/* =========================================================================
   PART 5: PAYMENT BEHAVIOR FEATURES
   ========================================================================= */

/* Calculate payment behavior metrics */
proc sql;
    create table work.payment_features as
    select 
        loan_id,
        /* Current DPD */
        max(days_past_due) as current_dpd,
        
        /* Historical DPD metrics */
        max(days_past_due) as max_dpd_ever,
        avg(days_past_due) as avg_dpd,
        std(days_past_due) as std_dpd,
        
        /* Payment patterns */
        sum(case when days_past_due > 0 then 1 else 0 end) as count_dpd_positive,
        sum(case when days_past_due > 30 then 1 else 0 end) as count_dpd_30plus,
        sum(case when days_past_due > 60 then 1 else 0 end) as count_dpd_60plus,
        sum(case when days_past_due > 90 then 1 else 0 end) as count_dpd_90plus,
        
        /* Payment performance (based on DPD patterns) */
        case 
            when max(days_past_due) = 0 then 1.0  /* Perfect payment */
            when max(days_past_due) <= 30 then 0.8 /* Minor delinquency */
            when max(days_past_due) <= 60 then 0.5 /* Moderate delinquency */
            else 0.2 /* Severe delinquency */
        end as payment_performance_ratio,
        
        /* Count of payments made */
        count(*) as total_payments,
        
        /* Recent behavior (last 12 months) */
        max(case when payment_date >= intnx('month', "&snapshot_date"d, -12) 
                then days_past_due else 0 end) as max_dpd_12m,
        avg(case when payment_date >= intnx('month', "&snapshot_date"d, -12) 
                then days_past_due else . end) as avg_dpd_12m,
        
        /* Recent behavior (last 6 months) */
        max(case when payment_date >= intnx('month', "&snapshot_date"d, -6) 
                then days_past_due else 0 end) as max_dpd_6m,
        avg(case when payment_date >= intnx('month', "&snapshot_date"d, -6) 
                then days_past_due else . end) as avg_dpd_6m,
        
        /* Recent behavior (last 3 months) */
        max(case when payment_date >= intnx('month', "&snapshot_date"d, -3) 
                then days_past_due else 0 end) as max_dpd_3m,
        avg(case when payment_date >= intnx('month', "&snapshot_date"d, -3) 
                then days_past_due else . end) as avg_dpd_3m
                
    from rawdata.payment_history
    group by loan_id;
quit;

/* =========================================================================
   PART 6: MERGE FEATURES AND CREATE ANALYTICAL DATASET
   ========================================================================= */

/* Merge all features */
data work.model_dataset;
    merge work.loans_clean(in=a)
          work.payment_features(in=b);
    by loan_id;
    if a; /* Keep all loans even if no payment history */
    
    /* Handle missing payment features (new loans) */
    array pay_vars{*} current_dpd max_dpd_ever avg_dpd std_dpd 
                      count_dpd_positive count_dpd_30plus count_dpd_60plus count_dpd_90plus
                      payment_performance_ratio total_payments
                      max_dpd_12m avg_dpd_12m max_dpd_6m avg_dpd_6m max_dpd_3m avg_dpd_3m;
    
    do i = 1 to dim(pay_vars);
        if missing(pay_vars{i}) then pay_vars{i} = 0;
    end;
    
    /* Set default payment performance for new loans */
    if missing(payment_performance_ratio) or total_payments = 0 then 
        payment_performance_ratio = 1.0;
    
    /* Create interaction features */
    credit_income_ratio = credit_score / (annual_income / 1000);
    ltv_credit_interaction = loan_to_value * credit_score;
    payment_burden = (original_amount * interest_rate / 12) / (annual_income / 12);
    
    /* Create polynomial features for key variables */
    credit_score_sq = credit_score ** 2;
    ltv_sq = loan_to_value ** 2;
    income_log = log(max(annual_income, 1));
    
    /* Risk flags */
    if max_dpd_ever >= 90 then flag_ever_90dpd = 1; else flag_ever_90dpd = 0;
    if max_dpd_12m >= 60 then flag_recent_delinquent = 1; else flag_recent_delinquent = 0;
    if payment_burden > 0.5 then flag_high_burden = 1; else flag_high_burden = 0;
    if loan_to_value > 0.95 then flag_high_ltv = 1; else flag_high_ltv = 0;
    if credit_score < 620 then flag_subprime = 1; else flag_subprime = 0;
    
    drop i;
run;

/* =========================================================================
   PART 7: IMPLEMENT IFRS 9 STAGING
   ========================================================================= */

data work.model_dataset_staged;
    set work.model_dataset;
    
    /* Calculate Probability of Default proxy (simplified) */
    pd_proxy = 1 / (1 + exp(5 - 0.01*credit_score + 2*loan_to_value + 0.05*current_dpd));
    
    /* Initial PD at origination (simplified) */
    pd_initial = 1 / (1 + exp(5 - 0.01*credit_score + 2*loan_to_value));
    
    /* SICR (Significant Increase in Credit Risk) */
    if pd_proxy > 2 * pd_initial then sicr_flag = 1;
    else sicr_flag = 0;
    
    /* IFRS 9 Staging Logic */
    if current_dpd >= 90 or default_flag = 1 then stage_ifrs9 = 3;
    else if current_dpd >= 30 or sicr_flag = 1 or max_dpd_12m >= 60 then stage_ifrs9 = 2;
    else stage_ifrs9 = 1;
    
    /* Create stage description */
    length stage_desc $50;
    select(stage_ifrs9);
        when(1) stage_desc = 'Stage 1 - Performing';
        when(2) stage_desc = 'Stage 2 - Under-performing';
        when(3) stage_desc = 'Stage 3 - Non-performing';
        otherwise stage_desc = 'Unknown';
    end;
    
    /* Risk segmentation for modeling */
    length risk_segment $30;
    if stage_ifrs9 = 3 then risk_segment = 'DEFAULT';
    else if credit_score < 620 then risk_segment = 'SUBPRIME';
    else if credit_score < 680 then risk_segment = 'NEAR_PRIME';
    else if credit_score < 740 then risk_segment = 'PRIME';
    else risk_segment = 'SUPER_PRIME';
    
    /* Product-specific risk indicators */
    select(product_type);
        when('MORTGAGE') do;
            if loan_to_value > 0.8 and province in ('ON','BC') then 
                flag_high_risk_mortgage = 1;
            else flag_high_risk_mortgage = 0;
        end;
        when('CREDIT_CARD') do;
            /* Use payment performance ratio instead of avg_payment_ratio */
            if payment_performance_ratio < 0.5 then flag_min_payer = 1;
            else flag_min_payer = 0;
        end;
        when('AUTO_LOAN') do;
            if loan_to_value > 1.0 then flag_negative_equity = 1;
            else flag_negative_equity = 0;
        end;
        otherwise do;
            flag_high_risk_mortgage = 0;
            flag_min_payer = 0;
            flag_negative_equity = 0;
        end;
    end;
run;

/* =========================================================================
   PART 8: CREATE MODELING TARGET VARIABLES
   ========================================================================= */

data work.model_dataset_final;
    set work.model_dataset_staged;
    
    /* Binary default target (for PD model) */
    if default_flag = 1 or stage_ifrs9 = 3 then target_default_12m = 1;
    else target_default_12m = 0;
    
    /* Forward-looking default (requires historical data - simplified) */
    if max_dpd_ever >= 90 then target_default_ever = 1;
    else target_default_ever = 0;
    
    /* LGD target (simplified - would need recovery data) */
    if target_default_12m = 1 then do;
        /* Simplified LGD based on product type and collateral */
        select(product_type);
            when('MORTGAGE') target_lgd = 0.10 + 0.30 * (loan_to_value - 0.8);
            when('AUTO_LOAN') target_lgd = 0.30 + 0.40 * (loan_to_value - 0.8);
            when('CREDIT_CARD') target_lgd = 0.85;
            when('PERSONAL_LOAN') target_lgd = 0.75;
            when('HELOC') target_lgd = 0.15 + 0.35 * (loan_to_value - 0.65);
            otherwise target_lgd = 0.65;
        end;
        /* Cap LGD between 0 and 1 */
        if target_lgd < 0 then target_lgd = 0;
        if target_lgd > 1 then target_lgd = 1;
    end;
    else target_lgd = 0;
    
    /* EAD target (simplified) */
    if product_type in ('CREDIT_CARD','HELOC') then do;
        /* For revolving credit, EAD includes undrawn amounts */
        target_ead = original_amount * 1.1; /* Simplified CCF of 1.1 */
    end;
    else do;
        /* For term loans, EAD is outstanding balance */
        target_ead = original_amount * max(0, (1 - years_on_book/5)); /* Simple amortization */
    end;
    
    /* Create observation weight for imbalanced classes */
    if target_default_12m = 1 then weight = 10;
    else weight = 1;
run;

/* =========================================================================
   PART 9: TRAIN/VALIDATION/TEST SPLIT
   ========================================================================= */

/* Sort by origination date for time-based split */
proc sort data=work.model_dataset_final;
    by orig_date;
run;

/* Create train/validation/test indicators */
data processed.model_dataset;
    set work.model_dataset_final;
    
    /* Time-based split */
    if orig_date <= &train_end_date then data_partition = 'TRAIN';
    else if orig_date <= &valid_end_date then data_partition = 'VALIDATE';
    else data_partition = 'TEST';
    
    /* Add random number for alternative splitting */
    call streaminit(12345);
    random_num = rand('uniform');
    
    /* Alternative: Random split (commented out - use time-based by default) */
    /*
    if random_num < 0.6 then data_partition = 'TRAIN';
    else if random_num < 0.8 then data_partition = 'VALIDATE';
    else data_partition = 'TEST';
    */
    
    /* Create stratification variable */
    strata = cats(product_type, '_', stage_ifrs9, '_', risk_segment);
run;

/* =========================================================================
   PART 10: CREATE SUMMARY STATISTICS
   ========================================================================= */

/* Overall portfolio summary */
proc freq data=processed.model_dataset;
    title "Portfolio Distribution";
    tables product_type * stage_ifrs9 / nocol nopercent;
    tables province * risk_segment / nocol nopercent;
    tables data_partition * target_default_12m / nocol nopercent;
run;

/* Numeric variable distributions */
proc means data=processed.model_dataset n nmiss mean std min p25 p50 p75 max;
    title "Numeric Variable Summary";
    var credit_score annual_income loan_to_value interest_rate original_amount
        current_dpd max_dpd_ever months_on_book;
    class stage_ifrs9;
run;

/* Default rate by segments */
proc sql;
    title "Default Rates by Segment";
    create table work.default_rates as
    select 
        product_type,
        stage_ifrs9,
        risk_segment,
        count(*) as n_obs,
        sum(target_default_12m) as n_defaults,
        mean(target_default_12m) as default_rate format=percent8.2
    from processed.model_dataset
    group by product_type, stage_ifrs9, risk_segment
    order by product_type, stage_ifrs9, risk_segment;
quit;

proc print data=work.default_rates;
run;

/* =========================================================================
   PART 11: EXPORT PREPARED DATA
   ========================================================================= */

/* Export to CSV for SQL database load */
proc export data=processed.model_dataset
    outfile="&processed_path/model_dataset_&run_date..csv"
    dbms=csv
    replace;
run;

/* Export summary statistics */
ods html file="&output_path/data_preparation_summary_&run_date..html";
    title "Data Preparation Summary Report";
    
    proc freq data=processed.model_dataset;
        tables stage_ifrs9 product_type risk_segment data_partition;
    run;
    
    proc means data=processed.model_dataset;
        var target_default_12m target_lgd target_ead;
        class stage_ifrs9;
    run;
    
    proc corr data=processed.model_dataset;
        var credit_score loan_to_value current_dpd payment_burden;
        with target_default_12m;
    run;
ods html close;

/* Create metadata table */
data processed.variable_metadata;
    length variable_name $32 variable_type $10 description $200 source $50;
    infile datalines delimiter='|' truncover;
    input variable_name $ variable_type $ description $ source $;
    datalines;
loan_id|KEY|Unique loan identifier|loans
customer_id|KEY|Unique customer identifier|loans
orig_date|DATE|Loan origination date|loans
product_type|CHAR|Standardized product type|loans
province|CHAR|Canadian province code|loans
original_amount|NUM|Original loan amount|loans
interest_rate|NUM|Annual interest rate|loans
credit_score|NUM|Credit bureau score at origination|loans
annual_income|NUM|Annual income at origination|loans
loan_to_value|NUM|Loan to value ratio|loans
default_flag|NUM|Historical default indicator|loans
current_dpd|NUM|Current days past due|payment_history
max_dpd_ever|NUM|Maximum DPD ever observed|payment_history
max_dpd_12m|NUM|Maximum DPD in last 12 months|payment_history
payment_performance_ratio|NUM|Payment performance indicator|payment_history
total_payments|NUM|Count of payments made|payment_history
stage_ifrs9|NUM|IFRS 9 stage (1/2/3)|derived
risk_segment|CHAR|Risk segmentation|derived
target_default_12m|NUM|12-month default target|derived
target_lgd|NUM|Loss given default target|derived
target_ead|NUM|Exposure at default target|derived
data_partition|CHAR|Train/Validate/Test indicator|derived
;
run;

/* =========================================================================
   PART 12: LOG COMPLETION
   ========================================================================= */

%put NOTE: ====================================;
%put NOTE: Data Preparation Completed Successfully;
%put NOTE: Output dataset: processed.model_dataset;
%put NOTE: Number of observations: %obs(processed.model_dataset);
%put NOTE: Run date: &run_date;
%put NOTE: ====================================;

/* End of program */