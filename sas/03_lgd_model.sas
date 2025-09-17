/*****************************************************************************
 * Program: 03_lgd_model.sas
 * Purpose: Develop Loss Given Default (LGD) model for IFRS 9 ECL
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Analyzes historical recovery patterns
 * 2. Develops LGD models by product type
 * 3. Implements Downturn LGD adjustments
 * 4. Creates Expected/Best/Worst case LGD estimates
 * 5. Incorporates collateral valuation models
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

%include "&project_path/sas/00_setup.sas";

/* Load PD model results */
data work.model_data_lgd;
    merge processed.model_dataset(in=a)
          models.pd_results(in=b keep=loan_id pd_12m_pit pd_lifetime);
    by loan_id;
    if a;
run;

/* Filter to defaulted loans for LGD modeling */
data work.defaulted_loans;
    set work.model_data_lgd;
    where target_default_12m = 1 or stage_ifrs9 = 3;
    
    /* Calculate time since default (simplified) */
    if stage_ifrs9 = 3 then months_since_default = min(current_dpd / 30, 24);
    else months_since_default = 0;
run;

/* =========================================================================
   PART 2: HISTORICAL RECOVERY ANALYSIS
   ========================================================================= */

/* Analyze recovery patterns by product type */
proc sql;
    create table work.recovery_stats as
    select 
        product_type,
        count(*) as n_defaults,
        mean(target_lgd) as avg_lgd,
        std(target_lgd) as std_lgd,
        min(target_lgd) as min_lgd,
        median(target_lgd) as median_lgd,
        max(target_lgd) as max_lgd,
        
        /* Recovery rate = 1 - LGD */
        mean(1 - target_lgd) as avg_recovery_rate,
        
        /* By collateral level */
        mean(case when loan_to_value <= 0.6 then target_lgd else . end) as lgd_low_ltv,
        mean(case when loan_to_value > 0.6 and loan_to_value <= 0.8 
                 then target_lgd else . end) as lgd_med_ltv,
        mean(case when loan_to_value > 0.8 then target_lgd else . end) as lgd_high_ltv
        
    from work.defaulted_loans
    group by product_type;
quit;

proc print data=work.recovery_stats;
    title "Historical LGD Statistics by Product";
run;

/* Analyze recovery timeline (workout period) */
proc sql;
    create table work.recovery_timeline as
    select 
        product_type,
        months_since_default,
        count(*) as n_obs,
        mean(target_lgd) as avg_lgd,
        mean(1 - target_lgd) as recovery_rate
    from work.defaulted_loans
    where months_since_default > 0
    group by product_type, months_since_default
    order by product_type, months_since_default;
quit;

/* =========================================================================
   PART 3: LGD SEGMENTATION
   ========================================================================= */

data work.lgd_segments;
    set work.defaulted_loans;
    
    /* Create LGD segments based on key drivers */
    length lgd_segment $50;
    
    /* Secured products segmentation */
    if product_type in ('MORTGAGE','HELOC') then do;
        if loan_to_value <= 0.6 then lgd_segment = 'SECURED_LOW_LTV';
        else if loan_to_value <= 0.8 then lgd_segment = 'SECURED_MED_LTV';
        else if loan_to_value <= 0.95 then lgd_segment = 'SECURED_HIGH_LTV';
        else lgd_segment = 'SECURED_VERY_HIGH_LTV';
    end;
    
    /* Auto loans segmentation */
    else if product_type = 'AUTO_LOAN' then do;
        if loan_to_value <= 0.8 then lgd_segment = 'AUTO_LOW_LTV';
        else if loan_to_value <= 1.0 then lgd_segment = 'AUTO_MED_LTV';
        else lgd_segment = 'AUTO_NEGATIVE_EQUITY';
    end;
    
    /* Unsecured products segmentation */
    else if product_type in ('CREDIT_CARD','PERSONAL_LOAN') then do;
        if credit_score >= 700 then lgd_segment = 'UNSECURED_PRIME';
        else if credit_score >= 620 then lgd_segment = 'UNSECURED_NEAR_PRIME';
        else lgd_segment = 'UNSECURED_SUBPRIME';
    end;
    
    else lgd_segment = 'OTHER';
    
    /* Cure probability indicator */
    if current_dpd < 90 and stage_ifrs9 < 3 then cure_flag = 1;
    else cure_flag = 0;
run;

/* Calculate segment-level LGD */
proc sql;
    create table work.lgd_by_segment as
    select 
        lgd_segment,
        product_type,
        count(*) as n_obs,
        mean(target_lgd) as segment_lgd,
        std(target_lgd) as segment_lgd_std,
        min(target_lgd) as segment_lgd_min,
        max(target_lgd) as segment_lgd_max,
        mean(cure_flag) as cure_rate
    from work.lgd_segments
    group by lgd_segment, product_type
    order by product_type, lgd_segment;
quit;

/* =========================================================================
   PART 4: BETA REGRESSION MODEL FOR LGD
   ========================================================================= */

/* Transform LGD for beta regression (must be between 0 and 1, exclusive) */
data work.lgd_model_data;
    set work.defaulted_loans;
    
    /* Transform LGD to (0,1) interval */
    lgd_transformed = (target_lgd * 0.9998) + 0.0001;
    
    /* Log transform for beta regression */
    logit_lgd = log(lgd_transformed / (1 - lgd_transformed));
    
    /* Create dummy variables for categorical predictors */
    if product_type = 'MORTGAGE' then prod_mortgage = 1; else prod_mortgage = 0;
    if product_type = 'AUTO_LOAN' then prod_auto = 1; else prod_auto = 0;
    if product_type = 'CREDIT_CARD' then prod_cc = 1; else prod_cc = 0;
    if product_type = 'PERSONAL_LOAN' then prod_personal = 1; else prod_personal = 0;
    if product_type = 'HELOC' then prod_heloc = 1; else prod_heloc = 0;
    
    /* Province dummies for real estate related products */
    if province in ('ON','BC') then high_value_market = 1; else high_value_market = 0;
    if province in ('AB','SK') then oil_province = 1; else oil_province = 0;
run;

/* Beta regression using PROC NLMIXED */
proc nlmixed data=work.lgd_model_data;
    /* Define parameters */
    parms b0=0 b1=0 b2=0 b3=0 b4=0 b5=0 b6=0 b7=0 b8=0 b9=0 phi=1;
    
    /* Linear predictor */
    eta = b0 + 
          b1*loan_to_value + 
          b2*credit_score/100 +
          b3*prod_mortgage + 
          b4*prod_auto + 
          b5*prod_cc +
          b6*prod_heloc +
          b7*high_value_market +
          b8*oil_province +
          b9*log(max(months_since_default,1));
    
    /* Mean of beta distribution */
    mu = exp(eta) / (1 + exp(eta));
    
    /* Variance parameters */
    a = mu * phi;
    b = (1 - mu) * phi;
    
    /* Log likelihood for beta distribution */
    ll = lgamma(a+b) - lgamma(a) - lgamma(b) + 
         (a-1)*log(lgd_transformed) + (b-1)*log(1-lgd_transformed);
    
    /* Model specification */
    model lgd_transformed ~ general(ll);
    
    /* Output predictions */
    predict mu out=work.lgd_beta_pred;
    
    /* Store parameters */
    ods output ParameterEstimates=work.lgd_beta_params;
run;

/* Alternative: Tobit regression for bounded LGD */
proc qlim data=work.lgd_model_data;
    model target_lgd = loan_to_value credit_score 
                      prod_mortgage prod_auto prod_cc prod_heloc
                      high_value_market oil_province months_since_default;
    endogenous target_lgd ~ truncated(lb=0 ub=1);
    output out=work.lgd_tobit_pred predicted=lgd_pred_tobit;
run;

/* =========================================================================
   PART 5: SIMPLIFIED LGD MODEL BY SEGMENTS
   ========================================================================= */

/* Create lookup table for segment-based LGD */
proc sql;
    create table work.lgd_lookup as
    select distinct
        lgd_segment,
        product_type,
        
        /* Expected LGD (mean) */
        segment_lgd as lgd_expected,
        
        /* Best estimate LGD (median equivalent) */
        segment_lgd - 0.5*segment_lgd_std as lgd_best,
        
        /* Downturn LGD (conservative) */
        min(segment_lgd + 1.5*segment_lgd_std, 0.95) as lgd_downturn,
        
        /* Regulatory floor by product type */
        case product_type
            when 'MORTGAGE' then 0.10
            when 'HELOC' then 0.15
            when 'AUTO_LOAN' then 0.30
            when 'CREDIT_CARD' then 0.75
            when 'PERSONAL_LOAN' then 0.65
            else 0.50
        end as lgd_regulatory_floor
        
    from work.lgd_by_segment;
quit;

/* Apply floors and caps */
data work.lgd_lookup_final;
    set work.lgd_lookup;
    
    /* Apply regulatory floors */
    if lgd_expected < lgd_regulatory_floor then lgd_expected = lgd_regulatory_floor;
    if lgd_best < lgd_regulatory_floor * 0.8 then lgd_best = lgd_regulatory_floor * 0.8;
    if lgd_downturn < lgd_regulatory_floor * 1.2 then lgd_downturn = lgd_regulatory_floor * 1.2;
    
    /* Apply caps */
    if lgd_expected > 0.95 then lgd_expected = 0.95;
    if lgd_best > 0.85 then lgd_best = 0.85;
    if lgd_downturn > 1.00 then lgd_downturn = 1.00;
run;

/* =========================================================================
   PART 6: COLLATERAL VALUATION MODEL
   ========================================================================= */

/* Create collateral haircut model for secured products */
data work.collateral_model;
    set work.model_data_lgd;
    where product_type in ('MORTGAGE','HELOC','AUTO_LOAN');
    
    /* Property value indexation for mortgages */
    if product_type in ('MORTGAGE','HELOC') then do;
        /* Simplified house price index adjustment */
        select(province);
            when('ON') hpi_adjustment = 1.05; /* 5% appreciation */
            when('BC') hpi_adjustment = 1.03;
            when('AB') hpi_adjustment = 0.95; /* 5% depreciation */
            when('SK') hpi_adjustment = 0.93;
            otherwise hpi_adjustment = 1.00;
        end;
        
        /* Forced sale discount */
        if stage_ifrs9 = 3 then forced_sale_discount = 0.85;
        else forced_sale_discount = 0.95;
        
        /* Current collateral value */
        current_collateral_value = (original_amount / loan_to_value) * 
                                   hpi_adjustment * forced_sale_discount;
        
        /* Recovery amount */
        recovery_amount = min(original_amount, current_collateral_value * 0.95); /* 5% costs */
    end;
    
    /* Auto depreciation for auto loans */
    else if product_type = 'AUTO_LOAN' then do;
        /* Annual depreciation rate */
        depreciation_rate = 0.15;
        
        /* Current vehicle value */
        current_collateral_value = (original_amount / loan_to_value) * 
                                  (1 - depreciation_rate) ** years_on_book;
        
        /* Recovery amount with repo costs */
        recovery_amount = current_collateral_value * 0.70; /* 30% costs and discount */
    end;
    
    /* Implied LGD from collateral */
    if original_amount > 0 then
        collateral_implied_lgd = max(0, 1 - recovery_amount / original_amount);
    else collateral_implied_lgd = 0.5;
    
    /* Cap collateral-based LGD */
    if collateral_implied_lgd > 1 then collateral_implied_lgd = 1;
run;

/* =========================================================================
   PART 7: MACROECONOMIC OVERLAY FOR LGD
   ========================================================================= */

/* Create macro adjustments for LGD */
data work.lgd_macro_factors;
    set rawdata.macro_data;
    
    /* LGD multipliers based on economic conditions */
    
    /* Housing market impact on secured LGD - use HPI_Change_YoY */
    if hpi_change_yoy < -10 then lgd_housing_mult = 1.3;
    else if hpi_change_yoy < 0 then lgd_housing_mult = 1.15;
    else if hpi_change_yoy > 10 then lgd_housing_mult = 0.90;
    else lgd_housing_mult = 1.0;
    
    /* Unemployment impact on unsecured LGD */
    if unemployment_rate > 10 then lgd_unemploy_mult = 1.25;
    else if unemployment_rate > 8 then lgd_unemploy_mult = 1.15;
    else if unemployment_rate < 5 then lgd_unemploy_mult = 0.95;
    else lgd_unemploy_mult = 1.0;
    
    /* Interest rate impact on all LGD */
    if policy_rate > 5 then lgd_rate_mult = 1.1;
    else if policy_rate < 2 then lgd_rate_mult = 0.95;
    else lgd_rate_mult = 1.0;
    
    /* Combined scenario multiplier */
    select(scenario_type);
        when('baseline') lgd_scenario_mult = 1.0;
        when('adverse') lgd_scenario_mult = 1.15;
        when('severely_adverse') lgd_scenario_mult = 1.30;
        otherwise lgd_scenario_mult = 1.0;
    end;
    
    keep forecast_date scenario_type lgd_housing_mult lgd_unemploy_mult 
         lgd_rate_mult lgd_scenario_mult;
run;

/* =========================================================================
   PART 8: COMBINE ALL LGD COMPONENTS
   ========================================================================= */

/* Merge all LGD estimates */
proc sql;
    create table work.lgd_combined as
    select 
        a.loan_id,
        a.customer_id,
        a.product_type,
        a.stage_ifrs9,
        a.loan_to_value,
        a.province,
        a.target_default_12m,
        
        /* Segment-based LGD */
        coalesce(b.lgd_segment, 'OTHER') as lgd_segment,
        coalesce(c.lgd_expected, 0.50) as lgd_expected_seg,
        coalesce(c.lgd_best, 0.40) as lgd_best_seg,
        coalesce(c.lgd_downturn, 0.65) as lgd_downturn_seg,
        
        /* Collateral-based LGD (if applicable) */
        case 
            when a.product_type in ('MORTGAGE','HELOC','AUTO_LOAN') 
            then coalesce(d.collateral_implied_lgd, c.lgd_expected)
            else c.lgd_expected
        end as lgd_collateral_adj,
        
        /* Stage-based LGD adjustment */
        case a.stage_ifrs9
            when 1 then coalesce(c.lgd_expected, 0.50)
            when 2 then coalesce(c.lgd_expected * 1.1, 0.55)
            when 3 then coalesce(c.lgd_downturn, 0.70)
            else coalesce(c.lgd_expected, 0.50)
        end as lgd_stage_adj
        
    from work.model_data_lgd a
    left join work.lgd_segments b
        on a.loan_id = b.loan_id
    left join work.lgd_lookup_final c
        on b.lgd_segment = c.lgd_segment and b.product_type = c.product_type
    left join work.collateral_model d
        on a.loan_id = d.loan_id;
quit;

/* Apply final LGD model */
data models.lgd_results;
    set work.lgd_combined;
    
    /* Final LGD selection based on data availability and product type */
    if product_type in ('MORTGAGE','HELOC','AUTO_LOAN') then do;
        /* Use collateral-adjusted LGD for secured products */
        lgd_pit = lgd_collateral_adj;
        lgd_expected = lgd_collateral_adj;
        lgd_downturn = min(lgd_collateral_adj * 1.3, 0.95);
    end;
    else do;
        /* Use segment-based LGD for unsecured products */
        lgd_pit = lgd_expected_seg;
        lgd_expected = lgd_expected_seg;
        lgd_downturn = lgd_downturn_seg;
    end;
    
    /* Through-the-cycle LGD (blend PIT with long-run average) */
    lgd_ttc = lgd_pit * 0.7 + lgd_expected_seg * 0.3;
    
    /* Create LGD grades */
    if lgd_expected <= 0.10 then lgd_grade = 'A';
    else if lgd_expected <= 0.20 then lgd_grade = 'B';
    else if lgd_expected <= 0.35 then lgd_grade = 'C';
    else if lgd_expected <= 0.50 then lgd_grade = 'D';
    else if lgd_expected <= 0.70 then lgd_grade = 'E';
    else lgd_grade = 'F';
    
    /* Forward-looking LGD for different time horizons */
    lgd_12m = lgd_pit;
    lgd_lifetime = lgd_ttc;
    
    /* Keep only necessary variables */
    keep loan_id customer_id product_type stage_ifrs9
         lgd_segment lgd_grade
         lgd_pit lgd_ttc lgd_expected lgd_downturn
         lgd_12m lgd_lifetime;
run;

/* =========================================================================
   PART 9: LGD VALIDATION
   ========================================================================= */

/* Compare predicted vs actual LGD for defaulted loans */
proc sql;
    create table work.lgd_validation as
    select 
        a.loan_id,
        a.product_type,
        a.target_lgd as actual_lgd,
        b.lgd_expected as predicted_lgd,
        b.lgd_expected - a.target_lgd as prediction_error,
        abs(calculated prediction_error) as abs_error
    from work.defaulted_loans a
    inner join models.lgd_results b
        on a.loan_id = b.loan_id
    where a.target_lgd > 0;
quit;

/* Calculate validation metrics */
proc means data=work.lgd_validation mean std min p25 median p75 max;
    title "LGD Model Validation Statistics";
    var actual_lgd predicted_lgd prediction_error abs_error;
    class product_type;
run;

/* Mean Absolute Error by product */
proc sql;
    create table work.lgd_mae as
    select 
        product_type,
        count(*) as n_obs,
        mean(actual_lgd) as mean_actual_lgd,
        mean(predicted_lgd) as mean_predicted_lgd,
        mean(abs_error) as mae,
        sqrt(mean(prediction_error**2)) as rmse,
        
        /* Correlation between actual and predicted */
        (sum((actual_lgd - mean(actual_lgd)) * 
             (predicted_lgd - mean(predicted_lgd))) /
         (sqrt(sum((actual_lgd - mean(actual_lgd))**2)) *
          sqrt(sum((predicted_lgd - mean(predicted_lgd))**2)))) as correlation
          
    from work.lgd_validation
    group by product_type;
quit;

/* =========================================================================
   PART 10: CREATE LGD TERM STRUCTURE WITH SCENARIOS
   ========================================================================= */

/* Merge with macro factors */
proc sql;
    create table models.lgd_with_scenarios as
    select 
        a.*,
        b.scenario_type,
        b.lgd_scenario_mult,
        
        /* Scenario-adjusted LGD */
        min(a.lgd_12m * b.lgd_scenario_mult, 0.99) as lgd_12m_scenario,
        min(a.lgd_lifetime * b.lgd_scenario_mult, 0.99) as lgd_lifetime_scenario,
        min(a.lgd_downturn * b.lgd_scenario_mult, 1.00) as lgd_downturn_scenario
        
    from models.lgd_results a
    cross join (select distinct scenario_type, lgd_scenario_mult 
                from work.lgd_macro_factors) b;
quit;

/* =========================================================================
   PART 11: REPORTING AND DOCUMENTATION
   ========================================================================= */

/* Create LGD summary report */
ods html file="&output_path/lgd_model_report_&run_date..html";
    
    title "LGD Model Summary Report";
    
    /* Distribution of LGD by product */
    proc means data=models.lgd_results n mean std min p25 median p75 max;
        title2 "LGD Distribution by Product Type";
        var lgd_pit lgd_ttc lgd_expected lgd_downturn;
        class product_type;
    run;
    
    /* LGD by stage */
    proc means data=models.lgd_results n mean std;
        title2 "LGD by IFRS 9 Stage";
        var lgd_expected;
        class stage_ifrs9;
    run;
    
    /* LGD grades distribution */
    proc freq data=models.lgd_results;
        title2 "LGD Grade Distribution";
        tables lgd_grade * product_type / nocol nopercent;
    run;
    
    /* Validation metrics */
    proc print data=work.lgd_mae;
        title2 "LGD Model Performance Metrics";
    run;
    
    /* Scenario impact */
    proc means data=models.lgd_with_scenarios mean;
        title2 "LGD under Different Scenarios";
        var lgd_12m_scenario lgd_lifetime_scenario;
        class scenario_type product_type;
    run;
    
ods html close;

/* Export LGD parameters for production */
proc export data=work.lgd_lookup_final
    outfile="&output_path/lgd_parameters_&run_date..csv"
    dbms=csv replace;
run;

%put NOTE: ====================================;
%put NOTE: LGD Model Development Completed;
%put NOTE: Output: models.lgd_results;
%put NOTE: Scenario output: models.lgd_with_scenarios;
%put NOTE: ====================================;

/* End of program */