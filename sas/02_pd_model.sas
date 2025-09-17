/*****************************************************************************
 * Program: 02_pd_model.sas
 * Purpose: Develop Probability of Default (PD) model for IFRS 9 ECL
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Performs univariate analysis and variable selection
 * 2. Develops logistic regression PD model
 * 3. Creates PD term structure (12-month to lifetime)
 * 4. Incorporates macroeconomic adjustments
 * 5. Validates model performance
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

%include "&project_path/sas/00_setup.sas";

/* Set modeling parameters */
%let target_var = target_default_12m;
%let key_var = loan_id;
%let partition_var = data_partition;
%let weight_var = weight;

/* Load prepared dataset */
data work.model_data;
    set processed.model_dataset;
    where data_partition in ('TRAIN','VALIDATE');
run;

/* =========================================================================
   PART 2: VARIABLE SELECTION - UNIVARIATE ANALYSIS
   ========================================================================= */

/* Define candidate variables */
%let numeric_vars = credit_score loan_to_value interest_rate payment_burden
                    current_dpd max_dpd_12m max_dpd_6m max_dpd_3m
                    payment_performance_ratio months_on_book
                    credit_income_ratio ltv_credit_interaction
                    credit_score_sq ltv_sq income_log;

%let categorical_vars = product_type province income_band credit_band ltv_band
                       risk_segment stage_desc
                       flag_ever_90dpd flag_recent_delinquent flag_high_burden
                       flag_high_ltv flag_subprime;

/* Calculate Information Value for numeric variables */
%macro calculate_iv_numeric(vars=);
    %let n_vars = %sysfunc(countw(&vars));
    
    %do i = 1 %to &n_vars;
        %let var = %scan(&vars, &i);
        
        /* Create binned version */
        proc rank data=work.model_data(where=(data_partition='TRAIN'))
                  out=work.temp_rank groups=10;
            var &var;
            ranks &var._rank;
        run;
        
        /* Calculate WOE and IV */
        %calculate_woe_iv(
            data=work.temp_rank,
            var=&var._rank,
            target=&target_var,
            weight=&weight_var
        );
        
        /* Store results */
        data work.iv_&var;
            set work.woe_output;
            variable = "&var";
        run;
    %end;
    
    /* Combine all IV results */
    data work.iv_numeric_all;
        set 
        %do i = 1 %to &n_vars;
            %let var = %scan(&vars, &i);
            work.iv_&var
        %end;
        ;
    run;
%mend;

%calculate_iv_numeric(vars=&numeric_vars);

/* Calculate IV for categorical variables */
%macro calculate_iv_categorical(vars=);
    %let n_vars = %sysfunc(countw(&vars));
    
    %do i = 1 %to &n_vars;
        %let var = %scan(&vars, &i);
        
        %calculate_woe_iv(
            data=work.model_data(where=(data_partition='TRAIN')),
            var=&var,
            target=&target_var,
            weight=&weight_var
        );
        
        /* Store results */
        data work.iv_&var;
            set work.woe_output;
            variable = "&var";
        run;
    %end;
    
    /* Combine all IV results */
    data work.iv_categorical_all;
        set 
        %do i = 1 %to &n_vars;
            %let var = %scan(&vars, &i);
            work.iv_&var
        %end;
        ;
    run;
%mend;

%calculate_iv_categorical(vars=&categorical_vars);

/* Summarize IV results */
proc sql;
    create table work.variable_importance as
    select distinct
        variable,
        sum(iv_contribution) as total_iv,
        case 
            when calculated total_iv < 0.02 then 'Useless'
            when calculated total_iv < 0.1 then 'Weak'
            when calculated total_iv < 0.3 then 'Medium'
            when calculated total_iv < 0.5 then 'Strong'
            else 'Suspicious'
        end as iv_category
    from (
        select variable, iv_contribution from work.iv_numeric_all
        union all
        select variable, iv_contribution from work.iv_categorical_all
    )
    group by variable
    order by total_iv desc;
quit;

proc print data=work.variable_importance(obs=20);
    title "Top 20 Variables by Information Value";
run;

/* =========================================================================
   PART 3: CORRELATION ANALYSIS AND MULTICOLLINEARITY CHECK
   ========================================================================= */

/* Check correlation among numeric variables */
proc corr data=work.model_data(where=(data_partition='TRAIN')) 
          outp=work.correlation_matrix noprint;
    var &numeric_vars;
run;

/* Identify highly correlated pairs */
proc sql;
    create table work.high_correlation as
    select a._NAME_ as var1,
           b._NAME_ as var2,
           a.&target_var as correlation
    from work.correlation_matrix a
    join work.correlation_matrix b
    on a._NAME_ < b._NAME_
    where a._TYPE_ = 'CORR' and b._TYPE_ = 'CORR'
    and abs(a.&target_var) > 0.7
    order by abs(correlation) desc;
quit;

/* =========================================================================
   PART 4: FEATURE ENGINEERING FOR PD MODEL
   ========================================================================= */

data work.model_data_pd;
    set work.model_data;
    
    /* Create WOE transformed variables for selected high-IV variables */
    /* Note: In production, use actual WOE values from analysis */
    
    /* Credit score WOE */
    if credit_score < 600 then woe_credit = -1.5;
    else if credit_score < 650 then woe_credit = -0.8;
    else if credit_score < 700 then woe_credit = -0.2;
    else if credit_score < 750 then woe_credit = 0.3;
    else woe_credit = 0.8;
    
    /* Current DPD WOE */
    if current_dpd = 0 then woe_dpd = 0.5;
    else if current_dpd <= 30 then woe_dpd = -0.3;
    else if current_dpd <= 60 then woe_dpd = -1.2;
    else woe_dpd = -2.5;
    
    /* LTV WOE */
    if loan_to_value <= 0.6 then woe_ltv = 0.4;
    else if loan_to_value <= 0.8 then woe_ltv = 0.1;
    else if loan_to_value <= 0.95 then woe_ltv = -0.4;
    else woe_ltv = -1.0;
    
    /* Payment burden WOE */
    if payment_burden <= 0.2 then woe_burden = 0.3;
    else if payment_burden <= 0.4 then woe_burden = -0.1;
    else if payment_burden <= 0.6 then woe_burden = -0.5;
    else woe_burden = -1.2;
    
    /* Product type scoring */
    select(product_type);
        when('MORTGAGE') prod_score = 0.2;
        when('AUTO_LOAN') prod_score = 0.0;
        when('PERSONAL_LOAN') prod_score = -0.3;
        when('CREDIT_CARD') prod_score = -0.5;
        when('HELOC') prod_score = 0.1;
        otherwise prod_score = -0.2;
    end;
    
    /* Province risk adjustment */
    select(province);
        when('AB','SK') prov_risk = -0.2;  /* Oil provinces - higher risk */
        when('ON','BC') prov_risk = 0.0;   /* Large markets - neutral */
        when('QC') prov_risk = 0.1;        /* Quebec - slightly lower risk */
        otherwise prov_risk = 0.0;
    end;
run;

/* =========================================================================
   PART 5: BUILD LOGISTIC REGRESSION MODEL
   ========================================================================= */

/* Split features for modeling */
%let model_features = woe_credit woe_dpd woe_ltv woe_burden 
                     prod_score prov_risk
                     months_on_book flag_recent_delinquent;

/* Initial model with all features */
proc logistic data=work.model_data_pd(where=(data_partition='TRAIN'))
              outmodel=models.pd_model_full;
    class flag_recent_delinquent / param=ref;
    model &target_var(event='1') = &model_features / 
          selection=stepwise
          slentry=0.05
          slstay=0.05
          lackfit
          rsquare
          details;
    weight &weight_var;
    output out=work.train_scored_full
           p=pd_score_full
           xbeta=logit_score;
    
    /* Store model statistics */
    ods output Association=work.model_stats_full
               ParameterEstimates=work.model_params_full
               LackFitChiSq=work.lackfit_full;
run;

/* Score validation set */
proc logistic inmodel=models.pd_model_full;
    score data=work.model_data_pd(where=(data_partition='VALIDATE'))
          out=work.valid_scored_full;
run;

/* =========================================================================
   PART 6: MODEL CALIBRATION AND SCALING
   ========================================================================= */

/* Calculate actual default rates by score band for calibration */
proc rank data=work.train_scored_full groups=20 out=work.train_ranked;
    var pd_score_full;
    ranks score_rank;
run;

proc sql;
    create table work.calibration_table as
    select 
        score_rank,
        count(*) as n_obs,
        sum(&target_var) as n_defaults,
        mean(&target_var) as actual_dr,
        mean(pd_score_full) as predicted_dr,
        mean(pd_score_full) / mean(&target_var) as calibration_ratio
    from work.train_ranked
    group by score_rank
    order by score_rank;
quit;

/* Calculate calibration factor */
proc sql noprint;
    select mean(&target_var) / mean(pd_score_full) into :calib_factor
    from work.train_scored_full;
quit;

%put Calibration Factor: &calib_factor;

/* Apply calibration */
data work.model_data_calibrated;
    set work.train_scored_full
        work.valid_scored_full;
    
    /* Calibrated PD */
    pd_calibrated = pd_score_full * &calib_factor;
    
    /* Cap PD between 0.0001 and 0.9999 */
    if pd_calibrated < 0.0001 then pd_calibrated = 0.0001;
    if pd_calibrated > 0.9999 then pd_calibrated = 0.9999;
    
    /* Create PD master scale (1-20) */
    if pd_calibrated <= 0.001 then pd_rating = 1;
    else if pd_calibrated <= 0.002 then pd_rating = 2;
    else if pd_calibrated <= 0.003 then pd_rating = 3;
    else if pd_calibrated <= 0.005 then pd_rating = 4;
    else if pd_calibrated <= 0.007 then pd_rating = 5;
    else if pd_calibrated <= 0.010 then pd_rating = 6;
    else if pd_calibrated <= 0.015 then pd_rating = 7;
    else if pd_calibrated <= 0.020 then pd_rating = 8;
    else if pd_calibrated <= 0.030 then pd_rating = 9;
    else if pd_calibrated <= 0.045 then pd_rating = 10;
    else if pd_calibrated <= 0.065 then pd_rating = 11;
    else if pd_calibrated <= 0.090 then pd_rating = 12;
    else if pd_calibrated <= 0.130 then pd_rating = 13;
    else if pd_calibrated <= 0.180 then pd_rating = 14;
    else if pd_calibrated <= 0.250 then pd_rating = 15;
    else if pd_calibrated <= 0.350 then pd_rating = 16;
    else if pd_calibrated <= 0.500 then pd_rating = 17;
    else if pd_calibrated <= 0.700 then pd_rating = 18;
    else if pd_calibrated <= 0.900 then pd_rating = 19;
    else pd_rating = 20;
run;

/* =========================================================================
   PART 7: PD TERM STRUCTURE
   ========================================================================= */

/* Create PD term structure (1-5 years) */
data work.pd_term_structure;
    set work.model_data_calibrated;
    
    /* 12-month PD (Point-in-Time) */
    pd_12m_pit = pd_calibrated;
    
    /* Through-the-Cycle (TTC) adjustment */
    pd_12m_ttc = pd_12m_pit * 0.8 + 0.05 * 0.2; /* Blend with long-term average */
    
    /* Multi-year cumulative PD using survival analysis approach */
    /* Assume constant hazard rate for simplification */
    hazard_rate = -log(1 - pd_12m_ttc);
    
    /* Marginal PDs with maturity adjustment */
    pd_year1 = pd_12m_ttc;
    pd_year2 = (1 - pd_year1) * (pd_12m_ttc * 0.95); /* 5% improvement */
    pd_year3 = (1 - pd_year1 - pd_year2) * (pd_12m_ttc * 0.90);
    pd_year4 = (1 - pd_year1 - pd_year2 - pd_year3) * (pd_12m_ttc * 0.85);
    pd_year5 = (1 - pd_year1 - pd_year2 - pd_year3 - pd_year4) * (pd_12m_ttc * 0.80);
    
    /* Cumulative PDs */
    pd_cum_1y = pd_year1;
    pd_cum_2y = pd_year1 + pd_year2;
    pd_cum_3y = pd_cum_2y + pd_year3;
    pd_cum_4y = pd_cum_3y + pd_year4;
    pd_cum_5y = pd_cum_4y + pd_year5;
    
    /* Lifetime PD (simplified - extend to maturity) */
    select(product_type);
        when('MORTGAGE') remaining_term = max(1, 25 - years_on_book);
        when('AUTO_LOAN') remaining_term = max(1, 7 - years_on_book);
        when('PERSONAL_LOAN') remaining_term = max(1, 5 - years_on_book);
        when('CREDIT_CARD','HELOC') remaining_term = 10; /* Behavioral maturity */
        otherwise remaining_term = 5;
    end;
    
    /* Lifetime PD calculation */
    if remaining_term <= 5 then do;
        if remaining_term <= 1 then pd_lifetime = pd_cum_1y;
        else if remaining_term <= 2 then pd_lifetime = pd_cum_2y;
        else if remaining_term <= 3 then pd_lifetime = pd_cum_3y;
        else if remaining_term <= 4 then pd_lifetime = pd_cum_4y;
        else pd_lifetime = pd_cum_5y;
    end;
    else do;
        /* Extrapolate using exponential decay */
        pd_lifetime = 1 - exp(-hazard_rate * remaining_term);
    end;
    
    /* Cap lifetime PD */
    if pd_lifetime > 0.9999 then pd_lifetime = 0.9999;
run;

/* =========================================================================
   PART 8: MACROECONOMIC ADJUSTMENT
   ========================================================================= */

/* Load macro scenarios */
data work.macro_scenarios;
    set rawdata.macro_data;
    where scenario_type in ('baseline','adverse','severely_adverse');
    
    /* Use actual field names from the data */
    keep forecast_date scenario_type 
         unemployment_rate gdp_growth policy_rate house_price_index;
run;

/* Create macro adjustment factors */
proc sql;
    create table work.macro_adjustments as
    select distinct
        scenario_type,
        /* Simplified macro adjustment based on key indicators */
        case scenario_type
            when 'baseline' then 1.0
            when 'adverse' then 1.3
            when 'severely_adverse' then 1.8
            else 1.0
        end as pd_multiplier,
        
        /* GDP growth impact */
        case 
            when gdp_growth < -2 then 1.5
            when gdp_growth < 0 then 1.2
            when gdp_growth < 2 then 1.0
            else 0.9
        end as gdp_adjustment,
        
        /* Unemployment impact */
        case
            when unemployment_rate > 10 then 1.4
            when unemployment_rate > 8 then 1.2
            when unemployment_rate > 6 then 1.0
            else 0.95
        end as unemployment_adjustment,
        
        /* Interest rate impact */
        case
            when policy_rate > 5 then 1.15
            when policy_rate > 3 then 1.0
            else 0.95
        end as interest_adjustment
        
    from work.macro_scenarios;
quit;

/* Apply macro adjustments to PD */
proc sql;
    create table work.pd_with_macro as
    select 
        a.*,
        b.scenario_type,
        b.pd_multiplier,
        b.gdp_adjustment,
        b.unemployment_adjustment,
        b.interest_adjustment,
        
        /* Baseline scenario PDs */
        a.pd_12m_pit as pd_12m_baseline,
        a.pd_lifetime as pd_lifetime_baseline,
        
        /* Adjusted PDs for different scenarios */
        a.pd_12m_pit * b.pd_multiplier * b.gdp_adjustment * 
            b.unemployment_adjustment * b.interest_adjustment as pd_12m_scenario,
        
        a.pd_lifetime * b.pd_multiplier * b.gdp_adjustment * 
            b.unemployment_adjustment * b.interest_adjustment as pd_lifetime_scenario
        
    from work.pd_term_structure a
    cross join work.macro_adjustments b;
quit;

/* =========================================================================
   PART 9: MODEL VALIDATION
   ========================================================================= */

/* Calculate model performance metrics */
%model_validation(
    data=work.model_data_calibrated,
    target=&target_var,
    prediction=pd_calibrated,
    partition=&partition_var,
    weight=&weight_var
);

/* ROC Curve and AUC */
proc logistic data=work.model_data_calibrated;
    model &target_var(event='1') = pd_calibrated;
    roc 'PD Model' pd_calibrated;
    roccontrast;
    ods output ROCAssociation=work.roc_stats;
run;

/* Gini coefficient */
proc sql noprint;
    select 2*Area-1 into :gini_coef
    from work.roc_stats
    where ROCModel = 'PD Model';
quit;

%put Gini Coefficient: &gini_coef;

/* Kolmogorov-Smirnov (KS) Statistic */
proc npar1way data=work.model_data_calibrated edf;
    class &target_var;
    var pd_calibrated;
    ods output KolSmir2Stats=work.ks_stats;
run;

/* Population Stability Index (PSI) between train and validate */
%calculate_psi(
    base_data=work.model_data_calibrated(where=(data_partition='TRAIN')),
    compare_data=work.model_data_calibrated(where=(data_partition='VALIDATE')),
    score_var=pd_calibrated,
    n_bins=10
);

/* =========================================================================
   PART 10: SAVE RESULTS AND CREATE REPORTS
   ========================================================================= */

/* Save PD results */
data models.pd_results;
    set work.pd_with_macro;
    keep loan_id customer_id product_type stage_ifrs9
         pd_rating pd_12m_pit pd_12m_ttc pd_lifetime
         pd_year1-pd_year5 pd_cum_1y-pd_cum_5y
         scenario_type pd_12m_scenario pd_lifetime_scenario;
run;

/* Export model parameters */
proc export data=work.model_params_full
    outfile="&output_path/pd_model_parameters_&run_date..csv"
    dbms=csv replace;
run;

/* Create model documentation */
ods pdf file="&output_path/pd_model_documentation_&run_date..pdf";
    title "PD Model Documentation";
    
    proc print data=work.model_params_full;
        title2 "Model Coefficients";
    run;
    
    proc print data=work.variable_importance(obs=20);
        title2 "Variable Importance (Information Value)";
    run;
    
    proc print data=work.calibration_table;
        title2 "Model Calibration Table";
    run;
    
    proc sgplot data=work.calibration_table;
        title2 "Calibration Plot";
        series x=predicted_dr y=actual_dr;
        lineparm x=0 y=0 slope=1 / lineattrs=(color=red pattern=dash);
        xaxis label="Predicted Default Rate";
        yaxis label="Actual Default Rate";
    run;
    
    proc print data=work.roc_stats;
        title2 "ROC Statistics";
    run;
    
    proc freq data=work.pd_term_structure;
        title2 "PD Rating Distribution";
        tables pd_rating * data_partition / nocol nopercent;
    run;
ods pdf close;

/* =========================================================================
   PART 11: CREATE MONITORING DATASET
   ========================================================================= */

/* Prepare dataset for ongoing monitoring */
data models.pd_monitoring;
    retain model_name "PD_MODEL_V1" model_date "&sysdate9";
    set work.model_data_calibrated;
    
    /* Add model metadata */
    model_version = "1.0";
    model_type = "LOGISTIC_REGRESSION";
    last_updated = datetime();
    format last_updated datetime20.;
    
    /* Performance flags for monitoring */
    if abs(pd_calibrated - &target_var) > 0.5 then large_error_flag = 1;
    else large_error_flag = 0;
    
    /* Score bands for monitoring */
    if pd_calibrated < 0.01 then score_band = "AAA";
    else if pd_calibrated < 0.03 then score_band = "AA";
    else if pd_calibrated < 0.05 then score_band = "A";
    else if pd_calibrated < 0.10 then score_band = "BBB";
    else if pd_calibrated < 0.20 then score_band = "BB";
    else if pd_calibrated < 0.40 then score_band = "B";
    else score_band = "CCC";
run;

%put NOTE: ====================================;
%put NOTE: PD Model Development Completed;
%put NOTE: Model Gini: &gini_coef;
%put NOTE: Calibration Factor: &calib_factor;
%put NOTE: Output: models.pd_results;
%put NOTE: ====================================;

/* End of program */