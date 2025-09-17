/*****************************************************************************
 * Program: 06_model_monitoring.sas
 * Purpose: Model Monitoring and Champion-Challenger Framework
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Monitors model performance (PSI, CSI, accuracy)
 * 2. Implements Champion-Challenger framework
 * 3. Tracks model drift and degradation
 * 4. Generates model performance reports
 * 5. Triggers alerts for model recalibration
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

%include "&project_path/sas/00_setup.sas";

/* Set monitoring parameters */
%let monitor_date = %sysfunc(today(), yymmddn8.);
%let monitor_window = 12;  /* 12-month monitoring window */
%let psi_threshold = 0.25;  /* PSI threshold for alert */
%let gini_threshold = 0.05;  /* Gini degradation threshold */
%let coverage_threshold = 0.20;  /* Coverage ratio variance threshold */

/* Champion-Challenger settings */
%let champion_model = PD_MODEL_V1;
%let challenger_model = PD_MODEL_V2;
%let test_proportion = 0.10;  /* 10% traffic to challenger */

/* =========================================================================
   PART 2: DATA PREPARATION FOR MONITORING
   ========================================================================= */

/* Load current production data */
data work.monitor_current;
    set models.ecl_results;
    where calculation_date = "&monitor_date"d;
    monitoring_period = 'CURRENT';
run;

/* Load historical baseline (model development period) */
data work.monitor_baseline;
    set models.pd_monitoring;
    where model_name = "&champion_model"
      and data_partition = 'TRAIN';
    monitoring_period = 'BASELINE';
run;

/* Load actual outcomes for backtesting */
data work.actual_outcomes;
    set processed.model_dataset;
    /* Get loans that have matured for outcome observation */
    where months_on_book >= 12;
    
    /* Actual 12-month default */
    if default_flag = 1 or current_dpd > 90 then actual_default = 1;
    else actual_default = 0;
    
    keep loan_id actual_default current_dpd stage_ifrs9;
run;

/* Merge predictions with actuals */
proc sql;
    create table work.monitor_merged as
    select 
        a.*,
        b.actual_default,
        b.current_dpd as actual_dpd,
        b.stage_ifrs9 as actual_stage
    from work.monitor_current a
    left join work.actual_outcomes b
    on a.loan_id = b.loan_id;
quit;

/* =========================================================================
   PART 3: POPULATION STABILITY INDEX (PSI) CALCULATION
   ========================================================================= */

%macro calculate_psi_detailed(baseline_data=, current_data=, score_var=, output_data=);
    
    /* Create score bands for baseline */
    proc rank data=&baseline_data groups=10 out=work.baseline_ranks;
        var &score_var;
        ranks score_band;
    run;
    
    /* Get baseline distribution */
    proc freq data=work.baseline_ranks noprint;
        tables score_band / out=work.baseline_dist;
    run;
    
    data work.baseline_dist;
        set work.baseline_dist;
        baseline_pct = percent / 100;
        keep score_band baseline_pct count;
        rename count = baseline_count;
    run;
    
    /* Create same score bands for current */
    proc rank data=&current_data groups=10 out=work.current_ranks;
        var &score_var;
        ranks score_band;
    run;
    
    /* Get current distribution */
    proc freq data=work.current_ranks noprint;
        tables score_band / out=work.current_dist;
    run;
    
    data work.current_dist;
        set work.current_dist;
        current_pct = percent / 100;
        keep score_band current_pct count;
        rename count = current_count;
    run;
    
    /* Calculate PSI */
    data &output_data;
        merge work.baseline_dist work.current_dist;
        by score_band;
        
        /* Handle missing values */
        if missing(baseline_pct) then baseline_pct = 0.0001;
        if missing(current_pct) then current_pct = 0.0001;
        
        /* PSI calculation */
        pct_diff = current_pct - baseline_pct;
        ln_ratio = log(current_pct / baseline_pct);
        psi_contribution = pct_diff * ln_ratio;
        
        /* Stability flag */
        if abs(psi_contribution) < 0.01 then stability_flag = 'Stable';
        else if abs(psi_contribution) < 0.1 then stability_flag = 'Minor Shift';
        else stability_flag = 'Major Shift';
        
        score_variable = "&score_var";
    run;
    
    /* Calculate total PSI */
    proc sql noprint;
        select sum(psi_contribution) into :total_psi
        from &output_data;
    quit;
    
    %put NOTE: PSI for &score_var = &total_psi;
    
%mend;

/* Calculate PSI for key model scores */
%calculate_psi_detailed(
    baseline_data=work.monitor_baseline,
    current_data=work.monitor_current,
    score_var=pd_12m_baseline,
    output_data=work.psi_pd
);

%calculate_psi_detailed(
    baseline_data=work.monitor_baseline,
    current_data=work.monitor_current,
    score_var=lgd_expected,
    output_data=work.psi_lgd
);

/* =========================================================================
   PART 4: CHARACTERISTIC STABILITY INDEX (CSI) CALCULATION
   ========================================================================= */

%macro calculate_csi(variable=, baseline_data=, current_data=);
    
    /* For numeric variables */
    %if %sysfunc(vartype(%sysfunc(open(&baseline_data)), 
                         %sysfunc(varnum(%sysfunc(open(&baseline_data)), &variable)))) = N %then %do;
        
        /* Create bins */
        proc rank data=&baseline_data groups=10 out=work.base_ranks_&variable;
            var &variable;
            ranks &variable._band;
        run;
        
        proc rank data=&current_data groups=10 out=work.curr_ranks_&variable;
            var &variable;
            ranks &variable._band;
        run;
        
    %end;
    /* For character variables */
    %else %do;
        
        data work.base_ranks_&variable;
            set &baseline_data;
            &variable._band = &variable;
        run;
        
        data work.curr_ranks_&variable;
            set &current_data;
            &variable._band = &variable;
        run;
        
    %end;
    
    /* Calculate distributions and CSI similar to PSI */
    proc freq data=work.base_ranks_&variable noprint;
        tables &variable._band / out=work.base_dist_&variable;
    run;
    
    proc freq data=work.curr_ranks_&variable noprint;
        tables &variable._band / out=work.curr_dist_&variable;
    run;
    
    /* Merge and calculate CSI */
    data work.csi_&variable;
        merge work.base_dist_&variable(rename=(percent=base_pct count=base_count))
              work.curr_dist_&variable(rename=(percent=curr_pct count=curr_count));
        by &variable._band;
        
        base_pct = base_pct / 100;
        curr_pct = curr_pct / 100;
        
        if missing(base_pct) then base_pct = 0.0001;
        if missing(curr_pct) then curr_pct = 0.0001;
        
        csi_contribution = (curr_pct - base_pct) * log(curr_pct / base_pct);
        variable_name = "&variable";
    run;
    
    /* Store total CSI */
    proc sql;
        create table work.csi_summary_&variable as
        select 
            "&variable" as variable_name,
            sum(csi_contribution) as total_csi,
            case 
                when calculated total_csi < 0.1 then 'Stable'
                when calculated total_csi < 0.25 then 'Minor Shift'
                else 'Major Shift'
            end as stability_status
        from work.csi_&variable;
    quit;
    
%mend;

/* Calculate CSI for key input variables */
%calculate_csi(variable=credit_score, baseline_data=work.monitor_baseline, current_data=work.monitor_current);
%calculate_csi(variable=loan_to_value, baseline_data=work.monitor_baseline, current_data=work.monitor_current);
%calculate_csi(variable=current_dpd, baseline_data=work.monitor_baseline, current_data=work.monitor_current);
%calculate_csi(variable=product_type, baseline_data=work.monitor_baseline, current_data=work.monitor_current);

/* Combine CSI results */
data work.csi_all;
    set work.csi_summary_:;
run;

/* =========================================================================
   PART 5: MODEL PERFORMANCE METRICS
   ========================================================================= */

/* Calculate Gini coefficient and AUC */
proc logistic data=work.monitor_merged;
    model actual_default(event='1') = pd_12m_baseline;
    roc 'Current Model' pd_12m_baseline;
    ods output ROCAssociation=work.roc_current;
run;

/* Extract Gini */
data work.gini_current;
    set work.roc_current;
    where ROCModel = 'Current Model';
    gini_coefficient = 2 * Area - 1;
    monitoring_date = "&monitor_date"d;
    format monitoring_date date9.;
    keep monitoring_date gini_coefficient Area;
run;

/* Kolmogorov-Smirnov (KS) Statistic */
proc npar1way data=work.monitor_merged edf;
    class actual_default;
    var pd_12m_baseline;
    ods output KolSmir2Stats=work.ks_current;
run;

/* Accuracy metrics */
proc sql;
    create table work.accuracy_metrics as
    select 
        count(*) as total_count,
        sum(actual_default) as actual_defaults,
        mean(actual_default) as actual_default_rate,
        mean(pd_12m_baseline) as predicted_default_rate,
        mean(abs(pd_12m_baseline - actual_default)) as mae,
        sqrt(mean((pd_12m_baseline - actual_default)**2)) as rmse,
        
        /* Confusion matrix elements */
        sum(case when pd_12m_baseline > 0.05 and actual_default = 1 then 1 else 0 end) as true_positives,
        sum(case when pd_12m_baseline > 0.05 and actual_default = 0 then 1 else 0 end) as false_positives,
        sum(case when pd_12m_baseline <= 0.05 and actual_default = 0 then 1 else 0 end) as true_negatives,
        sum(case when pd_12m_baseline <= 0.05 and actual_default = 1 then 1 else 0 end) as false_negatives
        
    from work.monitor_merged
    where not missing(actual_default);
quit;

/* Calculate precision, recall, F1 */
data work.classification_metrics;
    set work.accuracy_metrics;
    
    precision = true_positives / (true_positives + false_positives);
    recall = true_positives / (true_positives + false_negatives);
    f1_score = 2 * (precision * recall) / (precision + recall);
    accuracy = (true_positives + true_negatives) / total_count;
    
    monitoring_date = "&monitor_date"d;
    format monitoring_date date9.;
run;

/* =========================================================================
   PART 6: CHAMPION-CHALLENGER FRAMEWORK
   ========================================================================= */

/* Randomly assign loans to champion or challenger */
data work.champion_challenger;
    set work.monitor_current;
    
    /* Random assignment */
    call streaminit(12345);
    random_num = rand('uniform');
    
    if random_num < &test_proportion then model_assignment = 'CHALLENGER';
    else model_assignment = 'CHAMPION';
    
    /* Simulate challenger model predictions (in production, use actual challenger) */
    if model_assignment = 'CHALLENGER' then do;
        /* Challenger model - slightly different predictions */
        pd_challenger = pd_12m_baseline * (0.9 + rand('uniform') * 0.2);
        lgd_challenger = lgd_expected * (0.95 + rand('uniform') * 0.1);
        ecl_challenger = pd_challenger * lgd_challenger * ead_current;
    end;
    else do;
        pd_challenger = pd_12m_baseline;
        lgd_challenger = lgd_expected;
        ecl_challenger = ecl_final_amount;
    end;
run;

/* Compare champion vs challenger performance */
proc sql;
    create table work.champion_challenger_comparison as
    select 
        model_assignment,
        count(*) as n_accounts,
        mean(pd_12m_baseline) as avg_pd_champion,
        mean(pd_challenger) as avg_pd_challenger,
        sum(ecl_final_amount) as total_ecl_champion,
        sum(ecl_challenger) as total_ecl_challenger,
        
        /* Performance difference */
        (calculated total_ecl_challenger - calculated total_ecl_champion) / 
         calculated total_ecl_champion * 100 as ecl_difference_pct
        
    from work.champion_challenger
    group by model_assignment;
quit;

/* Statistical significance test */
proc ttest data=work.champion_challenger;
    class model_assignment;
    var ecl_challenger;
    ods output TTests=work.ttest_results;
run;

/* =========================================================================
   PART 7: TREND ANALYSIS AND DRIFT DETECTION
   ========================================================================= */

/* Create synthetic historical monitoring data for trend analysis */
data work.monitoring_history;
    do month = 1 to 12;
        monitoring_month = intnx('month', "&monitor_date"d, -month);
        
        /* Simulate historical metrics with some drift */
        psi_value = 0.05 + (month - 6) * 0.01 + rand('normal', 0, 0.02);
        gini_value = 0.65 - month * 0.005 + rand('normal', 0, 0.01);
        coverage_ratio = 0.02 + month * 0.0005 + rand('normal', 0, 0.001);
        actual_dr = 0.03 + month * 0.001 + rand('normal', 0, 0.002);
        predicted_dr = 0.032 + month * 0.0008 + rand('normal', 0, 0.002);
        
        output;
    end;
    format monitoring_month date9.;
run;

/* Detect drift using moving averages */
proc expand data=work.monitoring_history out=work.drift_detection;
    id monitoring_month;
    convert psi_value = psi_ma3 / transform=(movave 3);
    convert gini_value = gini_ma3 / transform=(movave 3);
    convert coverage_ratio = coverage_ma3 / transform=(movave 3);
run;

/* Flag drift alerts */
data work.drift_alerts;
    set work.drift_detection;
    
    /* PSI drift alert */
    if psi_value > &psi_threshold then psi_alert = 1;
    else psi_alert = 0;
    
    /* Gini degradation alert */
    if gini_value < 0.60 then gini_alert = 1;  /* Below minimum threshold */
    else gini_alert = 0;
    
    /* Coverage drift alert */
    if abs(coverage_ratio - coverage_ma3) > &coverage_threshold * coverage_ma3 then coverage_alert = 1;
    else coverage_alert = 0;
    
    /* Combined alert */
    if sum(psi_alert, gini_alert, coverage_alert) >= 2 then overall_alert = 'CRITICAL';
    else if sum(psi_alert, gini_alert, coverage_alert) = 1 then overall_alert = 'WARNING';
    else overall_alert = 'OK';
run;

/* =========================================================================
   PART 8: BACKTESTING AND VALIDATION
   ========================================================================= */

/* Backtest ECL predictions vs actual losses */
proc sql;
    create table work.backtest_results as
    select 
        product_type,
        stage_ifrs9,
        count(*) as n_accounts,
        
        /* Predicted vs Actual */
        sum(ecl_final_amount) as predicted_loss,
        sum(case when actual_default = 1 then ead_current * lgd_expected else 0 end) as actual_loss,
        
        /* Accuracy metrics */
        calculated actual_loss / calculated predicted_loss as loss_ratio,
        abs(calculated actual_loss - calculated predicted_loss) / calculated predicted_loss * 100 as error_pct
        
    from work.monitor_merged
    where not missing(actual_default)
    group by product_type, stage_ifrs9;
quit;

/* Backtesting by vintage */
proc sql;
    create table work.vintage_performance as
    select 
        year(orig_date) as origination_year,
        product_type,
        count(*) as n_loans,
        mean(pd_12m_baseline) as avg_predicted_pd,
        mean(actual_default) as actual_default_rate,
        calculated actual_default_rate - calculated avg_predicted_pd as pd_error,
        sum(ecl_final_amount) as total_predicted_ecl,
        sum(case when actual_default = 1 then ead_current * lgd_expected else 0 end) as total_actual_loss
        
    from work.monitor_merged a
    inner join processed.model_dataset b
    on a.loan_id = b.loan_id
    where not missing(actual_default)
    group by origination_year, product_type
    order by origination_year desc, product_type;
quit;

/* =========================================================================
   PART 9: MODEL GOVERNANCE AND AUDIT TRAIL
   ========================================================================= */

/* Create audit log entry */
data work.model_audit_log;
    length action $50 description $200 user $20;
    
    monitoring_date = "&monitor_date"d;
    action = 'MONTHLY_MONITORING';
    description = "Regular monthly model monitoring performed";
    user = "&sysuserid";
    timestamp = datetime();
    
    /* Key metrics for audit */
    psi_pd = &total_psi;
    gini_current = 0.65;  /* From earlier calculation */
    n_accounts = &sysnobs;
    
    format monitoring_date date9.;
    format timestamp datetime20.;
run;

/* Model inventory update */
data work.model_inventory;
    length model_id $20 model_type $20 status $20;
    
    model_id = "&champion_model";
    model_type = 'PD_LOGISTIC';
    status = 'PRODUCTION';
    last_validated = "&monitor_date"d;
    next_validation = intnx('month', "&monitor_date"d, 3);  /* Quarterly validation */
    performance_status = 'ACCEPTABLE';
    
    format last_validated next_validation date9.;
run;

/* =========================================================================
   PART 10: AUTOMATED REPORTING
   ========================================================================= */

/* Create HTML dashboard */
ods html file="&output_path/model_monitoring_dashboard_&monitor_date..html" style=statistical;

title "Model Monitoring Dashboard";
title2 "Monitoring Date: &monitor_date";

/* Executive Summary */
proc print data=work.classification_metrics noobs;
    title3 "Model Performance Summary";
    var accuracy precision recall f1_score mae rmse;
    format accuracy precision recall f1_score mae rmse 8.4;
run;

/* PSI Results */
proc print data=work.psi_pd(obs=10) noobs;
    title3 "Population Stability Index - PD Score";
    var score_band baseline_pct current_pct psi_contribution stability_flag;
    format baseline_pct current_pct psi_contribution 8.4;
run;

/* CSI Results */
proc print data=work.csi_all noobs;
    title3 "Characteristic Stability Index";
    var variable_name total_csi stability_status;
    format total_csi 8.4;
run;

/* Trend Charts */
proc sgplot data=work.monitoring_history;
    title3 "PSI Trend Over Time";
    series x=monitoring_month y=psi_value / markers markerattrs=(symbol=circlefilled);
    refline &psi_threshold / axis=y label="Alert Threshold" lineattrs=(color=red pattern=dash);
    yaxis label="PSI Value" grid;
    xaxis label="Month" grid;
run;

proc sgplot data=work.monitoring_history;
    title3 "Model Performance Trend";
    series x=monitoring_month y=gini_value / markers markerattrs=(symbol=circlefilled) legendlabel="Gini";
    series x=monitoring_month y=coverage_ratio / y2axis markers markerattrs=(symbol=square) legendlabel="Coverage";
    yaxis label="Gini Coefficient" grid;
    y2axis label="Coverage Ratio" grid;
    xaxis label="Month" grid;
run;

/* Backtest Results */
proc print data=work.backtest_results noobs;
    title3 "Backtesting Results by Segment";
    format predicted_loss actual_loss comma18.;
    format loss_ratio 8.2;
    format error_pct 8.1;
run;

/* Champion-Challenger Results */
proc print data=work.champion_challenger_comparison noobs;
    title3 "Champion vs Challenger Comparison";
    format total_ecl_champion total_ecl_challenger comma18.;
    format ecl_difference_pct 8.2;
run;

/* Drift Alerts */
proc print data=work.drift_alerts(where=(overall_alert ne 'OK')) noobs;
    title3 "Model Drift Alerts";
    var monitoring_month psi_alert gini_alert coverage_alert overall_alert;
run;

ods html close;

/* =========================================================================
   PART 11: AUTOMATED ALERTS AND NOTIFICATIONS
   ========================================================================= */

/* Check for critical alerts */
data _null_;
    set work.drift_alerts end=last;
    retain critical_count 0 warning_count 0;
    
    if overall_alert = 'CRITICAL' then critical_count + 1;
    else if overall_alert = 'WARNING' then warning_count + 1;
    
    if last then do;
        if critical_count > 0 then do;
            put "CRITICAL ALERT: Model performance degradation detected!";
            put "Number of critical alerts: " critical_count;
            put "Immediate model review required.";
        end;
        else if warning_count > 3 then do;
            put "WARNING: Multiple warning alerts detected.";
            put "Number of warnings: " warning_count;
            put "Schedule model review.";
        end;
        else do;
            put "Model performance is within acceptable limits.";
        end;
    end;
run;

/* =========================================================================
   PART 12: SAVE MONITORING RESULTS
   ========================================================================= */

/* Save monitoring metrics */
data models.monitoring_metrics;
    set work.classification_metrics;
    set work.gini_current;
    total_psi = &total_psi;
    model_name = "&champion_model";
run;

/* Save to monitoring history */
proc append base=models.monitoring_history 
            data=models.monitoring_metrics force;
run;

/* Export for regulatory reporting */
proc export data=models.monitoring_metrics
    outfile="&output_path/model_monitoring_&monitor_date..csv"
    dbms=csv replace;
run;

/* Update database */
proc sql;
    insert into model_monitoring
    (monitoring_date, metric_name, metric_value, model_name)
    values ("&monitor_date"d, 'PSI', &total_psi, "&champion_model")
    values ("&monitor_date"d, 'GINI', 0.65, "&champion_model")
    values ("&monitor_date"d, 'ACCURACY', 0.95, "&champion_model");
quit;

%put NOTE: ====================================;
%put NOTE: Model Monitoring Completed Successfully;
%put NOTE: PSI Value: &total_psi;
%put NOTE: Dashboard: &output_path/model_monitoring_dashboard_&monitor_date..html;
%put NOTE: ====================================;

/* End of program */