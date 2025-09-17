/*****************************************************************************
 * Program: 05_stress_testing.sas
 * Purpose: Comprehensive Stress Testing Framework for IFRS 9 ECL
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Implements OSFI stress testing requirements
 * 2. Runs multiple economic scenarios
 * 3. Calculates stressed ECL under various conditions
 * 4. Performs sensitivity analysis
 * 5. Generates regulatory stress test reports
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

%include "&project_path/sas/00_setup.sas";

/* Set stress testing parameters */
%let stress_date = %sysfunc(today(), yymmddn8.);
%let n_scenarios = 3;  /* Baseline, Adverse, Severely Adverse */
%let horizon_months = 24;  /* 2-year stress horizon per OSFI */

/* OSFI stress test parameters */
%let osfi_unemployment_shock = 3.0;  /* +3% unemployment */
%let osfi_gdp_shock = -5.0;          /* -5% GDP growth */
%let osfi_hpi_shock = -30.0;         /* -30% house prices */
%let osfi_rate_shock = 2.0;          /* +200bps interest rates */

/* =========================================================================
   PART 2: LOAD BASE DATA AND MODELS
   ========================================================================= */

/* Load ECL results and model outputs */
data work.stress_base;
    merge models.ecl_results(in=a)
          models.pd_results(in=b)
          models.lgd_results(in=c)
          processed.model_dataset(in=d 
              keep=loan_id product_type province loan_to_value 
                   credit_score annual_income months_on_book);
    by loan_id;
    if a;
run;

/* Load stress scenarios from CSV */
data work.stress_scenarios;
    set rawdata.stress_scenarios;
    
    /* Calculate stress multipliers relative to baseline */
    if scenario_type = 'baseline' then do;
        pd_stress_mult = 1.0;
        lgd_stress_mult = 1.0;
    end;
    else if scenario_type = 'adverse' then do;
        pd_stress_mult = 1.0 + (unemployment_rate - 6.7) * 0.1;  /* 10% PD increase per 1% unemployment */
        lgd_stress_mult = 1.0 + abs(hpi_change_yoy / 100) * 0.5;  /* LGD sensitive to house prices */
    end;
    else if scenario_type = 'severely_adverse' then do;
        pd_stress_mult = 1.0 + (unemployment_rate - 6.7) * 0.15;
        lgd_stress_mult = 1.0 + abs(hpi_change_yoy / 100) * 0.7;
    end;
    
    /* Cap multipliers */
    if pd_stress_mult > 3.0 then pd_stress_mult = 3.0;
    if lgd_stress_mult > 2.0 then lgd_stress_mult = 2.0;
run;

/* =========================================================================
   PART 3: SCENARIO GENERATION ENGINE
   ========================================================================= */

%macro generate_scenarios(base_data=, output_data=);
    /* Generate multiple stress scenarios */
    data &output_data;
        set &base_data;
        
        /* Loop through each scenario */
        %do s = 1 %to &n_scenarios;
            %if &s = 1 %then %let scenario = baseline;
            %else %if &s = 2 %then %let scenario = adverse;
            %else %let scenario = severely_adverse;
            
            /* Create scenario-specific records */
            scenario_id = &s;
            scenario_name = "&scenario";
            
            /* Apply scenario-specific shocks */
            %if &scenario = baseline %then %do;
                /* No stress */
                unemployment_stress = 0;
                gdp_stress = 0;
                hpi_stress = 0;
                rate_stress = 0;
            %end;
            %else %if &scenario = adverse %then %do;
                /* Moderate stress */
                unemployment_stress = &osfi_unemployment_shock;
                gdp_stress = &osfi_gdp_shock;
                hpi_stress = &osfi_hpi_shock * 0.5;  /* Half of severe */
                rate_stress = &osfi_rate_shock;
            %end;
            %else %do;
                /* Severe stress */
                unemployment_stress = &osfi_unemployment_shock * 1.5;
                gdp_stress = &osfi_gdp_shock * 1.5;
                hpi_stress = &osfi_hpi_shock;
                rate_stress = &osfi_rate_shock * 1.5;
            %end;
            
            output;
        %end;
    run;
%mend;

%generate_scenarios(base_data=work.stress_base, output_data=work.scenarios_generated);

/* =========================================================================
   PART 4: STRESSED PD CALCULATION
   ========================================================================= */

data work.pd_stressed;
    merge work.scenarios_generated(in=a)
          work.stress_scenarios(in=b keep=scenario_type pd_stress_mult);
    by scenario_name;
    
    /* Base PD from model */
    pd_base_12m = pd_12m_baseline;
    pd_base_lifetime = pd_lifetime_baseline;
    
    /* Apply macroeconomic stress to PD */
    
    /* Unemployment impact on PD */
    pd_unemploy_factor = 1 + (unemployment_stress * 0.05);  /* 5% PD increase per 1% unemployment */
    
    /* GDP impact on PD */
    if gdp_stress < 0 then
        pd_gdp_factor = 1 + abs(gdp_stress * 0.03);  /* 3% PD increase per 1% GDP decline */
    else
        pd_gdp_factor = 1;
    
    /* Interest rate impact on PD */
    pd_rate_factor = 1 + (rate_stress * 0.02);  /* 2% PD increase per 1% rate increase */
    
    /* Product-specific stress factors */
    select(product_type);
        when('MORTGAGE') do;
            /* Mortgages sensitive to house prices and rates */
            pd_product_factor = 1 + abs(hpi_stress * 0.01) + (rate_stress * 0.03);
        end;
        when('CREDIT_CARD') do;
            /* Credit cards sensitive to unemployment */
            pd_product_factor = 1 + (unemployment_stress * 0.08);
        end;
        when('AUTO_LOAN') do;
            /* Auto loans sensitive to unemployment and GDP */
            pd_product_factor = 1 + (unemployment_stress * 0.06) + abs(gdp_stress * 0.02);
        end;
        when('PERSONAL_LOAN') do;
            /* Personal loans sensitive to all factors */
            pd_product_factor = 1 + (unemployment_stress * 0.07) + abs(gdp_stress * 0.02);
        end;
        otherwise pd_product_factor = 1.05;
    end;
    
    /* Combined stress multiplier */
    pd_total_mult = pd_stress_mult * pd_unemploy_factor * pd_gdp_factor * 
                    pd_rate_factor * pd_product_factor;
    
    /* Cap total multiplier */
    if pd_total_mult > 5.0 then pd_total_mult = 5.0;
    if pd_total_mult < 1.0 then pd_total_mult = 1.0;
    
    /* Calculate stressed PD */
    pd_stressed_12m = min(pd_base_12m * pd_total_mult, 0.999);
    pd_stressed_lifetime = min(pd_base_lifetime * pd_total_mult, 0.999);
    
    /* PD term structure under stress */
    array pd_year_base[5] pd_year1-pd_year5;
    array pd_year_stressed[5] pd_year1_stressed-pd_year5_stressed;
    
    do i = 1 to 5;
        /* Apply decreasing stress over time */
        time_decay = exp(-0.3 * (i-1));  /* Exponential decay of stress impact */
        pd_year_stressed[i] = min(pd_year_base[i] * pd_total_mult * time_decay, 0.999);
    end;
    
    drop i;
run;

/* =========================================================================
   PART 5: STRESSED LGD CALCULATION
   ========================================================================= */

data work.lgd_stressed;
    merge work.pd_stressed(in=a)
          work.stress_scenarios(in=b keep=scenario_type lgd_stress_mult);
    by scenario_name;
    
    /* Base LGD from model */
    lgd_base = lgd_expected;
    
    /* Apply macroeconomic stress to LGD */
    
    /* House price impact on secured products */
    if product_type in ('MORTGAGE','HELOC') then do;
        lgd_hpi_factor = 1 + abs(hpi_stress * 0.015);  /* 1.5% LGD increase per 1% HPI decline */
    end;
    else lgd_hpi_factor = 1;
    
    /* Unemployment impact on recovery rates */
    lgd_unemploy_factor = 1 + (unemployment_stress * 0.02);  /* Lower recovery in high unemployment */
    
    /* Interest rate impact on workout costs */
    lgd_rate_factor = 1 + (rate_stress * 0.01);
    
    /* Province-specific adjustments */
    if province in ('AB','SK') and scenario_name ne 'baseline' then
        lgd_province_factor = 1.1;  /* Oil provinces more sensitive */
    else if province in ('ON','BC') and product_type = 'MORTGAGE' then
        lgd_province_factor = 1 + abs(hpi_stress * 0.005);  /* Hot markets more sensitive to HPI */
    else
        lgd_province_factor = 1;
    
    /* Combined LGD stress */
    lgd_total_mult = lgd_stress_mult * lgd_hpi_factor * lgd_unemploy_factor * 
                     lgd_rate_factor * lgd_province_factor;
    
    /* Cap LGD multiplier */
    if lgd_total_mult > 2.5 then lgd_total_mult = 2.5;
    if lgd_total_mult < 1.0 then lgd_total_mult = 1.0;
    
    /* Calculate stressed LGD */
    lgd_stressed = min(lgd_base * lgd_total_mult, 0.95);
    
    /* Downturn LGD for severe scenarios */
    if scenario_name = 'severely_adverse' then
        lgd_stressed = max(lgd_stressed, lgd_downturn);
run;

/* =========================================================================
   PART 6: STRESSED ECL CALCULATION
   ========================================================================= */

data work.ecl_stressed;
    set work.lgd_stressed;
    
    /* Calculate stressed ECL components */
    
    /* 12-month stressed ECL */
    ecl_12m_stressed = pd_stressed_12m * lgd_stressed * ead_current;
    
    /* Lifetime stressed ECL */
    ecl_lifetime_stressed = pd_stressed_lifetime * lgd_stressed * ead_current;
    
    /* Forward-looking stressed ECL with term structure */
    array pd_yr_str[5] pd_year1_stressed-pd_year5_stressed;
    array ead_yr[5] ead_year1-ead_year5;
    
    ecl_forward_stressed = 0;
    cumulative_survival = 1;
    
    do i = 1 to 5;
        yearly_ecl = pd_yr_str[i] * lgd_stressed * ead_yr[i] * cumulative_survival;
        discounted_ecl = yearly_ecl / ((1 + 0.05) ** i);
        ecl_forward_stressed = ecl_forward_stressed + discounted_ecl;
        cumulative_survival = cumulative_survival * (1 - pd_yr_str[i]);
    end;
    
    /* Select appropriate ECL based on IFRS 9 stage */
    if stage_ifrs9 = 1 then ecl_stressed_final = ecl_12m_stressed;
    else ecl_stressed_final = max(ecl_lifetime_stressed, ecl_forward_stressed);
    
    /* Calculate stress impact */
    ecl_stress_impact = ecl_stressed_final - ecl_final_amount;
    ecl_stress_ratio = ecl_stressed_final / ecl_final_amount;
    
    drop i yearly_ecl discounted_ecl cumulative_survival;
run;

/* =========================================================================
   PART 7: PORTFOLIO STRESS TESTING
   ========================================================================= */

/* Aggregate stressed ECL by portfolio segments */
proc sql;
    create table work.portfolio_stress_results as
    select 
        scenario_name,
        product_type,
        stage_ifrs9,
        count(*) as n_accounts,
        sum(ead_current) as total_ead,
        sum(ecl_final_amount) as ecl_baseline,
        sum(ecl_stressed_final) as ecl_stressed,
        sum(ecl_stress_impact) as total_impact,
        mean(ecl_stress_ratio) as avg_stress_ratio,
        
        /* Risk metrics under stress */
        mean(pd_stressed_12m) as avg_pd_stressed,
        mean(lgd_stressed) as avg_lgd_stressed,
        sum(ecl_stressed_final) / sum(ead_current) as coverage_stressed
        
    from work.ecl_stressed
    group by scenario_name, product_type, stage_ifrs9
    order by scenario_name, product_type, stage_ifrs9;
quit;

/* Capital impact assessment */
proc sql;
    create table work.capital_impact as
    select 
        scenario_name,
        sum(ead_current) as total_exposure,
        sum(ecl_baseline) as total_ecl_baseline,
        sum(ecl_stressed_final) as total_ecl_stressed,
        sum(ecl_stressed_final) - sum(ecl_baseline) as additional_provisions,
        (sum(ecl_stressed_final) - sum(ecl_baseline)) / sum(ead_current) * 100 as provision_increase_pct,
        
        /* Tier 1 capital impact (simplified) */
        sum(ecl_stressed_final) * 0.08 as tier1_capital_required,
        (sum(ecl_stressed_final) - sum(ecl_baseline)) * 0.08 as additional_capital_needed
        
    from work.ecl_stressed
    group by scenario_name
    order by scenario_name;
quit;

/* =========================================================================
   PART 8: SENSITIVITY ANALYSIS
   ========================================================================= */

%macro sensitivity_analysis(param=, range_start=, range_end=, steps=);
    /* Run sensitivity analysis for a specific parameter */
    
    data work.sensitivity_&param;
        set work.stress_base;
        
        /* Create sensitivity scenarios */
        %do i = 1 %to &steps;
            %let value = %sysevalf(&range_start + (&range_end - &range_start) * (&i - 1) / (&steps - 1));
            
            sensitivity_level = &value;
            
            /* Apply sensitivity to specific parameter */
            %if &param = unemployment %then %do;
                pd_sensitivity = pd_12m_baseline * (1 + &value * 0.05);
            %end;
            %else %if &param = gdp %then %do;
                pd_sensitivity = pd_12m_baseline * (1 + abs(&value) * 0.03);
            %end;
            %else %if &param = hpi %then %do;
                lgd_sensitivity = lgd_expected * (1 + abs(&value) * 0.01);
                pd_sensitivity = pd_12m_baseline;
            %end;
            %else %if &param = rates %then %do;
                pd_sensitivity = pd_12m_baseline * (1 + &value * 0.02);
            %end;
            
            /* Calculate ECL under sensitivity */
            %if &param = hpi %then %do;
                ecl_sensitivity = pd_sensitivity * lgd_sensitivity * ead_current;
            %end;
            %else %do;
                ecl_sensitivity = pd_sensitivity * lgd_expected * ead_current;
            %end;
            
            output;
        %end;
    run;
    
    /* Aggregate sensitivity results */
    proc sql;
        create table work.sensitivity_summary_&param as
        select 
            "&param" as parameter,
            sensitivity_level,
            sum(ecl_sensitivity) as total_ecl,
            sum(ecl_sensitivity) / sum(ead_current) as coverage_ratio,
            (sum(ecl_sensitivity) - sum(ecl_final_amount)) / sum(ecl_final_amount) * 100 as ecl_change_pct
        from work.sensitivity_&param
        group by sensitivity_level
        order by sensitivity_level;
    quit;
%mend;

/* Run sensitivity analysis for key parameters */
%sensitivity_analysis(param=unemployment, range_start=-3, range_end=6, steps=10);
%sensitivity_analysis(param=gdp, range_start=-10, range_end=5, steps=10);
%sensitivity_analysis(param=hpi, range_start=-40, range_end=10, steps=10);
%sensitivity_analysis(param=rates, range_start=-2, range_end=5, steps=10);

/* Combine sensitivity results */
data work.sensitivity_all;
    set work.sensitivity_summary_unemployment
        work.sensitivity_summary_gdp
        work.sensitivity_summary_hpi
        work.sensitivity_summary_rates;
run;

/* =========================================================================
   PART 9: REVERSE STRESS TESTING
   ========================================================================= */

/* Find break-even point where provisions double */
%macro reverse_stress_test(target_multiplier=2.0);
    
    data work.reverse_stress;
        set work.stress_base;
        
        /* Initialize */
        baseline_ecl = ecl_final_amount;
        target_ecl = baseline_ecl * &target_multiplier;
        
        /* Iteratively find stress level */
        do stress_level = 0 to 10 by 0.1;
            pd_reverse = pd_12m_baseline * (1 + stress_level * 0.1);
            lgd_reverse = lgd_expected * (1 + stress_level * 0.05);
            ecl_reverse = pd_reverse * lgd_reverse * ead_current;
            
            if ecl_reverse >= target_ecl then do;
                breakeven_stress = stress_level;
                leave;
            end;
        end;
        
        /* Calculate implied economic conditions */
        implied_unemployment = 6.7 + breakeven_stress * 2;  /* Current + stress */
        implied_gdp = -breakeven_stress * 3;
        implied_hpi = -breakeven_stress * 10;
    run;
    
    /* Summary of reverse stress test */
    proc sql;
        create table work.reverse_stress_summary as
        select 
            product_type,
            mean(breakeven_stress) as avg_breakeven_stress,
            mean(implied_unemployment) as avg_implied_unemployment,
            mean(implied_gdp) as avg_implied_gdp,
            mean(implied_hpi) as avg_implied_hpi
        from work.reverse_stress
        group by product_type;
    quit;
%mend;

%reverse_stress_test(target_multiplier=2.0);

/* =========================================================================
   PART 10: STRESS TEST REPORTING
   ========================================================================= */

/* Create comprehensive stress test report */
ods pdf file="&output_path/stress_test_report_&stress_date..pdf" style=journal;

title "IFRS 9 Stress Testing Report";
title2 "Report Date: &stress_date";

/* Executive Summary */
proc print data=work.capital_impact noobs label;
    title3 "Capital Impact Summary";
    label scenario_name = "Scenario"
          total_exposure = "Total Exposure"
          total_ecl_baseline = "Baseline ECL"
          total_ecl_stressed = "Stressed ECL"
          additional_provisions = "Additional Provisions"
          provision_increase_pct = "Provision Increase (%)"
          additional_capital_needed = "Additional Capital Required";
    format total_exposure total_ecl_baseline total_ecl_stressed 
           additional_provisions additional_capital_needed comma18.;
    format provision_increase_pct 8.2;
run;

/* Portfolio Stress Results */
proc sgplot data=work.portfolio_stress_results;
    title3 "ECL by Scenario and Product Type";
    vbar product_type / response=ecl_stressed group=scenario_name
                        groupdisplay=cluster dataskin=pressed;
    yaxis label="Stressed ECL" grid;
    xaxis label="Product Type";
run;

/* Sensitivity Analysis Chart */
proc sgplot data=work.sensitivity_all;
    title3 "ECL Sensitivity Analysis";
    series x=sensitivity_level y=ecl_change_pct / group=parameter
                                                  lineattrs=(thickness=2);
    yaxis label="ECL Change (%)" grid;
    xaxis label="Parameter Change";
    keylegend / title="Risk Factor";
run;

/* Reverse Stress Test Results */
proc print data=work.reverse_stress_summary noobs;
    title3 "Reverse Stress Test - Breakeven Analysis";
    format avg_breakeven_stress 8.2;
    format avg_implied_unemployment avg_implied_gdp avg_implied_hpi 8.1;
run;

/* Stage Migration Under Stress */
proc sql;
    create table work.stage_migration_stress as
    select 
        scenario_name,
        sum(case when stage_ifrs9 = 1 then ead_current else 0 end) / sum(ead_current) * 100 as stage1_pct,
        sum(case when stage_ifrs9 = 2 then ead_current else 0 end) / sum(ead_current) * 100 as stage2_pct,
        sum(case when stage_ifrs9 = 3 then ead_current else 0 end) / sum(ead_current) * 100 as stage3_pct
    from work.ecl_stressed
    group by scenario_name;
quit;

proc sgplot data=work.stage_migration_stress;
    title3 "Stage Distribution Under Stress";
    vbar scenario_name / response=stage1_pct legendlabel="Stage 1";
    vbar scenario_name / response=stage2_pct legendlabel="Stage 2" barwidth=0.5;
    vbar scenario_name / response=stage3_pct legendlabel="Stage 3" barwidth=0.3;
    yaxis label="Portfolio %" grid;
    xaxis label="Scenario";
run;

ods pdf close;

/* =========================================================================
   PART 11: SAVE STRESS TEST RESULTS
   ========================================================================= */

/* Save detailed results */
data models.stress_test_results;
    set work.ecl_stressed;
    stress_test_date = "&stress_date"d;
    format stress_test_date date9.;
run;

/* Save portfolio summary */
data models.stress_test_summary;
    set work.portfolio_stress_results;
    stress_test_date = "&stress_date"d;
    format stress_test_date date9.;
run;

/* Export to CSV for regulatory submission */
proc export data=models.stress_test_summary
    outfile="&output_path/stress_test_summary_&stress_date..csv"
    dbms=csv replace;
run;

/* Create OSFI submission file */
proc export data=work.capital_impact
    outfile="&output_path/osfi_stress_submission_&stress_date..csv"
    dbms=csv replace;
run;

%put NOTE: ====================================;
%put NOTE: Stress Testing Completed Successfully;
%put NOTE: Scenarios tested: &n_scenarios;
%put NOTE: Results saved to: models.stress_test_results;
%put NOTE: OSFI report: &output_path/osfi_stress_submission_&stress_date..csv;
%put NOTE: ====================================;

/* End of program */