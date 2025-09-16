/*****************************************************************************
 * Program: 04_ecl_calculation.sas
 * Purpose: Calculate Expected Credit Loss (ECL) for IFRS 9
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Calculates Exposure at Default (EAD)
 * 2. Implements 12-month and lifetime ECL calculations
 * 3. Applies IFRS 9 staging rules
 * 4. Calculates probability-weighted ECL across scenarios
 * 5. Produces final ECL provisions
 *****************************************************************************/

/* =========================================================================
   PART 1: ENVIRONMENT SETUP
   ========================================================================= */

%include "&project_path/sas/00_setup.sas";

/* Set calculation parameters */
%let calc_date = %sysfunc(today(), yymmddn8.);
%let discount_rate = 0.05; /* Risk-free rate for discounting */

/* Scenario weights for probability-weighted ECL */
%let weight_baseline = 0.60;
%let weight_adverse = 0.30;
%let weight_severe = 0.10;

/* =========================================================================
   PART 2: LOAD AND MERGE MODEL OUTPUTS
   ========================================================================= */

/* Merge PD and LGD results */
proc sql;
    create table work.ecl_base as
    select 
        a.loan_id,
        a.customer_id,
        a.product_type,
        a.province,
        a.stage_ifrs9,
        a.original_amount,
        a.interest_rate,
        a.months_on_book,
        a.years_on_book,
        a.current_dpd,
        
        /* PD components */
        b.pd_12m_pit,
        b.pd_12m_ttc,
        b.pd_lifetime,
        b.pd_year1,
        b.pd_year2,
        b.pd_year3,
        b.pd_year4,
        b.pd_year5,
        
        /* LGD components */
        c.lgd_12m,
        c.lgd_lifetime,
        c.lgd_expected,
        c.lgd_downturn
        
    from processed.model_dataset a
    left join models.pd_results b
        on a.loan_id = b.loan_id and b.scenario_type = 'baseline'
    left join models.lgd_results c
        on a.loan_id = c.loan_id;
quit;

/* =========================================================================
   PART 3: CALCULATE EXPOSURE AT DEFAULT (EAD)
   ========================================================================= */

data work.ead_calculation;
    set work.ecl_base;
    
    /* Current outstanding balance (simplified amortization) */
    if product_type in ('MORTGAGE','AUTO_LOAN','PERSONAL_LOAN') then do;
        /* Term loans - declining balance */
        select(product_type);
            when('MORTGAGE') do;
                loan_term = 300; /* 25 years in months */
                monthly_rate = interest_rate / 12;
                if months_on_book < loan_term then
                    current_balance = original_amount * 
                        (1 - (1 - (1 + monthly_rate)**(-loan_term + months_on_book)) / 
                         (1 - (1 + monthly_rate)**(-loan_term)));
                else current_balance = 0;
            end;
            when('AUTO_LOAN') do;
                loan_term = 84; /* 7 years */
                monthly_rate = interest_rate / 12;
                if months_on_book < loan_term then
                    current_balance = original_amount * 
                        (1 - months_on_book / loan_term); /* Simplified linear */
                else current_balance = 0;
            end;
            when('PERSONAL_LOAN') do;
                loan_term = 60; /* 5 years */
                if months_on_book < loan_term then
                    current_balance = original_amount * 
                        (1 - months_on_book / loan_term);
                else current_balance = 0;
            end;
        end;
    end;
    
    /* Revolving credit - apply Credit Conversion Factor (CCF) */
    else if product_type in ('CREDIT_CARD','HELOC') then do;
        /* Assume utilization rate */
        if stage_ifrs9 = 1 then utilization_rate = 0.30;
        else if stage_ifrs9 = 2 then utilization_rate = 0.50;
        else utilization_rate = 0.80;
        
        current_balance = original_amount * utilization_rate;
        
        /* Credit Conversion Factor for undrawn amounts */
        if stage_ifrs9 = 1 then ccf = 0.50;
        else if stage_ifrs9 = 2 then ccf = 0.75;
        else ccf = 1.00;
        
        /* Undrawn commitment */
        undrawn_amount = original_amount * (1 - utilization_rate);
    end;
    
    else do;
        current_balance = original_amount * 0.5; /* Default assumption */
        undrawn_amount = 0;
        ccf = 0;
    end;
    
    /* Calculate EAD */
    if product_type in ('CREDIT_CARD','HELOC') then
        ead_current = current_balance + (undrawn_amount * ccf);
    else
        ead_current = current_balance;
    
    /* Ensure EAD is non-negative */
    if ead_current < 0 then ead_current = 0;
    
    /* Future EAD projection (simplified) */
    array ead_year{5} ead_year1-ead_year5;
    do i = 1 to 5;
        if product_type in ('MORTGAGE','AUTO_LOAN','PERSONAL_LOAN') then do;
            /* Amortizing loans */
            future_months = months_on_book + (i * 12);
            if product_type = 'MORTGAGE' and future_months < 300 then
                ead_year{i} = original_amount * 
                    (1 - future_months / 300); /* Simplified */
            else if product_type = 'AUTO_LOAN' and future_months < 84 then
                ead_year{i} = original_amount * 
                    (1 - future_months / 84);
            else if product_type = 'PERSONAL_LOAN' and future_months < 60 then
                ead_year{i} = original_amount * 
                    (1 - future_months / 60);
            else ead_year{i} = 0;
        end;
        else do;
            /* Revolving credit - assume constant */
            ead_year{i} = ead_current;
        end;
    end;
    
    drop i future_months loan_term monthly_rate;
run;

/* =========================================================================
   PART 4: CALCULATE 12-MONTH ECL
   ========================================================================= */

data work.ecl_12month;
    set work.ead_calculation;
    
    /* 12-month ECL = PD(12m) × LGD(12m) × EAD */
    ecl_12m_uncapped = pd_12m_pit * lgd_12m * ead_current;
    
    /* Apply caps based on product type */
    select(product_type);
        when('MORTGAGE') ecl_12m_cap = ead_current * 0.05;
        when('HELOC') ecl_12m_cap = ead_current * 0.10;
        when('AUTO_LOAN') ecl_12m_cap = ead_current * 0.20;
        when('CREDIT_CARD') ecl_12m_cap = ead_current * 0.30;
        when('PERSONAL_LOAN') ecl_12m_cap = ead_current * 0.25;
        otherwise ecl_12m_cap = ead_current * 0.15;
    end;
    
    /* Apply cap */
    ecl_12m = min(ecl_12m_uncapped, ecl_12m_cap);
    
    /* Stage 1 gets 12-month ECL */
    if stage_ifrs9 = 1 then ecl_stage1 = ecl_12m;
    else ecl_stage1 = 0;
run;

/* =========================================================================
   PART 5: CALCULATE LIFETIME ECL
   ========================================================================= */

data work.ecl_lifetime;
    set work.ecl_12month;
    
    /* Calculate year-by-year ECL and discount */
    array pd_yr{5} pd_year1-pd_year5;
    array ead_yr{5} ead_year1-ead_year5;
    array ecl_yr{5} ecl_year1-ecl_year5;
    array pv_ecl_yr{5} pv_ecl_year1-pv_ecl_year5;
    
    /* Effective interest rate for discounting */
    if interest_rate > 0 then eff_rate = interest_rate;
    else eff_rate = &discount_rate;
    
    /* Year-by-year ECL calculation */
    cumulative_survival = 1;
    total_lifetime_ecl = 0;
    
    do i = 1 to 5;
        /* Marginal ECL for each year */
        ecl_yr{i} = pd_yr{i} * lgd_lifetime * ead_yr{i} * cumulative_survival;
        
        /* Discount to present value */
        pv_ecl_yr{i} = ecl_yr{i} / ((1 + eff_rate) ** i);
        
        /* Add to total */
        total_lifetime_ecl = total_lifetime_ecl + pv_ecl_yr{i};
        
        /* Update survival probability */
        cumulative_survival = cumulative_survival * (1 - pd_yr{i});
    end;
    
    /* For products with longer terms, extrapolate */
    remaining_term = .;
    if product_type = 'MORTGAGE' then remaining_term = max(0, 25 - years_on_book);
    else if product_type = 'AUTO_LOAN' then remaining_term = max(0, 7 - years_on_book);
    else if product_type = 'PERSONAL_LOAN' then remaining_term = max(0, 5 - years_on_book);
    else if product_type in ('CREDIT_CARD','HELOC') then remaining_term = 10; /* Behavioral */
    
    /* Add remaining years ECL if beyond 5 years */
    if remaining_term > 5 then do;
        /* Use year 5 PD as steady state */
        annual_ecl = pd_year5 * lgd_lifetime * ead_year5;
        do year = 6 to min(remaining_term, 30);
            additional_ecl = annual_ecl * cumulative_survival / ((1 + eff_rate) ** year);
            total_lifetime_ecl = total_lifetime_ecl + additional_ecl;
            cumulative_survival = cumulative_survival * (1 - pd_year5);
        end;
    end;
    
    /* Final lifetime ECL */
    ecl_lifetime = total_lifetime_ecl;
    
    /* Alternative simplified lifetime ECL */
    ecl_lifetime_simple = pd_lifetime * lgd_lifetime * ead_current;
    
    /* Use more conservative estimate */
    ecl_lifetime_final = max(ecl_lifetime, ecl_lifetime_simple);
    
    /* Stage 2 and 3 get lifetime ECL */
    if stage_ifrs9 = 2 then ecl_stage2 = ecl_lifetime_final;
    else if stage_ifrs9 = 3 then ecl_stage3 = ecl_lifetime_final;
    else do;
        ecl_stage2 = 0;
        ecl_stage3 = 0;
    end;
    
    drop i year annual_ecl additional_ecl cumulative_survival eff_rate;
run;

/* =========================================================================
   PART 6: SCENARIO-WEIGHTED ECL CALCULATION
   ========================================================================= */

/* Get scenario PD and LGD */
proc sql;
    create table work.scenario_components as
    select 
        a.loan_id,
        a.ead_current,
        a.stage_ifrs9,
        
        /* Baseline scenario */
        b1.pd_12m_scenario as pd_12m_baseline,
        b1.pd_lifetime_scenario as pd_lifetime_baseline,
        c1.lgd_12m_scenario as lgd_12m_baseline,
        c1.lgd_lifetime_scenario as lgd_lifetime_baseline,
        
        /* Adverse scenario */
        b2.pd_12m_scenario as pd_12m_adverse,
        b2.pd_lifetime_scenario as pd_lifetime_adverse,
        c2.lgd_12m_scenario as lgd_12m_adverse,
        c2.lgd_lifetime_scenario as lgd_lifetime_adverse,
        
        /* Severely adverse scenario */
        b3.pd_12m_scenario as pd_12m_severe,
        b3.pd_lifetime_scenario as pd_lifetime_severe,
        c3.lgd_12m_scenario as lgd_12m_severe,
        c3.lgd_lifetime_scenario as lgd_lifetime_severe
        
    from work.ecl_lifetime a
    
    left join models.pd_results b1
        on a.loan_id = b1.loan_id and b1.scenario_type = 'baseline'
    left join models.lgd_with_scenarios c1
        on a.loan_id = c1.loan_id and c1.scenario_type = 'baseline'
        
    left join models.pd_results b2
        on a.loan_id = b2.loan_id and b2.scenario_type = 'adverse'
    left join models.lgd_with_scenarios c2
        on a.loan_id = c2.loan_id and c2.scenario_type = 'adverse'
        
    left join models.pd_results b3
        on a.loan_id = b3.loan_id and b3.scenario_type = 'severely_adverse'
    left join models.lgd_with_scenarios c3
        on a.loan_id = c3.loan_id and c3.scenario_type = 'severely_adverse';
quit;

/* Calculate ECL for each scenario */
data work.ecl_scenarios;
    set work.scenario_components;
    
    /* 12-month ECL by scenario */
    ecl_12m_baseline = pd_12m_baseline * lgd_12m_baseline * ead_current;
    ecl_12m_adverse = pd_12m_adverse * lgd_12m_adverse * ead_current;
    ecl_12m_severe = pd_12m_severe * lgd_12m_severe * ead_current;
    
    /* Lifetime ECL by scenario */
    ecl_lifetime_baseline = pd_lifetime_baseline * lgd_lifetime_baseline * ead_current;
    ecl_lifetime_adverse = pd_lifetime_adverse * lgd_lifetime_adverse * ead_current;
    ecl_lifetime_severe = pd_lifetime_severe * lgd_lifetime_severe * ead_current;
    
    /* Probability-weighted ECL */
    ecl_12m_weighted = ecl_12m_baseline * &weight_baseline +
                       ecl_12m_adverse * &weight_adverse +
                       ecl_12m_severe * &weight_severe;
    
    ecl_lifetime_weighted = ecl_lifetime_baseline * &weight_baseline +
                           ecl_lifetime_adverse * &weight_adverse +
                           ecl_lifetime_severe * &weight_severe;
    
    /* Final ECL based on stage */
    if stage_ifrs9 = 1 then do;
        ecl_final = ecl_12m_weighted;
        ecl_type = '12-Month';
    end;
    else if stage_ifrs9 in (2,3) then do;
        ecl_final = ecl_lifetime_weighted;
        ecl_type = 'Lifetime';
    end;
    else do;
        ecl_final = ecl_12m_weighted; /* Default to 12-month */
        ecl_type = '12-Month';
    end;
run;

/* =========================================================================
   PART 7: POST-MODEL ADJUSTMENTS AND OVERLAYS
   ========================================================================= */

data work.ecl_adjusted;
    set work.ecl_scenarios;
    
    /* Management overlay factors */
    overlay_factor = 1.0;
    overlay_reason = '';
    
    /* Emerging risk overlay */
    if province in ('AB','SK') and product_type = 'MORTGAGE' then do;
        overlay_factor = overlay_factor * 1.1; /* Oil region adjustment */
        overlay_reason = cats(overlay_reason, 'OilRegion;');
    end;
    
    /* COVID-19 or similar pandemic overlay (if applicable) */
    if product_type in ('PERSONAL_LOAN','CREDIT_CARD') and stage_ifrs9 = 2 then do;
        overlay_factor = overlay_factor * 1.05;
        overlay_reason = cats(overlay_reason, 'PandemicRisk;');
    end;
    
    /* New origination overlay (limited performance history) */
    if months_on_book < 6 then do;
        overlay_factor = overlay_factor * 1.15;
        overlay_reason = cats(overlay_reason, 'NewOrigination;');
    end;
    
    /* High-risk segment overlay */
    if current_dpd > 60 and stage_ifrs9 = 2 then do;
        overlay_factor = overlay_factor * 1.2;
        overlay_reason = cats(overlay_reason, 'HighRisk;');
    end;
    
    /* Apply overlay */
    ecl_before_overlay = ecl_final;
    ecl_after_overlay = ecl_final * overlay_factor;
    
    /* Apply regulatory floor */
    select(product_type);
        when('MORTGAGE') ecl_floor = ead_current * 0.0005;
        when('HELOC') ecl_floor = ead_current * 0.001;
        when('AUTO_LOAN') ecl_floor = ead_current * 0.002;
        when('CREDIT_CARD') ecl_floor = ead_current * 0.02;
        when('PERSONAL_LOAN') ecl_floor = ead_current * 0.01;
        otherwise ecl_floor = ead_current * 0.005;
    end;
    
    /* Final ECL with floor */
    ecl_final_amount = max(ecl_after_overlay, ecl_floor);
    
    /* Coverage ratio */
    if ead_current > 0 then coverage_ratio = ecl_final_amount / ead_current;
    else coverage_ratio = 0;
    
    format coverage_ratio percent8.2;
run;

/* =========================================================================
   PART 8: COLLECTIVE ASSESSMENT AND PROVISIONS
   ========================================================================= */

/* Aggregate ECL by segments for collective provisions */
proc sql;
    create table work.collective_provisions as
    select 
        product_type,
        stage_ifrs9,
        count(*) as n_accounts,
        sum(ead_current) as total_ead,
        sum(ecl_final_amount) as total_ecl,
        mean(coverage_ratio) as avg_coverage_ratio,
        
        /* Breakdown by ECL type */
        sum(case when ecl_type = '12-Month' then ecl_final_amount else 0 end) as ecl_12month_total,
        sum(case when ecl_type = 'Lifetime' then ecl_final_amount else 0 end) as ecl_lifetime_total,
        
        /* Breakdown by scenario */
        sum(ecl_12m_baseline * &weight_baseline) as ecl_baseline_contribution,
        sum(ecl_12m_adverse * &weight_adverse) as ecl_adverse_contribution,
        sum(ecl_12m_severe * &weight_severe) as ecl_severe_contribution
        
    from work.ecl_adjusted
    group by product_type, stage_ifrs9
    order by product_type, stage_ifrs9;
quit;

/* Calculate provision rates */
data work.provision_rates;
    set work.collective_provisions;
    
    /* Provision rate = ECL / EAD */
    if total_ead > 0 then provision_rate = total_ecl / total_ead;
    else provision_rate = 0;
    
    /* Annualized provision rate */
    if stage_ifrs9 = 1 then annualized_provision = provision_rate;
    else annualized_provision = provision_rate / 3; /* Assume 3-year average life */
    
    format provision_rate annualized_provision percent8.4;
run;

/* =========================================================================
   PART 9: FINAL ECL OUTPUT
   ========================================================================= */

/* Create final ECL results table */
data models.ecl_results;
    set work.ecl_adjusted;
    
    /* Key identifiers */
    keep loan_id customer_id product_type province stage_ifrs9
         
         /* Exposure metrics */
         original_amount current_balance ead_current
         
         /* Risk components */
         pd_12m_baseline pd_lifetime_baseline
         lgd_12m_baseline lgd_lifetime_baseline
         
         /* ECL amounts */
         ecl_12m_weighted ecl_lifetime_weighted
         ecl_final_amount ecl_type
         
         /* Adjustments */
         overlay_factor overlay_reason
         ecl_before_overlay ecl_after_overlay
         
         /* Metrics */
         coverage_ratio
         
         /* Metadata */
         calc_date;
         
    calc_date = "&calc_date"d;
    format calc_date date9.;
    format current_balance ead_current original_amount comma18.2;
    format ecl_: comma18.2;
run;

/* Create summary for reporting */
proc sql;
    create table models.ecl_summary as
    select 
        product_type,
        stage_ifrs9,
        count(*) as n_accounts format=comma12.,
        sum(original_amount) as total_original format=comma18.2,
        sum(ead_current) as total_ead format=comma18.2,
        sum(ecl_final_amount) as total_ecl format=comma18.2,
        
        /* Coverage metrics */
        calculated total_ecl / calculated total_ead as coverage_ratio format=percent8.3,
        
        /* Stage distribution */
        sum(case when stage_ifrs9 = 1 then ecl_final_amount else 0 end) 
            as stage1_ecl format=comma18.2,
        sum(case when stage_ifrs9 = 2 then ecl_final_amount else 0 end) 
            as stage2_ecl format=comma18.2,
        sum(case when stage_ifrs9 = 3 then ecl_final_amount else 0 end) 
            as stage3_ecl format=comma18.2
            
    from models.ecl_results
    group by product_type, stage_ifrs9
    
    union all
    
    /* Add total row */
    select 
        'TOTAL' as product_type,
        . as stage_ifrs9,
        count(*) as n_accounts,
        sum(original_amount) as total_original,
        sum(ead_current) as total_ead,
        sum(ecl_final_amount) as total_ecl,
        calculated total_ecl / calculated total_ead as coverage_ratio,
        sum(case when stage_ifrs9 = 1 then ecl_final_amount else 0 end) as stage1_ecl,
        sum(case when stage_ifrs9 = 2 then ecl_final_amount else 0 end) as stage2_ecl,
        sum(case when stage_ifrs9 = 3 then ecl_final_amount else 0 end) as stage3_ecl
    from models.ecl_results
    
    order by product_type, stage_ifrs9;
quit;

/* =========================================================================
   PART 10: ECL MOVEMENT ANALYSIS
   ========================================================================= */

/* Create movement analysis (if prior period data exists) */
%macro ecl_movement(prior_date=);
    %if %sysfunc(exist(models.ecl_results_&prior_date)) %then %do;
        
        proc sql;
            create table work.ecl_movement as
            select 
                coalesce(a.product_type, b.product_type) as product_type,
                coalesce(a.stage_ifrs9, b.stage_ifrs9) as stage_ifrs9,
                
                /* Opening balance */
                sum(b.ecl_final_amount) as ecl_opening,
                
                /* New originations */
                sum(case when a.loan_id is not null and b.loan_id is null 
                        then a.ecl_final_amount else 0 end) as ecl_new_origination,
                
                /* Payoffs */
                sum(case when a.loan_id is null and b.loan_id is not null 
                        then -b.ecl_final_amount else 0 end) as ecl_payoffs,
                
                /* Stage migrations */
                sum(case when a.stage_ifrs9 > b.stage_ifrs9 
                        then a.ecl_final_amount - b.ecl_final_amount else 0 end) 
                    as ecl_stage_deterioration,
                sum(case when a.stage_ifrs9 < b.stage_ifrs9 
                        then a.ecl_final_amount - b.ecl_final_amount else 0 end) 
                    as ecl_stage_improvement,
                
                /* Model and parameter changes */
                sum(case when a.loan_id = b.loan_id and a.stage_ifrs9 = b.stage_ifrs9
                        then a.ecl_final_amount - b.ecl_final_amount else 0 end) 
                    as ecl_model_change,
                
                /* Closing balance */
                sum(a.ecl_final_amount) as ecl_closing
                
            from models.ecl_results a
            full join models.ecl_results_&prior_date b
                on a.loan_id = b.loan_id
            group by calculated product_type, calculated stage_ifrs9;
        quit;
        
    %end;
%mend;

/* Run movement analysis if prior data exists */
/*%ecl_movement(prior_date=20231231);*/

/* =========================================================================
   PART 11: REPORTING AND VALIDATION
   ========================================================================= */

/* Create detailed ECL report */
ods html file="&output_path/ecl_calculation_report_&calc_date..html" style=htmlblue;

    title "IFRS 9 ECL Calculation Report";
    title2 "Calculation Date: &calc_date";
    
    /* Overall summary */
    proc print data=models.ecl_summary noobs;
        title3 "ECL Summary by Product and Stage";
        where product_type ne 'TOTAL' or stage_ifrs9 = .;
    run;
    
    /* Coverage ratios */
    proc sgplot data=models.ecl_summary;
        where product_type ne 'TOTAL' and not missing(stage_ifrs9);
        title3 "Coverage Ratios by Product and Stage";
        vbar product_type / response=coverage_ratio group=stage_ifrs9
                           groupdisplay=cluster dataskin=gloss;
        yaxis label="ECL Coverage Ratio (%)" grid;
        xaxis label="Product Type";
    run;
    
    /* ECL distribution */
    proc sgplot data=models.ecl_results;
        title3 "ECL Distribution";
        histogram ecl_final_amount;
        density ecl_final_amount;
        xaxis label="ECL Amount" grid;
        yaxis label="Frequency";
    run;
    
    /* Stage distribution */
    proc freq data=models.ecl_results;
        title3 "Portfolio Stage Distribution";
        tables stage_ifrs9 * product_type / nocol nopercent;
    run;
    
    /* Provision rates */
    proc print data=work.provision_rates noobs;
        title3 "Provision Rates by Product and Stage";
        var product_type stage_ifrs9 n_accounts total_ead total_ecl 
            provision_rate annualized_provision;
    run;

ods html close;

/* Export results to CSV */
proc export data=models.ecl_results
    outfile="&output_path/ecl_results_&calc_date..csv"
    dbms=csv replace;
run;

proc export data=models.ecl_summary
    outfile="&output_path/ecl_summary_&calc_date..csv"
    dbms=csv replace;
run;

%put NOTE: ====================================;
%put NOTE: ECL Calculation Completed Successfully;
%put NOTE: Total ECL calculated for %obs(models.ecl_results) accounts;
%put NOTE: Results saved to: models.ecl_results;
%put NOTE: Summary saved to: models.ecl_summary;
%put NOTE: ====================================;

/* End of program */