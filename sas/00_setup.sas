/**********************************************************************
 * Program: 00_setup.sas
 * Purpose: Environment setup and macro definitions for IFRS 9 ECL Model
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program sets up the SAS environment, defines global parameters,
 * and creates reusable macros for the ECL model implementation
 **********************************************************************/

/* ===================================================================
   PART 1: SYSTEM OPTIONS AND ENVIRONMENT SETUP
   =================================================================== */

/* Clear log and output */
dm 'clear log';
dm 'clear output';

/* Set system options */
options validvarname=v7 
        compress=yes 
        mprint mlogic symbolgen
        fullstimer
        nodate nonumber
        linesize=256
        pagesize=max
        msglevel=i;

/* Set performance options */
options threads cpucount=actual 
        memsize=max
        sortsize=max
        sumsize=max
        bufsize=64k
        bufno=10;

/* ===================================================================
   PART 2: DEFINE LIBRARY REFERENCES
   =================================================================== */

/* Detect operating system and set appropriate path */
%macro set_project_paths();
    %global PROJECT_ROOT DATA_PATH SAMPLE_PATH OUTPUT_PATH SAS_PATH LOG_PATH;
    
    /* Check if running on Windows or Unix/Linux */
    %if &SYSSCP = WIN %then %do;
        /* Windows paths - MODIFY THE DRIVE LETTER AND PATH AS NEEDED */
        %let PROJECT_ROOT = D:\Backup\Documents\GitHub\YumECL-SAS;  /* Change C: to your actual drive */
    %end;
    %else %do;
        /* Unix/Linux paths */
        %let curr_path = %sysfunc(pathname(HOME));
        %let PROJECT_ROOT = &curr_path./Examples/YumECL-SAS;
    %end;
    
    /* Define sub-directories using appropriate separator */
    %if &SYSSCP = WIN %then %do;
        %let DATA_PATH = &PROJECT_ROOT\data;
        %let SAMPLE_PATH = &DATA_PATH\sample;
        %let OUTPUT_PATH = &DATA_PATH\output;
        %let SAS_PATH = &PROJECT_ROOT\sas;
        %let LOG_PATH = &PROJECT_ROOT\logs;
    %end;
    %else %do;
        %let DATA_PATH = &PROJECT_ROOT/data;
        %let SAMPLE_PATH = &DATA_PATH/sample;
        %let OUTPUT_PATH = &DATA_PATH/output;
        %let SAS_PATH = &PROJECT_ROOT/sas;
        %let LOG_PATH = &PROJECT_ROOT/logs;
    %end;
    
    /* Display the paths being used */
    %put NOTE: ====================================;
    %put NOTE: Project paths configuration:;
    %put NOTE: Operating System: &SYSSCP;
    %put NOTE: PROJECT_ROOT: &PROJECT_ROOT;
    %put NOTE: DATA_PATH: &DATA_PATH;
    %put NOTE: SAMPLE_PATH: &SAMPLE_PATH;
    %put NOTE: OUTPUT_PATH: &OUTPUT_PATH;
    %put NOTE: ====================================;
%mend set_project_paths;

/* Execute the macro to set paths */
%set_project_paths();

/* Create SAS libraries */
libname ECL_RAW "&SAMPLE_PATH" access=readonly;  /* Raw input data */
libname ECL_WORK "&OUTPUT_PATH";                  /* Working datasets */
libname ECL_PERM "&OUTPUT_PATH";                  /* Permanent results */

/* ===================================================================
   PART 3: GLOBAL PARAMETERS
   =================================================================== */

/* Model parameters */
%let MODEL_VERSION = 1.0.0;
%let CALCULATION_DATE = %sysfunc(today());
%let CALCULATION_DATE_FMT = %sysfunc(putn(&CALCULATION_DATE, yymmddn8.));

/* Date range parameters */
%let START_DATE = '01JAN2020'd;
%let END_DATE = '31DEC2024'd;
%let FORECAST_HORIZON = 60;  /* 5 years in months */

/* IFRS 9 Parameters */
%let STAGE1_DPD_THRESHOLD = 30;
%let STAGE2_DPD_THRESHOLD = 90;
%let SICR_THRESHOLD = 2.0;  /* Significant increase in credit risk multiplier */

/* Model thresholds */
%let MIN_SAMPLE_SIZE = 30;
%let MAX_CORRELATION = 0.95;
%let MIN_IV = 0.02;  /* Minimum Information Value for variable selection */
%let MAX_VIF = 10;   /* Maximum Variance Inflation Factor */

/* Scenario weights for probability-weighted ECL */
%let BASELINE_WEIGHT = 0.60;
%let ADVERSE_WEIGHT = 0.30;
%let SEVERE_WEIGHT = 0.10;

/* ===================================================================
   PART 4: UTILITY MACROS
   =================================================================== */

/* Macro: Check if dataset exists */
%macro check_dataset(lib=, dsn=, abort=YES);
    %local exist;
    %let exist = %sysfunc(exist(&lib..&dsn));
    
    %if &exist = 0 %then %do;
        %put ERROR: Dataset &lib..&dsn does not exist!;
        %if &abort = YES %then %do;
            %abort cancel;
        %end;
    %end;
    %else %do;
        %put NOTE: Dataset &lib..&dsn found successfully.;
    %end;
%mend check_dataset;

/* Macro: Create timestamp for outputs */
%macro get_timestamp();
    %local timestamp;
    %let timestamp = %sysfunc(datetime(), datetime20.);
    %let timestamp = %sysfunc(compress(&timestamp, :));
    &timestamp
%mend get_timestamp;

/* Macro: Export dataset to CSV */
%macro export_csv(data=, outfile=, replace=YES);
    proc export data=&data
        outfile="&OUTPUT_PATH/&outfile"
        dbms=csv
        %if &replace=YES %then replace;
        ;
    run;
    %put NOTE: Dataset &data exported to &outfile;
%mend export_csv;

/* Macro: Calculate missing rate for variables */
%macro missing_rate(data=, out=missing_summary);
    proc freq data=&data noprint;
        tables _all_ / missing out=work._miss_temp;
    run;
    
    proc sql;
        create table &out as
        select 
            substr(_TABLE_, 7) as Variable,
            sum(case when _TYPE_ = '0' then COUNT else 0 end) as Missing,
            sum(COUNT) as Total,
            calculated Missing / calculated Total as Missing_Rate format=percent8.2
        from work._miss_temp
        group by _TABLE_
        order by Missing_Rate desc;
    quit;
    
    proc datasets lib=work nolist;
        delete _miss_temp;
    quit;
%mend missing_rate;

/* ===================================================================
   PART 5: DATA QUALITY CHECK MACROS
   =================================================================== */

/* Macro: Comprehensive data quality check */
%macro data_quality_check(data=, report=YES);
    %local nvars nobs;
    
    /* Get dataset info */
    %let dsid = %sysfunc(open(&data));
    %let nobs = %sysfunc(attrn(&dsid, nobs));
    %let nvars = %sysfunc(attrn(&dsid, nvars));
    %let rc = %sysfunc(close(&dsid));
    
    %put NOTE: ====================================;
    %put NOTE: Data Quality Check for &data;
    %put NOTE: Number of observations: &nobs;
    %put NOTE: Number of variables: &nvars;
    %put NOTE: ====================================;
    
    %if &report = YES %then %do;
        /* Check for missing values */
        %missing_rate(data=&data, out=work.missing_summary);
        
        /* Check for duplicates */
        proc sql;
            create table work.duplicate_check as
            select *, count(*) as dup_count
            from &data
            group by _all_
            having calculated dup_count > 1;
        quit;
        
        %local n_dups;
        %let n_dups = 0;
        data _null_;
            if 0 then set work.duplicate_check nobs=n;
            call symputx('n_dups', n);
            stop;
        run;
        
        %put NOTE: Number of duplicate records: &n_dups;
        
        /* Basic statistics for numeric variables */
        proc means data=&data n nmiss mean std min max;
            title "Summary Statistics for &data";
        run;
        
        /* Clean up */
        proc datasets lib=work nolist;
            delete missing_summary duplicate_check;
        quit;
    %end;
%mend data_quality_check;

/* ===================================================================
   PART 6: STATISTICAL CALCULATION MACROS
   =================================================================== */

/* Macro: Calculate Weight of Evidence (WOE) and Information Value (IV) */
%macro calculate_woe_iv(data=, target=, var=, bins=10, out=);
    /* Bin the variable */
    proc rank data=&data out=work._ranked groups=&bins;
        var &var;
        ranks rank_&var;
    run;
    
    /* Calculate WOE and IV */
    proc sql;
        create table &out as
        select 
            rank_&var as Bin,
            min(&var) as Min_Value,
            max(&var) as Max_Value,
            sum(&target) as Bad,
            count(*) - calculated Bad as Good,
            calculated Bad / (select sum(&target) from work._ranked) as Bad_Rate,
            calculated Good / (select count(*) - sum(&target) from work._ranked) as Good_Rate,
            log(calculated Good_Rate / calculated Bad_Rate) as WOE,
            (calculated Good_Rate - calculated Bad_Rate) * calculated WOE as IV
        from work._ranked
        group by rank_&var
        order by rank_&var;
        
        select sum(IV) as Total_IV
        from &out;
    quit;
    
    /* Clean up */
    proc datasets lib=work nolist;
        delete _ranked;
    quit;
%mend calculate_woe_iv;

/* Macro: Calculate Population Stability Index (PSI) */
%macro calculate_psi(base=, current=, var=, bins=10);
    /* Bin both datasets */
    proc rank data=&base out=work._base_rank groups=&bins;
        var &var;
        ranks rank_&var;
    run;
    
    proc rank data=&current out=work._curr_rank groups=&bins;
        var &var;
        ranks rank_&var;
    run;
    
    /* Calculate distributions */
    proc sql;
        create table work._psi_calc as
        select 
            coalesce(b.Bin, c.Bin) as Bin,
            coalesce(b.Base_Pct, 0.0001) as Base_Pct,
            coalesce(c.Curr_Pct, 0.0001) as Curr_Pct,
            (calculated Curr_Pct - calculated Base_Pct) * 
            log(calculated Curr_Pct / calculated Base_Pct) as PSI
        from 
            (select rank_&var as Bin, 
                    count(*) / (select count(*) from work._base_rank) as Base_Pct
             from work._base_rank
             group by rank_&var) as b
        full join
            (select rank_&var as Bin,
                    count(*) / (select count(*) from work._curr_rank) as Curr_Pct
             from work._curr_rank
             group by rank_&var) as c
        on b.Bin = c.Bin;
        
        select sum(PSI) as Total_PSI format=8.4
        from work._psi_calc;
    quit;
    
    /* Clean up */
    proc datasets lib=work nolist;
        delete _base_rank _curr_rank _psi_calc;
    quit;
%mend calculate_psi;

/* ===================================================================
   PART 7: MODEL DEVELOPMENT MACROS
   =================================================================== */

/* Macro: Variable selection using stepwise */
%macro variable_selection(data=, target=, vars=, method=stepwise, out=);
    proc logistic data=&data;
        model &target(event='1') = &vars
            / selection=&method
              slentry=0.05
              slstay=0.05
              details
              lackfit
              rsquare;
        output out=&out p=prob_1;
    run;
%mend variable_selection;

/* Macro: Calculate Gini coefficient */
%macro calculate_gini(data=, actual=, predicted=);
    proc sql;
        create table work._gini_calc as
        select 
            &actual as Actual,
            &predicted as Predicted
        from &data
        order by Predicted desc;
    quit;
    
    data work._gini_calc2;
        set work._gini_calc;
        retain cum_bad 0 cum_total 0;
        cum_bad + Actual;
        cum_total + 1;
        bad_rate = cum_bad / sum(Actual);
        total_rate = cum_total / _N_;
    run;
    
    proc sql;
        select 2 * sum(bad_rate * (total_rate - lag(total_rate))) - 1 as Gini
        from work._gini_calc2;
    quit;
    
    /* Clean up */
    proc datasets lib=work nolist;
        delete _gini_calc _gini_calc2;
    quit;
%mend calculate_gini;

/* ===================================================================
   PART 8: REPORTING MACROS
   =================================================================== */

/* Macro: Generate model performance report */
%macro model_performance_report(model_data=, actual=, predicted=, report_name=);
    ods pdf file="&OUTPUT_PATH/&report_name._&CALCULATION_DATE_FMT..pdf";
    
    title "Model Performance Report - &report_name";
    title2 "Generated on %sysfunc(today(), worddate.)";
    
    /* ROC Curve */
    proc logistic data=&model_data;
        model &actual(event='1') = &predicted;
        roc 'ROC Curve' pred=&predicted;
        ods select ROCcurve;
    run;
    
    /* Classification metrics */
    proc freq data=&model_data;
        tables &actual*&predicted / measures;
    run;
    
    /* Distribution comparison */
    proc univariate data=&model_data;
        class &actual;
        var &predicted;
        histogram &predicted / nrows=2;
    run;
    
    ods pdf close;
    
    %put NOTE: Performance report saved to &OUTPUT_PATH/&report_name._&CALCULATION_DATE_FMT..pdf;
%mend model_performance_report;

/* Macro: Generate ECL summary report */
%macro ecl_summary_report(ecl_data=);
    proc tabulate data=&ecl_data;
        class product_type stage_ifrs9 scenario_name;
        var ecl_12m ecl_lifetime ead;
        
        table product_type='Product' all='Total',
              (stage_ifrs9='IFRS 9 Stage' all='All Stages') *
              (n='Count' ead='EAD'*sum='Total'*f=comma18.
               ecl_12m='12M ECL'*sum='Total'*f=comma18.
               ecl_lifetime='Lifetime ECL'*sum='Total'*f=comma18.);
        
        table scenario_name='Scenario',
              (n='Count' ead='EAD'*sum='Total'*f=comma18.
               ecl_12m='12M ECL'*sum='Total'*f=comma18.
               ecl_lifetime='Lifetime ECL'*sum='Total'*f=comma18.);
    run;
%mend ecl_summary_report;

/* ===================================================================
   PART 9: MAIN EXECUTION CONTROL MACROS
   =================================================================== */

/* Macro: Initialize project environment */
%macro initialize_project();
    %put NOTE: ====================================;
    %put NOTE: Initializing ECL Model Project;
    %put NOTE: Version: &MODEL_VERSION;
    %put NOTE: Date: %sysfunc(today(), worddate.);
    %put NOTE: ====================================;
    
    /* Check if required directories exist */
    %if %sysfunc(fileexist(&SAMPLE_PATH)) = 0 %then %do;
        %put ERROR: Sample data path does not exist: &SAMPLE_PATH;
        %put ERROR: Please ensure the following directory exists:;
        %put ERROR: &SAMPLE_PATH;
        %put ERROR: ;
        %put ERROR: For Windows users:;
        %put ERROR: 1. Check that PROJECT_ROOT is set correctly (e.g., C:\YumECL-SAS);
        %put ERROR: 2. Ensure the data\sample folder exists under your project root;
        %put ERROR: 3. Verify the path uses the correct drive letter;
        %abort cancel;
    %end;
    %else %do;
        %put NOTE: Sample data path found: &SAMPLE_PATH;
    %end;
    
    %if %sysfunc(fileexist(&OUTPUT_PATH)) = 0 %then %do;
        %put WARNING: Output path does not exist: &OUTPUT_PATH;
        %put WARNING: Attempting to create directory...;
        
        /* Try to create directory using different methods */
        %if &SYSSCP = WIN %then %do;
            /* Windows directory creation */
            data _null_;
                rc = dcreate("output", "&DATA_PATH");
            run;
        %end;
        %else %do;
            /* Unix/Linux directory creation */
            x "mkdir -p &OUTPUT_PATH";
        %end;
        
        /* Check again */
        %if %sysfunc(fileexist(&OUTPUT_PATH)) = 0 %then %do;
            %put WARNING: Could not create output directory. Please create manually: &OUTPUT_PATH;
        %end;
        %else %do;
            %put NOTE: Output directory created successfully: &OUTPUT_PATH;
        %end;
    %end;
    %else %do;
        %put NOTE: Output path found: &OUTPUT_PATH;
    %end;
    
    /* Check for required input files */
    %let required_files = loans payment_history macro_data stress_test_scenarios;
    %let current_date = %sysfunc(putn(%sysfunc(today()), yymmddn8.));
    %let sep = %str(\);
    %if &SYSSCP ne WIN %then %let sep = %str(/);
    
    %put NOTE: Looking for data files with date: &current_date;
    
    %do i = 1 %to %sysfunc(countw(&required_files));
        %let file = %scan(&required_files, &i);
        %let full_path = &SAMPLE_PATH&sep&file._&current_date..csv;
        
        %if %sysfunc(fileexist(&full_path)) = 1 %then %do;
            %put NOTE: Found file: &file._&current_date..csv;
        %end;
        %else %do;
            %put WARNING: Required file not found: &file._&current_date..csv;
            %put WARNING: Looking for alternative files with pattern: &file._*.csv;
            
            /* Try to find any file matching the pattern */
            filename filelist pipe "dir ""&SAMPLE_PATH&sep&file._*.csv"" /b 2>nul" %if &SYSSCP = WIN %then lrecl=256;
                                   %else pipe "ls &SAMPLE_PATH&sep&file._*.csv 2>/dev/null";;
            
            data _null_;
                infile filelist;
                input filename $100.;
                call symputx('found_file', filename);
                stop;
            run;
            
            %if %symexist(found_file) %then %do;
                %put NOTE: Alternative file found: &found_file;
                %put NOTE: Consider renaming to: &file._&current_date..csv;
            %end;
        %end;
    %end;
    
    %put NOTE: Project initialization complete.;
%mend initialize_project;

/* Macro: Clean up temporary datasets */
%macro cleanup_temp(lib=WORK);
    proc datasets lib=&lib kill nolist;
    quit;
    %put NOTE: Temporary datasets in &lib cleaned up.;
%mend cleanup_temp;

/* ===================================================================
   PART 10: LOGGING AND ERROR HANDLING
   =================================================================== */

/* Macro: Custom error handler */
%macro error_handler(step=);
    %if &syserr > 0 %then %do;
        %put ERROR: ====================================;
        %put ERROR: Error occurred in step: &step;
        %put ERROR: System error code: &syserr;
        %put ERROR: System error message: &syserrortext;
        %put ERROR: ====================================;
        
        /* Save error log */
        proc printto log="&LOG_PATH/error_&CALCULATION_DATE_FMT..log" new;
        run;
        
        %abort cancel;
    %end;
%mend error_handler;

/* Macro: Log execution time */
%macro log_runtime(step=);
    %local start_time end_time runtime;
    %let start_time = %sysfunc(datetime());
    
    /* Execute the step (passed as parameter) */
    &step;
    
    %let end_time = %sysfunc(datetime());
    %let runtime = %sysevalf(&end_time - &start_time);
    
    %put NOTE: Step completed in %sysfunc(putn(&runtime, time12.2));
%mend log_runtime;

/* ===================================================================
   INITIALIZATION
   =================================================================== */

/* Run initialization */
%initialize_project();

/* Set up format library */
proc format;
    /* IFRS 9 Stage formats */
    value stage
        1 = 'Stage 1 - Performing'
        2 = 'Stage 2 - Underperforming'
        3 = 'Stage 3 - Non-performing';
    
    /* Product type formats */
    value $product
        'Mortgage' = 'Mortgage Loans'
        'Auto' = 'Auto Loans'
        'Credit Card' = 'Credit Cards'
        'Personal' = 'Personal Loans'
        'HELOC' = 'Home Equity Line of Credit';
    
    /* Risk segment formats */
    value $risk_seg
        'LOW' = 'Low Risk'
        'MEDIUM' = 'Medium Risk'
        'HIGH' = 'High Risk';
    
    /* Province formats */
    value $prov
        'ON' = 'Ontario'
        'BC' = 'British Columbia'
        'AB' = 'Alberta'
        'QC' = 'Quebec'
        'MB' = 'Manitoba'
        'SK' = 'Saskatchewan'
        'NS' = 'Nova Scotia'
        'NB' = 'New Brunswick'
        'NL' = 'Newfoundland and Labrador'
        'PE' = 'Prince Edward Island';
run;

%put NOTE: ====================================;
%put NOTE: Environment setup complete!;
%put NOTE: Ready to run ECL model pipeline;
%put NOTE: ====================================;

/* End of setup program */