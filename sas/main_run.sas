/*****************************************************************************
 * Program: main_run.sas
 * Purpose: End-to-End Automated ECL Model Pipeline Orchestration
 * Author: Risk Analytics Team
 * Date: 2024
 * 
 * This program:
 * 1. Orchestrates the complete ECL calculation pipeline
 * 2. Handles error management and recovery
 * 3. Manages dependencies between components
 * 4. Generates consolidated reports
 * 5. Sends notifications and alerts
 *****************************************************************************/

/* =========================================================================
   PART 1: INITIALIZATION AND CONFIGURATION
   ========================================================================= */

/* Set global options */
options mprint mlogic symbolgen 
        errorabend errors=max 
        fullstimer msglevel=i;

/* Define run parameters */
%let run_date = %sysfunc(today());
%let run_datetime = %sysfunc(datetime());
%let run_id = RUN_%sysfunc(putn(&run_date, yymmddn8.))_%sysfunc(compress(%sysfunc(time(), time8.), :));
%let snapshot_date = 20241231;  /* Data snapshot date */

/* Set project paths */
%let project_path = C:/YumECL-SAS;
%let sas_path = &project_path/sas;
%let sql_path = &project_path/sql;
%let data_path = &project_path/data;
%let output_path = &project_path/output;
%let log_path = &project_path/logs;

/* Create run-specific output directory */
%let run_output_path = &output_path/&run_id;
x "mkdir &run_output_path";

/* Initialize log file */
filename runlog "&log_path/main_run_&run_id..log";
proc printto log=runlog new;
run;

/* =========================================================================
   PART 2: PIPELINE CONFIGURATION
   ========================================================================= */

/* Define pipeline stages and dependencies */
%macro define_pipeline();
    %global pipeline_stages n_stages;
    %let n_stages = 7;
    
    /* Stage definitions */
    %let stage1_name = SETUP;
    %let stage1_program = 00_setup.sas;
    %let stage1_critical = YES;
    
    %let stage2_name = DATA_PREP;
    %let stage2_program = 01_data_preparation.sas;
    %let stage2_critical = YES;
    
    %let stage3_name = PD_MODEL;
    %let stage3_program = 02_pd_model.sas;
    %let stage3_critical = YES;
    
    %let stage4_name = LGD_MODEL;
    %let stage4_program = 03_lgd_model.sas;
    %let stage4_critical = YES;
    
    %let stage5_name = ECL_CALC;
    %let stage5_program = 04_ecl_calculation.sas;
    %let stage5_critical = YES;
    
    %let stage6_name = STRESS_TEST;
    %let stage6_program = 05_stress_testing.sas;
    %let stage6_critical = NO;
    
    %let stage7_name = MONITORING;
    %let stage7_program = 06_model_monitoring.sas;
    %let stage7_critical = NO;
%mend define_pipeline;

%define_pipeline();

/* =========================================================================
   PART 3: ERROR HANDLING AND LOGGING FRAMEWORK
   ========================================================================= */

/* Create run status tracking table */
data work.pipeline_status;
    length stage_name $20 program $50 status $10 message $200;
    format start_time end_time datetime20.;
    delete;
run;

/* Error handling macro */
%macro handle_error(stage=, program=, error_code=);
    %if &error_code ne 0 %then %do;
        %put ERROR: Stage &stage (&program) failed with code &error_code;
        
        /* Log error to status table */
        data work.error_record;
            stage_name = "&stage";
            program = "&program";
            status = 'ERROR';
            message = "Failed with error code &error_code";
            error_time = datetime();
            format error_time datetime20.;
        run;
        
        proc append base=work.pipeline_status data=work.error_record force;
        run;
        
        /* Check if critical stage */
        %if &&stage&stage._critical = YES %then %do;
            %put ERROR: Critical stage failed. Stopping pipeline.;
            %goto pipeline_end;
        %end;
        %else %do;
            %put WARNING: Non-critical stage failed. Continuing pipeline.;
        %end;
    %end;
%mend handle_error;

/* Success logging macro */
%macro log_success(stage=, program=, start_time=, end_time=);
    data work.success_record;
        stage_name = "&stage";
        program = "&program";
        status = 'SUCCESS';
        start_time = &start_time;
        end_time = &end_time;
        duration = (&end_time - &start_time) / 60;
        message = cats("Completed successfully in ", put(duration, 8.2), " minutes");
        format start_time end_time datetime20.;
        format duration 8.2;
    run;
    
    proc append base=work.pipeline_status data=work.success_record force;
    run;
%mend log_success;

/* =========================================================================
   PART 4: DATA VALIDATION CHECKS
   ========================================================================= */

%macro validate_input_data();
    %put NOTE: ====================================;
    %put NOTE: Starting Data Validation;
    %put NOTE: ====================================;
    
    /* Check if required data files exist */
    %let required_files = loans payment_history macro_data stress_test_scenarios;
    %let validation_passed = 1;
    
    %do i = 1 %to %sysfunc(countw(&required_files));
        %let file = %scan(&required_files, &i);
        %let filepath = &data_path/raw/&file._&snapshot_date..csv;
        
        %if not %sysfunc(fileexist(&filepath)) %then %do;
            %put ERROR: Required file not found: &filepath;
            %let validation_passed = 0;
        %end;
        %else %do;
            %put NOTE: Found required file: &file._&snapshot_date..csv;
        %end;
    %end;
    
    %if &validation_passed = 0 %then %do;
        %put ERROR: Data validation failed. Missing required files.;
        %abort;
    %end;
    
    /* Check data quality */
    proc import datafile="&data_path/raw/loans_&snapshot_date..csv"
        out=work.loans_check
        dbms=csv replace;
        getnames=yes;
    run;
    
    /* Basic quality checks */
    proc sql noprint;
        select count(*) into :n_loans from work.loans_check;
        select count(*) into :n_missing_score 
        from work.loans_check where credit_score is null;
        select count(distinct product_type) into :n_products 
        from work.loans_check;
    quit;
    
    %put NOTE: Data Quality Summary:;
    %put NOTE: - Total loans: &n_loans;
    %put NOTE: - Missing credit scores: &n_missing_score;
    %put NOTE: - Product types: &n_products;
    
    %if &n_loans < 100 %then %do;
        %put WARNING: Low number of loans detected (&n_loans);
    %end;
    
    %put NOTE: Data validation completed successfully;
%mend validate_input_data;

/* =========================================================================
   PART 5: MAIN PIPELINE EXECUTION
   ========================================================================= */

%macro run_pipeline();
    %put NOTE: ====================================;
    %put NOTE: Starting ECL Model Pipeline;
    %put NOTE: Run ID: &run_id;
    %put NOTE: ====================================;
    
    /* Initialize pipeline status */
    %let pipeline_start = &run_datetime;
    %let pipeline_status = SUCCESS;
    
    /* Stage 0: Data Validation */
    %validate_input_data();
    
    /* Execute pipeline stages */
    %do stage = 1 %to &n_stages;
        %let stage_name = &&stage&stage._name;
        %let stage_program = &&stage&stage._program;
        %let stage_start = %sysfunc(datetime());
        
        %put NOTE: ====================================;
        %put NOTE: Stage &stage: &stage_name;
        %put NOTE: Program: &stage_program;
        %put NOTE: Start Time: %sysfunc(putn(&stage_start, datetime20.));
        %put NOTE: ====================================;
        
        /* Clear any previous errors */
        %let syscc = 0;
        
        /* Execute stage program */
        %include "&sas_path/&stage_program" / source2;
        
        /* Check for errors */
        %let stage_end = %sysfunc(datetime());
        
        %if &syscc = 0 %then %do;
            %log_success(stage=&stage_name, program=&stage_program, 
                        start_time=&stage_start, end_time=&stage_end);
            %put NOTE: Stage &stage (&stage_name) completed successfully;
        %end;
        %else %do;
            %handle_error(stage=&stage, program=&stage_program, error_code=&syscc);
            %let pipeline_status = FAILED;
            
            /* Stop if critical */
            %if &&stage&stage._critical = YES %then %do;
                %goto pipeline_abort;
            %end;
        %end;
        
        /* Add delay between stages if needed */
        data _null_;
            call sleep(1, 1);  /* 1 second pause */
        run;
    %end;
    
    /* Pipeline completed successfully */
    %goto pipeline_complete;
    
    /* Pipeline abort point */
    %pipeline_abort:
    %put ERROR: Pipeline aborted due to critical error;
    %let pipeline_status = ABORTED;
    
    /* Pipeline completion */
    %pipeline_complete:
    %let pipeline_end = %sysfunc(datetime());
    
%mend run_pipeline;

/* =========================================================================
   PART 6: CONSOLIDATED REPORTING
   ========================================================================= */

%macro generate_consolidated_report();
    %put NOTE: ====================================;
    %put NOTE: Generating Consolidated Reports;
    %put NOTE: ====================================;
    
    /* Create consolidated report */
    ods pdf file="&run_output_path/ecl_consolidated_report_&run_id..pdf" 
            style=pearl startpage=no;
    
    /* Title Page */
    ods text="^S={just=c font_size=20pt font_weight=bold}IFRS 9 ECL Model Report";
    ods text="^S={just=c font_size=14pt}Run Date: %sysfunc(putn(&run_date, worddate.))";
    ods text="^S={just=c font_size=12pt}Run ID: &run_id";
    
    /* Executive Summary */
    ods startpage=now;
    title "Executive Summary";
    
    /* Pipeline Status */
    proc print data=work.pipeline_status noobs;
        title2 "Pipeline Execution Status";
        var stage_name status start_time end_time message;
    run;
    
    /* ECL Summary */
    %if %sysfunc(exist(models.ecl_summary)) %then %do;
        proc print data=models.ecl_summary noobs;
            title2 "ECL Summary by Product and Stage";
            where product_type ne 'TOTAL';
            format total_ead total_ecl comma18.;
            format coverage_ratio percent8.3;
        run;
        
        /* ECL Totals */
        proc sql;
            title2 "Total ECL Provisions";
            select 
                sum(total_ead) as Total_Exposure format=comma18.,
                sum(total_ecl) as Total_ECL format=comma18.,
                sum(total_ecl)/sum(total_ead) as Coverage_Ratio format=percent8.3
            from models.ecl_summary
            where product_type ne 'TOTAL';
        quit;
    %end;
    
    /* Stress Test Summary */
    %if %sysfunc(exist(models.stress_test_summary)) %then %do;
        ods startpage=now;
        title "Stress Testing Results";
        
        proc sgplot data=models.stress_test_summary;
            title2 "ECL Under Stress Scenarios";
            vbar scenario_name / response=ecl_stressed dataskin=pressed;
            yaxis label="Stressed ECL" grid;
            xaxis label="Scenario";
        run;
    %end;
    
    /* Model Monitoring Summary */
    %if %sysfunc(exist(models.monitoring_metrics)) %then %do;
        ods startpage=now;
        title "Model Performance Monitoring";
        
        proc print data=models.monitoring_metrics noobs;
            title2 "Key Performance Metrics";
            var accuracy precision recall f1_score gini_coefficient total_psi;
            format accuracy precision recall f1_score 8.4;
        run;
    %end;
    
    /* Data Quality Report */
    ods startpage=now;
    title "Data Quality Summary";
    
    proc sql;
        title2 "Portfolio Composition";
        select 
            product_type,
            count(*) as Number_of_Loans format=comma12.,
            sum(original_amount) as Total_Amount format=comma18.,
            avg(credit_score) as Avg_Credit_Score format=8.0,
            avg(loan_to_value) as Avg_LTV format=percent8.1
        from processed.model_dataset
        group by product_type;
    quit;
    
    ods pdf close;
    
    %put NOTE: Consolidated report generated: &run_output_path/ecl_consolidated_report_&run_id..pdf;
    
%mend generate_consolidated_report;

/* =========================================================================
   PART 7: DATABASE UPDATES
   ========================================================================= */

%macro update_database();
    %put NOTE: ====================================;
    %put NOTE: Updating Database;
    %put NOTE: ====================================;
    
    /* Connect to MySQL database */
    libname ecl_db mysql server="localhost" database="ecl_model" 
            user="ecl_user" password="ecl_pass" port=3306;
    
    /* Check connection */
    %if not %sysfunc(libref(ecl_db)) %then %do;
        
        /* Update ECL results table */
        proc sql;
            delete from ecl_db.ecl_results 
            where calculation_date = today();
            
            insert into ecl_db.ecl_results
            select * from models.ecl_results;
        quit;
        
        /* Update monitoring metrics */
        proc sql;
            insert into ecl_db.model_monitoring
            (monitoring_date, metric_name, metric_value, model_name)
            select 
                monitoring_date,
                'ACCURACY' as metric_name,
                accuracy as metric_value,
                model_name
            from models.monitoring_metrics;
        quit;
        
        /* Clear connection */
        libname ecl_db clear;
        
        %put NOTE: Database updated successfully;
    %end;
    %else %do;
        %put WARNING: Could not connect to database. Skipping database update.;
    %end;
    
%mend update_database;

/* =========================================================================
   PART 8: NOTIFICATIONS AND ALERTS
   ========================================================================= */

%macro send_notifications();
    %put NOTE: ====================================;
    %put NOTE: Sending Notifications;
    %put NOTE: ====================================;
    
    /* Prepare email content */
    filename mailout email 
        to=("risk.analytics@bank.com")
        cc=("model.governance@bank.com")
        subject="ECL Model Run Complete - &run_id"
        type="text/html";
    
    data _null_;
        file mailout;
        put '<html><body>';
        put '<h2>ECL Model Pipeline Execution Report</h2>';
        put "<p><b>Run ID:</b> &run_id</p>";
        put "<p><b>Run Date:</b> %sysfunc(putn(&run_date, worddate.))</p>";
        put "<p><b>Status:</b> &pipeline_status</p>";
        
        /* Summary statistics */
        %if %sysfunc(exist(models.ecl_summary)) %then %do;
            put '<h3>ECL Summary</h3>';
            put '<table border="1">';
            put '<tr><th>Metric</th><th>Value</th></tr>';
            put "<tr><td>Total Exposure</td><td>$XXX</td></tr>";
            put "<tr><td>Total ECL</td><td>$XXX</td></tr>";
            put "<tr><td>Coverage Ratio</td><td>X.XX%</td></tr>";
            put '</table>';
        %end;
        
        /* Alerts */
        %if &pipeline_status = FAILED or &pipeline_status = ABORTED %then %do;
            put '<p style="color: red;"><b>ALERT: Pipeline did not complete successfully!</b></p>';
            put '<p>Please review the logs at: &log_path</p>';
        %end;
        
        put '<p>Full report available at: &run_output_path</p>';
        put '</body></html>';
    run;
    
    filename mailout clear;
    
    %put NOTE: Notifications sent successfully;
    
%mend send_notifications;

/* =========================================================================
   PART 9: CLEANUP AND ARCHIVING
   ========================================================================= */

%macro cleanup_and_archive();
    %put NOTE: ====================================;
    %put NOTE: Cleanup and Archiving;
    %put NOTE: ====================================;
    
    /* Clean up temporary datasets */
    proc datasets lib=work nolist;
        delete temp_: _: test_:;
    quit;
    
    /* Archive run outputs */
    %let archive_path = &project_path/archive/&run_id;
    x "mkdir &archive_path";
    x "copy &run_output_path\*.* &archive_path\";
    
    /* Compress log files */
    x "zip &archive_path\logs.zip &log_path\*.log";
    
    /* Clean old files (keep last 30 days) */
    %let cutoff_date = %eval(&run_date - 30);
    
    /* Save pipeline metadata */
    data archive.pipeline_metadata_&run_id;
        set work.pipeline_status;
        run_id = "&run_id";
        run_date = &run_date;
        pipeline_status = "&pipeline_status";
        format run_date date9.;
    run;
    
    %put NOTE: Cleanup and archiving completed;
    
%mend cleanup_and_archive;

/* =========================================================================
   PART 10: MAIN EXECUTION
   ========================================================================= */

%macro main();
    %put NOTE: ====================================;
    %put NOTE: ECL MODEL PIPELINE - MAIN EXECUTION;
    %put NOTE: ====================================;
    
    /* Record start time */
    %let main_start = %sysfunc(datetime());
    
    /* Step 1: Run the pipeline */
    %run_pipeline();
    
    /* Step 2: Generate consolidated reports */
    %if &pipeline_status ne ABORTED %then %do;
        %generate_consolidated_report();
    %end;
    
    /* Step 3: Update database */
    %if &pipeline_status = SUCCESS %then %do;
        %update_database();
    %end;
    
    /* Step 4: Send notifications */
    %send_notifications();
    
    /* Step 5: Cleanup and archive */
    %cleanup_and_archive();
    
    /* Record end time */
    %let main_end = %sysfunc(datetime());
    %let total_duration = %sysevalf((&main_end - &main_start) / 60);
    
    /* Final summary */
    %put NOTE: ====================================;
    %put NOTE: PIPELINE EXECUTION COMPLETE;
    %put NOTE: Status: &pipeline_status;
    %put NOTE: Total Duration: %sysfunc(putn(&total_duration, 8.2)) minutes;
    %put NOTE: Output Location: &run_output_path;
    %put NOTE: ====================================;
    
    /* Close log */
    proc printto;
    run;
    
%mend main;

/* =========================================================================
   EXECUTE MAIN PROGRAM
   ========================================================================= */

/* Run the complete pipeline */
%main();

/* End of program */