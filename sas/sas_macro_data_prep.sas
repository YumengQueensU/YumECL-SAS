/**********************************************************************
 * Program: 01_macro_data_preparation.sas
 * Purpose: 准备和整合加拿大宏观经济数据用于IFRS 9 ECL模型
 * Author: Risk Analytics Team
 * Date: 2024
 **********************************************************************/

/* 设置环境 */
%let data_path = /data/sample/macro_data;
%let output_path = /data/output;
%let start_date = '01JAN2020'd;
%let end_date = '31DEC2024'd;

/* 定义库 */
libname macro "&data_path";
libname output "&output_path";

/**********************************************************************
 * PART 1: 导入和处理CPI数据
 **********************************************************************/
proc import datafile="&data_path/CPI_Monthly-1810000401-eng.csv"
    out=work.cpi_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=8;
run;

/* 转换CPI数据格式 */
data work.cpi_clean;
    set work.cpi_raw;
    where "Products and product groups 3 4"n = "All-items";
    
    /* 转换月度数据 */
    array months{*} "January 2020"n--"December 2024"n;
    
    do i = 1 to dim(months);
        date = intnx('month', '01JAN2020'd, i-1);
        CPI = input(months{i}, best.);
        if not missing(CPI) then output;
    end;
    
    keep date CPI;
    format date date9.;
run;

/* 计算通胀率 */
proc sort data=work.cpi_clean; by date; run;

data work.cpi_final;
    set work.cpi_clean;
    by date;
    
    /* 计算年同比通胀率 */
    CPI_lag12 = lag12(CPI);
    if not missing(CPI_lag12) then 
        Inflation_YoY = ((CPI / CPI_lag12) - 1) * 100;
    
    drop CPI_lag12;
run;

/**********************************************************************
 * PART 2: 导入和处理劳动力市场数据
 **********************************************************************/
proc import datafile="&data_path/Labour_Force-1410028701-eng.csv"
    out=work.labour_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=5;
run;

/* 提取失业率和就业率 */
data work.labour_clean;
    set work.labour_raw;
    
    /* 失业率 */
    if "Labour force characteristics"n = "Unemployment rate 16" then do;
        metric = "Unemployment_Rate";
        output;
    end;
    
    /* 就业率 */
    if "Labour force characteristics"n = "Employment rate 18" then do;
        metric = "Employment_Rate";
        output;
    end;
    
    keep metric "January 2020"n--"December 2024"n;
run;

/* 转置数据 */
proc transpose data=work.labour_clean out=work.labour_long;
    id metric;
    var "January 2020"n--"December 2024"n;
run;

data work.labour_final;
    set work.labour_long;
    
    /* 解析日期 */
    month_year = _NAME_;
    month_name = scan(month_year, 1, ' ');
    year = input(scan(month_year, 2, ' '), 4.);
    
    /* 转换为SAS日期 */
    select (month_name);
        when ('January') month_num = 1;
        when ('February') month_num = 2;
        when ('March') month_num = 3;
        when ('April') month_num = 4;
        when ('May') month_num = 5;
        when ('June') month_num = 6;
        when ('July') month_num = 7;
        when ('August') month_num = 8;
        when ('September') month_num = 9;
        when ('October') month_num = 10;
        when ('November') month_num = 11;
        when ('December') month_num = 12;
    end;
    
    date = mdy(month_num, 1, year);
    
    /* 转换为数值 */
    Unemployment_Rate = input(Unemployment_Rate, best.);
    Employment_Rate = input(Employment_Rate, best.);
    
    keep date Unemployment_Rate Employment_Rate;
    format date date9.;
run;

/**********************************************************************
 * PART 3: 导入和处理利率数据
 **********************************************************************/

/* 政策利率 */
proc import datafile="&data_path/Policy_Interest_Rate-V39079-sd-2020-01-01-ed-2024-12-31.csv"
    out=work.policy_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=6;
run;

/* 转换为月度平均 */
proc sql;
    create table work.policy_monthly as
    select 
        intnx('month', input(date, yymmdd10.), 0, 'E') as date format=date9.,
        mean(V39079) as Policy_Rate
    from work.policy_raw
    group by calculated date;
quit;

/* 基准利率 */
proc import datafile="&data_path/Prime_Rate-V80691311-sd-2020-01-01-ed-2024-12-31.csv"
    out=work.prime_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=6;
run;

/* 转换为月度平均 */
proc sql;
    create table work.prime_monthly as
    select 
        intnx('month', input(date, yymmdd10.), 0, 'E') as date format=date9.,
        mean(V80691311) as Prime_Rate
    from work.prime_raw
    group by calculated date;
quit;

/* 5年期抵押贷款利率 */
proc import datafile="&data_path/5Year_Conventional_Mortgage-V80691335-sd-2020-01-01-ed-2024-12-31.csv"
    out=work.mortgage_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=6;
run;

/* 转换为月度平均 */
proc sql;
    create table work.mortgage_monthly as
    select 
        intnx('month', input(date, yymmdd10.), 0, 'E') as date format=date9.,
        mean(V80691335) as Mortgage_5Y_Rate
    from work.mortgage_raw
    group by calculated date;
quit;

/* 合并利率数据 */
data work.rates_final;
    merge work.policy_monthly 
          work.prime_monthly 
          work.mortgage_monthly;
    by date;
    
    /* 计算利差 */
    Prime_Policy_Spread = Prime_Rate - Policy_Rate;
    Mortgage_Prime_Spread = Mortgage_5Y_Rate - Prime_Rate;
run;

/**********************************************************************
 * PART 4: 导入和处理汇率数据
 **********************************************************************/
proc import datafile="&data_path/FX_USD_CAD-sd-2020-01-01-ed-2024-12-31.csv"
    out=work.fx_raw
    dbms=csv
    replace;
    getnames=yes;
    datarow=6;
run;

/* 转换为月度平均 */
proc sql;
    create table work.fx_monthly as
    select 
        intnx('month', input(date, yymmdd10.), 0, 'E') as date format=date9.,
        mean(FXUSDCAD) as USD_CAD
    from work.fx_raw
    group by calculated date;
quit;

/* 计算汇率变化率 */
proc sort data=work.fx_monthly; by date; run;

data work.fx_final;
    set work.fx_monthly;
    by date;
    
    /* 月环比 */
    USD_CAD_lag1 = lag(USD_CAD);
    if not missing(USD_CAD_lag1) then 
        FX_Change_MoM = ((USD_CAD / USD_CAD_lag1) - 1) * 100;
    
    /* 年同比 */
    USD_CAD_lag12 = lag12(USD_CAD);
    if not missing(USD_CAD_lag12) then 
        FX_Change_YoY = ((USD_CAD / USD_CAD_lag12) - 1) * 100;
    
    drop USD_CAD_lag1 USD_CAD_lag12;
run;

/**********************************************************************
 * PART 5: 导入和处理房价数据
 **********************************************************************/
proc import datafile="&data_path/MLS_HPI_data_August_2025.csv"
    out=work.hpi_raw
    dbms=csv
    replace;
    getnames=yes;
run;

data work.hpi_clean;
    set work.hpi_raw;
    
    /* 解析日期和价格 */
    date = input(Date, anydtdte.);
    HPI_str = compress("Aggregate Composite MLS® HPI*"n, '$,');
    HPI = input(HPI_str, best.);
    
    keep date HPI;
    format date date9.;
run;

/* 计算房价变化率 */
proc sort data=work.hpi_clean; by date; run;

data work.hpi_final;
    set work.hpi_clean;
    by date;
    
    /* 月环比 */
    HPI_lag1 = lag(HPI);
    if not missing(HPI_lag1) then 
        HPI_Change_MoM = ((HPI / HPI_lag1) - 1) * 100;
    
    /* 年同比 */
    HPI_lag12 = lag12(HPI);
    if not missing(HPI_lag12) then 
        HPI_Change_YoY = ((HPI / HPI_lag12) - 1) * 100;
    
    drop HPI_lag1 HPI_lag12;
run;

/**********************************************************************
 * PART 6: 导入和处理油价数据
 **********************************************************************/
proc import datafile="&data_path/WCS_Oil_Prices_Alberta_1757748101538.csv"
    out=work.oil_raw
    dbms=csv
    replace;
    getnames=yes;
run;

/* 分离WTI和WCS价格 */
data work.wti work.wcs;
    set work.oil_raw;
    
    date = datepart(Date);
    
    if Type = 'WTI' then do;
        WTI_Price = Value;
        output work.wti;
    end;
    else if Type = 'WCS' then do;
        WCS_Price = Value;
        output work.wcs;
    end;
    
    format date date9.;
    keep date WTI_Price WCS_Price;
run;

/* 合并油价数据 */
data work.oil_final;
    merge work.wti(keep=date WTI_Price)
          work.wcs(keep=date WCS_Price);
    by date;
    
    /* 计算价差 */
    WCS_WTI_Spread = WCS_Price - WTI_Price;
    
    /* 计算WTI年同比变化 */
    WTI_lag12 = lag12(WTI_Price);
    if not missing(WTI_lag12) then 
        WTI_Change_YoY = ((WTI_Price / WTI_lag12) - 1) * 100;
    
    drop WTI_lag12;
run;

/**********************************************************************
 * PART 7: 整合所有数据
 **********************************************************************/
data output.macro_data_consolidated;
    merge work.cpi_final
          work.labour_final
          work.rates_final
          work.fx_final
          work.hpi_final
          work.oil_final;
    by date;
    
    /* 只保留指定日期范围 */
    where date between &start_date and &end_date;
    
    /* 填充缺失值（前向填充） */
    array numeric_vars{*} _numeric_;
    do i = 1 to dim(numeric_vars);
        if missing(numeric_vars{i}) then 
            numeric_vars{i} = lag(numeric_vars{i});
    end;
    drop i;
run;

/**********************************************************************
 * PART 8: 添加风险指标
 **********************************************************************/
proc stdize data=output.macro_data_consolidated 
            out=work.standardized 
            method=std;
    var Unemployment_Rate GDP_Growth_YoY;
run;

data output.macro_data_consolidated;
    merge output.macro_data_consolidated 
          work.standardized(rename=(Unemployment_Rate=Unemp_std 
                                   GDP_Growth_YoY=GDP_std));
    by date;
    
    /* 经济周期得分 */
    Economic_Cycle_Score = (-1 * Unemp_std + GDP_std) / 2;
    
    /* 信贷条件指标 */
    Credit_Conditions = Prime_Rate * 0.4 + 
                        Mortgage_5Y_Rate * 0.4 + 
                        Prime_Policy_Spread * 0.2;
    
    /* 房地产风险指标 */
    Housing_Risk_Score = HPI_Change_YoY * (-0.5) + 
                        Mortgage_5Y_Rate * 0.3 + 
                        Unemployment_Rate * 0.2;
    
    drop Unemp_std GDP_std;
run;

/**********************************************************************
 * PART 9: 创建压力测试场景
 **********************************************************************/

/* 计算基准场景（最近12个月平均） */
proc means data=output.macro_data_consolidated noprint;
    where date >= intnx('month', &end_date, -11);
    var Unemployment_Rate GDP_Growth_YoY HPI_Change_YoY 
        Policy_Rate WTI_Price;
    output out=work.baseline mean=;
run;

/* 创建压力测试场景 */
data output.stress_test_scenarios;
    set work.baseline;
    
    length Scenario $20;
    
    /* 基准场景 */
    Scenario = 'Baseline';
    output;
    
    /* 不利场景 */
    Scenario = 'Adverse';
    Unemployment_Rate = Unemployment_Rate + 3.0;
    GDP_Growth_YoY = GDP_Growth_YoY - 3.0;
    HPI_Change_YoY = -10.0;
    Policy_Rate = Policy_Rate + 2.0;
    output;
    
    /* 严重不利场景 */
    Scenario = 'Severely_Adverse';
    Unemployment_Rate = Unemployment_Rate + 2.0;  /* 总计+5% */
    GDP_Growth_YoY = -5.0;
    HPI_Change_YoY = -20.0;
    Policy_Rate = Policy_Rate + 1.0;  /* 总计+3% */
    WTI_Price = 40.0;
    output;
    
    drop _TYPE_ _FREQ_;
run;

/**********************************************************************
 * PART 10: 生成数据质量报告
 **********************************************************************/
ods html file="&output_path/macro_data_quality_report.html";

title "Macro Data Quality Report";

/* 数据概览 */
proc contents data=output.macro_data_consolidated;
run;

/* 描述性统计 */
proc means data=output.macro_data_consolidated 
           n nmiss mean std min max;
    var _numeric_;
run;

/* 缺失值分析 */
proc freq data=output.macro_data_consolidated;
    tables _numeric_ / missing;
run;

/* 关键变量的时间序列图 */
proc sgplot data=output.macro_data_consolidated;
    series x=date y=Unemployment_Rate;
    title "Unemployment Rate Over Time";
run;

proc sgplot data=output.macro_data_consolidated;
    series x=date y=Policy_Rate;
    series x=date y=Prime_Rate;
    series x=date y=Mortgage_5Y_Rate;
    title "Interest Rates Over Time";
run;

proc sgplot data=output.macro_data_consolidated;
    series x=date y=HPI_Change_YoY;
    title "Housing Price Index YoY Change";
run;

ods html close;

/* 输出数据字典 */
proc sql;
    create table output.macro_data_dictionary as
    select 
        name as Variable,
        type as Type,
        length as Length,
        label as Label,
        format as Format
    from dictionary.columns
    where libname='OUTPUT' and memname='MACRO_DATA_CONSOLIDATED';
quit;

/* 打印完成信息 */
%put NOTE: Macro data consolidation completed successfully;
%put NOTE: Output saved to &output_path;

/* 显示最终数据集信息 */
proc print data=output.stress_test_scenarios;
    title "Stress Test Scenarios";
run;