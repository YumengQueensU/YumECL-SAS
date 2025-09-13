#!/bin/bash

#######################################################################
# 加拿大宏观经济数据处理管道
# 用于IFRS 9 ECL模型和压力测试
#######################################################################

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 设置路径
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${BASE_DIR}/data"
SAMPLE_DIR="${DATA_DIR}/sample/macro_data"
OUTPUT_DIR="${DATA_DIR}/output"
SCRIPTS_DIR="${BASE_DIR}/scripts"
LOG_DIR="${BASE_DIR}/logs"

# 创建必要的目录
mkdir -p "${OUTPUT_DIR}"
mkdir -p "${LOG_DIR}"

# 日志文件
LOG_FILE="${LOG_DIR}/macro_data_pipeline_$(date +%Y%m%d_%H%M%S).log"

# 函数：打印带时间戳的消息
log_message() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "${LOG_FILE}"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "${LOG_FILE}"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "${LOG_FILE}"
}

# 函数：检查Python环境
check_python_env() {
    log_message "Checking Python environment..."
    
    # 检查Python版本
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        log_message "Python version: ${PYTHON_VERSION}"
    else
        log_error "Python 3 is not installed!"
        exit 1
    fi
    
    # 检查必要的包
    REQUIRED_PACKAGES=("pandas" "numpy" "matplotlib" "seaborn" "pyyaml")
    
    for package in "${REQUIRED_PACKAGES[@]}"; do
        if python3 -c "import ${package}" &> /dev/null; then
            log_message "✓ Package '${package}' is installed"
        else
            log_warning "Package '${package}' is not installed. Installing..."
            pip3 install "${package}" >> "${LOG_FILE}" 2>&1
        fi
    done
}

# 函数：检查数据文件
check_data_files() {
    log_message "Checking macro data files..."
    
    # 必需的数据文件列表
    REQUIRED_FILES=(
        "CPI_Monthly-1810000401-eng.csv"
        "Labour_Force-1410028701-eng.csv"
        "GDP-3610040201-eng.csv"
        "Policy_Interest_Rate-V39079-sd-2020-01-01-ed-2024-12-31.csv"
        "Prime_Rate-V80691311-sd-2020-01-01-ed-2024-12-31.csv"
        "5Year_Conventional_Mortgage-V80691335-sd-2020-01-01-ed-2024-12-31.csv"
        "FX_USD_CAD-sd-2020-01-01-ed-2024-12-31.csv"
        "MLS_HPI_data_August_2025.csv"
        "WCS_Oil_Prices_Alberta_1757748101538.csv"
    )
    
    MISSING_FILES=()
    
    for file in "${REQUIRED_FILES[@]}"; do
        if [ -f "${SAMPLE_DIR}/${file}" ]; then
            log_message "✓ Found: ${file}"
        else
            log_error "✗ Missing: ${file}"
            MISSING_FILES+=("${file}")
        fi
    done
    
    if [ ${#MISSING_FILES[@]} -gt 0 ]; then
        log_error "Missing ${#MISSING_FILES[@]} required files!"
        log_error "Please ensure all macro data files are in: ${SAMPLE_DIR}"
        exit 1
    fi
}

# 函数：运行Python数据整合脚本
run_python_consolidation() {
    log_message "Starting Python data consolidation..."
    
    # 创建Python脚本（如果不存在）
    if [ ! -f "${SCRIPTS_DIR}/consolidate_macro_data.py" ]; then
        log_warning "Python script not found. Creating from artifact..."
        # 这里应该复制artifact中的Python代码
    fi
    
    # 运行Python脚本
    cd "${BASE_DIR}"
    python3 "${SCRIPTS_DIR}/consolidate_macro_data.py" >> "${LOG_FILE}" 2>&1
    
    if [ $? -eq 0 ]; then
        log_message "✓ Python data consolidation completed successfully"
    else
        log_error "Python data consolidation failed!"
        exit 1
    fi
}

# 函数：运行数据验证
run_data_validation() {
    log_message "Running data validation..."
    
    cd "${BASE_DIR}"
    python3 "${SCRIPTS_DIR}/validate_and_visualize.py" >> "${LOG_FILE}" 2>&1
    
    if [ $? -eq 0 ]; then
        log_message "✓ Data validation completed successfully"
    else
        log_warning "Data validation encountered issues - check log for details"
    fi
}

# 函数：运行SAS处理（如果SAS可用）
run_sas_processing() {
    log_message "Checking for SAS installation..."
    
    if command -v sas &> /dev/null; then
        log_message "SAS found. Running SAS data preparation..."
        
        cd "${BASE_DIR}"
        sas -log "${LOG_DIR}/sas_macro_prep.log" \
            -print "${LOG_DIR}/sas_macro_prep.lst" \
            "${BASE_DIR}/sas/01_macro_data_preparation.sas"
        
        if [ $? -eq 0 ]; then
            log_message "✓ SAS processing completed successfully"
        else
            log_warning "SAS processing encountered issues - check SAS log"
        fi
    else
        log_warning "SAS not found - skipping SAS processing"
        log_warning "To enable SAS processing, ensure SAS is installed and in PATH"
    fi
}

# 函数：生成数据质量报告
generate_reports() {
    log_message "Generating data quality reports..."
    
    # 创建报告目录
    REPORT_DIR="${OUTPUT_DIR}/reports_$(date +%Y%m%d)"
    mkdir -p "${REPORT_DIR}"
    
    # 移动生成的报告文件
    if [ -f "${OUTPUT_DIR}/correlation_matrix.png" ]; then
        mv "${OUTPUT_DIR}"/*.png "${REPORT_DIR}/"
        log_message "✓ Visualization files moved to ${REPORT_DIR}"
    fi
    
    if [ -f "${OUTPUT_DIR}/data_quality_report.txt" ]; then
        cp "${OUTPUT_DIR}/data_quality_report.txt" "${REPORT_DIR}/"
        log_message "✓ Quality report copied to ${REPORT_DIR}"
    fi
    
    # 生成执行摘要
    cat > "${REPORT_DIR}/execution_summary.txt" << EOF
================================================================================
MACRO DATA PROCESSING PIPELINE - EXECUTION SUMMARY
================================================================================
Date: $(date '+%Y-%m-%d %H:%M:%S')
User: $(whoami)
Directory: ${BASE_DIR}

DATA FILES PROCESSED:
$(ls -la "${SAMPLE_DIR}"/*.csv | wc -l) CSV files

OUTPUT FILES GENERATED:
- macro_data_consolidated.csv
- stress_test_scenarios.csv
- macro_data_dictionary.csv
- summary_statistics.csv

VISUALIZATIONS:
- correlation_matrix.png
- macro_indicators_timeseries.png
- stress_scenarios.png

LOG FILE: ${LOG_FILE}
================================================================================
EOF
    
    log_message "✓ Execution summary created"
}

# 函数：清理临时文件
cleanup() {
    log_message "Cleaning up temporary files..."
    
    # 删除Python缓存
    find "${BASE_DIR}" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
    
    # 删除临时文件
    rm -f "${BASE_DIR}"/*.tmp 2>/dev/null
    
    log_message "✓ Cleanup completed"
}

# 主执行流程
main() {
    echo "========================================================================"
    echo " CANADIAN MACRO DATA PROCESSING PIPELINE"
    echo " For IFRS 9 ECL Model and Stress Testing"
    echo "========================================================================"
    echo ""
    
    log_message "Starting macro data processing pipeline..."
    
    # 步骤1：检查环境
    log_message "Step 1/6: Environment check"
    check_python_env
    
    # 步骤2：检查数据文件
    log_message "Step 2/6: Data file verification"
    check_data_files
    
    # 步骤3：运行Python数据整合
    log_message "Step 3/6: Data consolidation"
    run_python_consolidation
    
    # 步骤4：运行数据验证
    log_message "Step 4/6: Data validation"
    run_data_validation
    
    # 步骤5：运行SAS处理（可选）
    log_message "Step 5/6: SAS processing (optional)"
    run_sas_processing
    
    # 步骤6：生成报告
    log_message "Step 6/6: Report generation"
    generate_reports
    
    # 清理
    cleanup
    
    # 完成
    echo ""
    echo "========================================================================"
    log_message "Pipeline execution completed successfully!"
    echo ""
    echo "Output files location: ${OUTPUT_DIR}"
    echo "Log file: ${LOG_FILE}"
    echo ""
    echo "Next steps:"
    echo "1. Review data quality report in ${OUTPUT_DIR}"
    echo "2. Check visualizations for data trends"
    echo "3. Proceed with ECL model development using consolidated data"
    echo "========================================================================"
}

# 错误处理
trap 'log_error "Pipeline interrupted!"; exit 1' INT TERM

# 运行主程序
main

# 返回状态
exit 0