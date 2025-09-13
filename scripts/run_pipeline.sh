# 1. 生成样本数据
python scripts/generate_sample_data.py

# 2. 创建数据库架构
sqlcmd -i sql/01_create_schema.sql

# 3. 运行SAS主程序
sas sas/main_run.sas

# 4. 查看输出报告
open data/output/ecl_report_*.pdf