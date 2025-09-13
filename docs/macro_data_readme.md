# 加拿大宏观经济数据整理方案

## 📋 概述

本文档说明如何整理和处理用于IFRS 9 ECL模型和压力测试的加拿大宏观经济数据。

## 🗂️ 数据文件清单

您已上传的9个数据文件包含了模型所需的所有宏观经济指标：

| 序号 | 文件名 | 数据内容 | 频率 | 来源 |
|------|--------|----------|------|------|
| 1 | CPI_Monthly-1810000401-eng.csv | 消费者价格指数 | 月度 | 加拿大统计局 |
| 2 | Labour_Force-1410028701-eng.csv | 失业率、就业率 | 月度 | 加拿大统计局 |
| 3 | GDP-3610040201-eng.csv | GDP数据 | 年度 | 加拿大统计局 |
| 4 | Policy_Interest_Rate-V39079-*.csv | 隔夜利率目标 | 日度 | 加拿大银行 |
| 5 | Prime_Rate-V80691311-*.csv | 基准利率 | 周度 | 加拿大银行 |
| 6 | 5Year_Conventional_Mortgage-*.csv | 5年期抵押贷款利率 | 周度 | 加拿大银行 |
| 7 | FX_USD_CAD-*.csv | 美元兑加元汇率 | 日度 | 加拿大银行 |
| 8 | MLS_HPI_data_August_2025.csv | 房价指数 | 月度 | CREA |
| 9 | WCS_Oil_Prices_Alberta_*.csv | WTI/WCS油价 | 月度 | 阿尔伯塔省 |

## 🔧 数据处理步骤

### 1. 环境准备

```bash
# 安装必要的Python包
pip install pandas numpy matplotlib seaborn pyyaml

# 创建目录结构
mkdir -p data/sample/macro_data
mkdir -p data/output
mkdir -p scripts
mkdir -p logs
```

### 2. 数据整合流程

#### 2.1 频率统一
- **目标频率**: 月度
- **转换方法**:
  - 日度→月度: 取月平均值
  - 周度→月度: 取月平均值
  - 年度→月度: 线性插值

#### 2.2 关键变量计算

| 变量名 | 计算方法 | 用途 |
|--------|----------|------|
| Inflation_YoY | CPI年同比变化率 | 通胀压力 |
| GDP_Growth_YoY | GDP年同比增长率 | 经济增长 |
| HPI_Change_YoY | 房价指数年同比变化 | 房地产风险 |
| FX_Change_YoY | 汇率年同比变化 | 汇率风险 |
| Prime_Policy_Spread | 基准利率-政策利率 | 信贷利差 |
| WCS_WTI_Spread | WCS-WTI价差 | 能源价格风险 |

#### 2.3 风险指标构建

```python
# 经济周期得分
Economic_Cycle_Score = (-0.5 * 标准化失业率 + 0.5 * 标准化GDP增长) 

# 信贷条件指标
Credit_Conditions = 0.4 * Prime_Rate + 0.4 * Mortgage_5Y_Rate + 0.2 * Spread

# 房地产风险得分
Housing_Risk_Score = -0.5 * HPI_Change_YoY + 0.3 * Mortgage_Rate + 0.2 * Unemployment
```

## 🚀 执行方案

### 方案1: Python脚本执行

```bash
# 运行数据整合脚本
python scripts/consolidate_macro_data.py

# 运行验证和可视化
python scripts/validate_and_visualize.py
```

### 方案2: SAS执行

```sas
/* 在SAS中运行 */
%include '/path/to/sas/01_macro_data_preparation.sas';
```

### 方案3: 自动化管道

```bash
# 运行完整管道
chmod +x scripts/run_macro_data_pipeline.sh
./scripts/run_macro_data_pipeline.sh
```

## 📊 输出文件

### 主要数据文件

1. **macro_data_consolidated.csv**
   - 整合后的月度宏观数据
   - 时间范围: 2020-01 至 2024-12
   - 包含30+个变量

2. **stress_test_scenarios.csv**
   - 三种压力测试场景
   - Baseline / Adverse / Severely Adverse

3. **macro_data_dictionary.csv**
   - 数据字典
   - 变量定义和统计信息

### 质量报告

- data_quality_report.txt
- summary_statistics.csv
- correlation_matrix.png
- macro_indicators_timeseries.png
- stress_scenarios.png

## 📈 压力测试场景设计

| 场景 | 失业率 | GDP增长 | 房价变化 | 政策利率 | 油价(WTI) |
|------|--------|---------|----------|----------|-----------|
| **基准** | 当前水平 | +2.0% | +5.0% | 3.25% | $70 |
| **不利** | +3.0% | -3.0% | -10.0% | +2.0% | $60 |
| **严重不利** | +5.0% | -5.0% | -20.0% | +3.0% | $40 |

## 🔍 数据质量检查

### 完整性检查
- ✓ 时间序列连续性
- ✓ 缺失值比例 < 5%
- ✓ 所有必需变量存在

### 一致性检查
- ✓ 利率层级: Mortgage > Prime > Policy
- ✓ 利差为正值
- ✓ 日期格式统一

### 合理性检查
- ✓ 失业率: 3% - 15%
- ✓ 通胀率: -2% - 10%
- ✓ 油价: $20 - $150

## 🔗 与ECL模型的集成

整合后的宏观数据将用于：

1. **PD模型**
   - Unemployment_Rate
   - GDP_Growth_YoY
   - Economic_Cycle_Score

2. **LGD模型**
   - HPI_Change_YoY
   - Housing_Risk_Score
   - Mortgage_5Y_Rate

3. **EAD模型**
   - Credit_Conditions
   - Prime_Rate
   - GDP_Growth_YoY

## 📝 注意事项

1. **数据更新频率**: 建议每月10日更新
2. **版本控制**: 使用Git管理数据版本
3. **数据安全**: 生产环境中需加密敏感数据
4. **监管合规**: 确保符合OSFI B-20和IFRS 9要求

## 🛠️ 故障排除

### 常见问题

1. **缺失值过多**
   - 检查数据源完整性
   - 使用前向填充或插值

2. **日期格式不一致**
   - 统一使用YYYY-MM-DD格式
   - 检查时区设置

3. **SAS/Python结果不一致**
   - 检查数值精度设置
   - 验证排序和合并逻辑

## 📞 联系支持

如有问题，请联系：
- 技术支持: risk.analytics@bank.ca
- 数据问题: data.governance@bank.ca

---

*最后更新: 2024-01-15*
*版本: 1.0.0*