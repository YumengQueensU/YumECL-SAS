# Canadian Credit Risk Model (IFRS 9 Compliant)

## Overview
Enterprise-grade credit risk modeling framework for Canadian retail portfolios, 
compliant with OSFI guidelines and IFRS 9 requirements.

## Features
- ✅ IFRS 9 ECL calculation with 3-stage allocation
- ✅ PD/LGD/EAD models using SAS
- ✅ Stress testing framework (OSFI scenarios)
- ✅ Champion-Challenger model comparison
- ✅ Automated audit trail and data lineage
- ✅ Canadian-specific risk factors integration

## Quick Start
1. Set up database connections in `/config/database_config.json`
2. Run SQL DDL scripts: `sql/01_ddl/*.sql`
3. Load sample data: `sas/01_data_prep/02_load_data.sas`
4. Execute main pipeline: `sas/main_pipeline.sas`

## Technology Stack
- **Database**: SQL Server 2019+ / Oracle 19c
- **Analytics**: SAS 9.4+ / SAS Viya
- **Reporting**: SAS ODS, Excel
- **Version Control**: Git

## Regulatory Compliance
- OSFI Guideline E-23 (Model Risk Management)
- IFRS 9 Financial Instruments
- Basel III Capital Requirements (CAR)