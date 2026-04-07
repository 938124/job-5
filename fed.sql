

-- Federated Backfill Scripts

-- 1
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_intl_performance_emea_africa AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Africa WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_intl_performance_emea_africa
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Africa
WHERE is_deleted = false;

-- 2
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_exp_royaltiesalliant AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_exp_royaltiesalliant
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant
WHERE is_deleted = false;

-- 3
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_performance_cds_class_ii_manualbilling AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II_ManualBilling WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_performance_cds_class_ii_manualbilling
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II_ManualBilling
WHERE is_deleted = false;

-- 4
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_performance_cds_class_ii AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_performance_cds_class_ii
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II
WHERE is_deleted = false;

-- 5
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.bw_copa_dailyfeepoker_units AS
SELECT * FROM development_021_bronze_finance.atlas.BW_COPA_DailyFeePoker_Units WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.bw_copa_dailyfeepoker_units
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.BW_COPA_DailyFeePoker_Units
WHERE is_deleted = false;

-- 6
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_exp_depreciation AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Exp_Depreciation WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_exp_depreciation
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Exp_Depreciation
WHERE is_deleted = false;

-- 7
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_intl_performance_emea_fixedfee AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_FixedFee WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_intl_performance_emea_fixedfee
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_FixedFee
WHERE is_deleted = false;

-- 8
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_performance_ff_mjp_priormonth AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Performance_FF_MJP_PriorMonth WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_performance_ff_mjp_priormonth
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Performance_FF_MJP_PriorMonth
WHERE is_deleted = false;

-- 9
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_intl_performance_emea_greecewla AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_GreeceWLA WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_intl_performance_emea_greecewla
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_GreeceWLA
WHERE is_deleted = false;

-- 10
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_intl_performance_emea_iceland AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Iceland WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_intl_performance_emea_iceland
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Iceland
WHERE is_deleted = false;

-- 11
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.gameopsinstallbaseactivity AS
SELECT * FROM development_021_bronze_finance.atlas.GameOpsInstallBaseActivity WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.gameopsinstallbaseactivity
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.GameOpsInstallBaseActivity
WHERE is_deleted = false;

-- 12
CREATE TABLE IF NOT EXISTS federated_101_atlas.dbo.rpm_intl_performance_lac AS
SELECT * FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_LAC WHERE 1=2;

INSERT OVERWRITE federated_101_atlas.dbo.rpm_intl_performance_lac
SELECT *, -1 AS etl_id
FROM development_021_bronze_finance.atlas.RPM_Intl_Performance_LAC
WHERE is_deleted = false;
