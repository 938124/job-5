-- BACKFILL SCRIPTS: Federated → Bronze

-- 1
INSERT INTO development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Africa
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_intl_performance_emea_africa;

-- 2
INSERT INTO development_021_bronze_finance.atlas.RPM_Exp_RoyaltiesAlliant
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_exp_royaltiesalliant;

-- 3
INSERT INTO development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II_ManualBilling
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_performance_cds_class_ii_manualbilling;

-- 4
INSERT INTO development_021_bronze_finance.atlas.RPM_Performance_CDS_Class_II
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_performance_cds_class_ii;

-- 5
INSERT INTO development_021_bronze_finance.atlas.BW_COPA_DailyFeePoker_Units
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.bw_copa_dailyfeepoker_units;

-- 6
INSERT INTO development_021_bronze_finance.atlas.RPM_Exp_Depreciation
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_exp_depreciation;

-- 7
INSERT INTO development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_FixedFee
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_intl_performance_emea_fixedfee;

-- 8
INSERT INTO development_021_bronze_finance.atlas.RPM_Performance_FF_MJP_PriorMonth
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_performance_ff_mjp_priormonth;

-- 9
INSERT INTO development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_GreeceWLA
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_intl_performance_emea_greecewla;

-- 10
INSERT INTO development_021_bronze_finance.atlas.RPM_Intl_Performance_EMEA_Iceland
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_intl_performance_emea_iceland;

-- 11
INSERT INTO development_021_bronze_finance.atlas.RPM_Intl_Performance_LAC
SELECT *, -1 AS etl_id, false AS is_deleted
FROM federated_101_atlas.dbo.rpm_intl_performance_lac;
