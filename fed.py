
from pyspark.sql import functions as F

source_catalog = "development_021_bronze_finance"
source_schema = "atlas"

target_catalog = "federated_101_atlas"
target_schema = "dbo"

tables = [
    "RPM_Intl_Performance_EMEA_Africa",
    "RPM_Exp_RoyaltiesAlliant",
    "RPM_Performance_CDS_Class_II_ManualBilling",
    "RPM_Performance_CDS_Class_II",
    "BW_COPA_DailyFeePoker_Units",
    "RPM_Exp_Depreciation",
    "RPM_Intl_Performance_EMEA_FixedFee",
    "RPM_Performance_FF_MJP_PriorMonth",
    "RPM_Intl_Performance_EMEA_GreeceWLA",
    "RPM_Intl_Performance_EMEA_Iceland",
    "GameOpsInstallBaseActivity",
    "RPM_Intl_Performance_LAC",
    "RPM_Performance_NonPremLotto_RI",
    "BPC_Plug_Financials_MachineCosts",
    "BPC_Plug_Financials",
    "RPM_Performance_PartAccrualMonth",
    "RPM_Performance_PartPriorMonth",
    "RPM_Performance_WAP",
    "RPM_Performance_WAP_NewCanada",
    "BW_ZRBINQ_BillingPlans"
]

for table in tables:
    
    source_table = f"{source_catalog}.{source_schema}.{table}"
    
    # ✅ lowercase target
    target_table_name = table.lower()
    target_table = f"{target_catalog}.{target_schema}.{target_table_name}"
    
    print(f"Processing: {source_table} → {target_table}")
    
    df = spark.table(source_table).filter("is_deleted = false")
    
    # Replace etl_id
    if "etl_id" in df.columns:
        df = df.drop("etl_id")
    
    df = df.withColumn("etl_id", F.lit(-1))
    
    df.write.mode("overwrite").saveAsTable(target_table)
