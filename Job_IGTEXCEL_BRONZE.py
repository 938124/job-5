# Databricks notebook source
# MAGIC %run ../initializeJobVariables
# MAGIC

# COMMAND ----------

import uuid
import re
import os
import pandas as pd
import unicodedata
from pyspark.sql.functions import sha2, concat_ws, col, coalesce, lit
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def clean_column_name(name: str) -> str:
    cleaned = re.sub(r"[\s\-\/]+", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned

def dedupe_names(names):
    seen = {}
    deduped = []
    for name in names:
        if name not in seen:
            seen[name] = 0
            deduped.append(name)
        else:
            seen[name] += 1
            deduped.append(f"{name}_{seen[name]}")
    return deduped

def normalize_columns(df):
    cleaned = [clean_column_name(sanitize_identifier(c)) for c in df.columns]
    cleaned = dedupe_names(cleaned)
    for original, new_name in zip(df.columns, cleaned):
        if original != new_name:
            df = df.withColumnRenamed(original, new_name)
    return df

def derive_table_name(workbook_file_name: str) -> str:
    base = os.path.splitext(os.path.basename(workbook_file_name))[0]
    return clean_column_name(base)

def copy_to_dbfs_tmp(source_path: str) -> str:
    tmp_name = f"{uuid.uuid4().hex}.xlsx"
    dbfs_tmp = f"dbfs:/tmp/{tmp_name}"
    dbutils.fs.cp(source_path, dbfs_tmp, True)
    return dbfs_tmp

def dbfs_to_local(dbfs_path: str) -> str:
    return "/dbfs/" + dbfs_path.replace("dbfs:/", "")

def get_local_workbook(source_path: str):
    dbfs_tmp = copy_to_dbfs_tmp(source_path)
    return dbfs_tmp, dbfs_to_local(dbfs_tmp)

def get_sheet_names(local_path: str):
    xls = pd.ExcelFile(local_path)
    return xls.sheet_names

def sanitize_identifier(name: str) -> str:
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if name and name[0].isdigit():
        name = "_" + name
    return name

def clean_col(c):
   return re.sub(r'[^a-zA-Z0-9_]', '_', c)

def find_sheet_name(local_path: str, pattern: str) -> str:
    xls = pd.ExcelFile(local_path)
    actual_sheets = xls.sheet_names
    if pattern in actual_sheets:
        return pattern
    matches = [s for s in actual_sheets if s.lower().startswith(pattern.lower())]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple sheets start with '{pattern}': {matches}. Use a more specific pattern.")
    matches = [s for s in actual_sheets if pattern.lower() in s.lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple sheets contain '{pattern}': {matches}. Use a more specific pattern.")
    raise RuntimeError(f"No sheet matches '{pattern}'. Available: {actual_sheets}")

def detect_data_sheet(local_path: str, explicit_sheet: str) -> str:
    if explicit_sheet:
        return explicit_sheet

    try:
        sheet_names = get_sheet_names(local_path)
    except Exception as exc:
        raise RuntimeError(
            "Unable to auto-detect sheet names. Set data_sheet_name."
        ) from exc

    best_sheet = None
    best_count = -1
    failures = []

    for sheet in sheet_names:
        try:
            df = pd.read_excel(local_path, sheet_name=sheet, header=0)
            if df.empty:
                continue
            non_null_count = df.notna().any(axis=1).sum()
            if non_null_count > best_count:
                best_count = non_null_count
                best_sheet = sheet
        except Exception as exc:
            failures.append((sheet, str(exc)))
            continue

    if best_sheet is None:
        raise RuntimeError(
            "No suitable data sheet found. Set data_sheet_name explicitly. "
            f"Sheet read failures: {failures}"
        )

    return best_sheet


# COMMAND ----------

from pyspark.sql import functions as F
import re
import json
from pyspark.sql.functions import current_date
from datetime import datetime, timezone
from pyspark.sql import functions as F
import uuid
from pyspark.sql.functions import col

dict_job_data = getRunDetails()
pipeline_name = dict_job_data["jobName"]
job_id = dict_job_data["jobId"]
job_run_id = dict_job_data["jobRunId"]
start_time = datetime.now(timezone.utc).isoformat()

try:
    environment = dbutils.secrets.get(scope="workspace-details", key="name")
except Exception:
    environment = "development"
spark.conf.set("sql.environment", environment)

source_files = set()

try:
    sql = f"SELECT pipeline_id FROM {environment}_011_bronze_core.db_admin.pipeline WHERE pipeline_name = '{pipeline_name}'"
    df = spark.sql(sql)
    new_id_row = df.collect()
    pipeline_id = new_id_row[0]["pipeline_id"] if new_id_row else None

    sql = f"SELECT variable_value FROM {environment}_011_bronze_core.db_admin.pipeline_parameter WHERE pipeline_id = '{pipeline_id}' and variable_name = 'workbook_path'"
    df = spark.sql(sql)
    new_id_row = df.collect()
    master_path = new_id_row[0]["variable_value"] if new_id_row else None

    sql = f"SELECT variable_value FROM {environment}_011_bronze_core.db_admin.pipeline_parameter WHERE pipeline_id = '{pipeline_id}' and variable_name = 'db_catalog'"
    df = spark.sql(sql)
    new_id_row = df.collect()
    db_schema_name = new_id_row[0]["variable_value"] if new_id_row else None

    if db_schema_name and "atlas_legacy" in db_schema_name:
        print(f"Skipping pipeline '{pipeline_name}': db_catalog '{db_schema_name}' is atlas_legacy. Only atlas pipelines are processed.")
        dbutils.notebook.exit("Skipped: atlas_legacy pipeline")

    sql = f"SELECT etl_id FROM {environment}_011_bronze_core.db_admin.pipeline_history WHERE job_run_id = '{job_run_id}'"
    df = spark.sql(sql)
    new_id_row = df.collect()
    new_id = new_id_row[0]["etl_id"] if new_id_row else None

    pdf = master_path
    file_name = pdf.split("/")[-1]

    sql = f"SELECT file_id, file_name FROM {environment}_011_bronze_core.db_admin.pipeline_file WHERE file_name = '{file_name}'"
    df_p_file = spark.sql(sql)
    table_name = db_schema_name + "." + clean_column_name(file_name.replace(".xlsx","").replace(".xls",""))
    file_row = df_p_file.collect()
    file_exists = file_row and file_row[0]["file_name"] is not None

    if not file_exists:
        sql = f"""
        INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_file
        (etl_id, file_name, file_path, file_date, file_import, file_status, created_date)
        VALUES (
        try_cast({new_id} as bigint),
        '{file_name}',
        '{pdf}',
        current_date(),
        TRUE,
        'Active',
        current_timestamp()
        )
        """
        spark.sql(sql)
    else:
        sql = f"""
        UPDATE {environment}_011_bronze_core.db_admin.pipeline_file
        SET file_status = 'Deleted'
        WHERE file_name = '{file_name}'
        """
        spark.sql(sql)
        sql = f"""
        INSERT INTO {environment}_011_bronze_core.db_admin.pipeline_file
        (etl_id, file_name, file_path, file_date, file_import, file_status, created_date)
        VALUES (
        try_cast({new_id} as bigint),
        '{file_name}',
        '{master_path}',
        current_date(),
        TRUE,
        'Active',
        current_timestamp()
        )
        """
        spark.sql(sql)

    sql = f"SELECT file_id, file_name FROM {environment}_011_bronze_core.db_admin.pipeline_file WHERE file_name = '{file_name}'"
    df_p_file = spark.sql(sql)
    file_row = df_p_file.collect()
    file_id = file_row[0]["file_id"]

    print(master_path)

    try:
        dbutils.fs.ls(master_path)
    except Exception:
        raise Exception(f"Source file not found in S3: {master_path}\nEnsure the file is in the manual/ folder before running this job.")

    dbfs_tmp, local_path = get_local_workbook(master_path)
    source_files.add(master_path)

    sql = f"SELECT variable_value FROM {environment}_011_bronze_core.db_admin.pipeline_parameter WHERE pipeline_id = '{pipeline_id}' and variable_name = 'sheet_mapping'"
    df = spark.sql(sql)
    new_id_row = df.collect()
    sheet_mapping = new_id_row[0]["variable_value"] if new_id_row else None

    try:
        if sheet_mapping:
            sheet_mapping = json.loads(sheet_mapping)
            truncated_tables = set()
            created_tables = set()

            for sheet in sheet_mapping:
                data_sheet = sheet.get('sheet_name')
                dest_table = sheet.get('destination_table')
                table_only = sanitize_identifier(dest_table.split(".")[-1])
                table_name = db_schema_name + "." + table_only
                skip_rows = sheet.get('header_rows_to_skip')
                footer_rows = sheet.get('footer_rows_to_skip')

                sheet_workbook = sheet.get('workbook_path') or master_path
                if sheet_workbook != master_path:
                    source_files.add(sheet_workbook)
                    _, sheet_local_path = get_local_workbook(sheet_workbook)
                else:
                    sheet_local_path = local_path

                data_sheet_name = find_sheet_name(sheet_local_path, data_sheet) if data_sheet else detect_data_sheet(sheet_local_path, None)

                excl = pd.read_excel(sheet_local_path, sheet_name=data_sheet_name, skiprows=skip_rows, dtype=str)
                excl = excl.dropna(axis=1, how='all')
                excl = excl.loc[:, excl.columns.astype(str).str.strip() != '']
                if excl.empty:
                    raise RuntimeError("Selected sheet is empty")

                data_df = spark.createDataFrame(excl.astype(str))
                data_df = normalize_columns(data_df)
                data_df = data_df.select([col(c) for c in data_df.columns if c.strip() != ''])
                data_df = (data_df
                    .withColumn("etl_id", F.lit(new_id))
                    .withColumn("file_id", F.lit(file_id))
                    .withColumn("IS_DELETED", F.lit("false"))
                )
				exclude_cols = ["etl_id", "file_id", "IS_DELETED"]

				hash_cols = sorted([c for c in data_df.columns if c not in exclude_cols])


                table_exists = spark.catalog.tableExists(table_name)
                if not table_exists and table_name not in created_tables:
                    (data_df.write.format("delta")
                        .mode("overwrite")
                        .saveAsTable(table_name)
                    )
                    created_tables.add(table_name)
                else:
                    #if table_exists and table_name not in truncated_tables:
                        #spark.sql(f"TRUNCATE TABLE {table_name}")
                        #truncated_tables.add(table_name)
					target = DeltaTable.forName(spark, table_name)
					target.update(
						condition="IS_DELETED = 'false'",
						set={"IS_DELETED": "'true'"}
					)
                    incoming_view = f"_incoming_{uuid.uuid4().hex[:8]}"
                    data_df.createOrReplaceTempView(incoming_view)
                    col_list = ", ".join(data_df.columns)
                    spark.sql(f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM {incoming_view}")
                    spark.catalog.dropTempView(incoming_view)

                now = datetime.now(timezone.utc)
                safe_sheet_name = data_sheet_name.replace("'", "''")
                safe_file_name = sheet_workbook.split("/")[-1].replace("'", "''")
                metadata_sql = f"""
                INSERT INTO {environment}_021_bronze_finance.atlas.file_store_metadata
                  (pipeline_name, file_name, sheet_name, destination_table, load_year, load_month)
                VALUES
                  ('{pipeline_name}', '{safe_file_name}', '{safe_sheet_name}', '{table_name}', {now.year}, {now.month})
                """
                try:
                    spark.sql(metadata_sql)
                    print(f"  Logged metadata: {safe_file_name} / {safe_sheet_name} -> {table_name}")
                except Exception as meta_err:
                    print(f"  WARNING: file_store_metadata insert failed (non-fatal): {meta_err}")

            sql = f"SELECT variable_value FROM {environment}_011_bronze_core.db_admin.pipeline_parameter WHERE pipeline_id = '{pipeline_id}' and variable_name = 'archive_folder'"
            df = spark.sql(sql)
            new_id_row = df.collect()
            archive_folder = new_id_row[0]["variable_value"] if new_id_row else None
            archive_folder = archive_folder.rstrip("/") + "/"

            for src_path in source_files:
                src_file_name = src_path.split("/")[-1]
                print(f"Loaded {src_file_name} -> {table_name}")
                dest_path = f"{archive_folder.rstrip('/')}/{src_file_name}"
                dbutils.fs.mv(src_path, dest_path, True)

            spark.sql(f"""
                UPDATE {environment}_011_bronze_core.db_admin.pipeline_file
                SET file_status = 'Success'
                WHERE file_id = {file_id}
            """)
        else:
            print("missing_sheet_name_details")
            raise Exception("missing_sheet_name_details")
    except Exception as exc:
        sql = f"SELECT variable_value FROM {environment}_011_bronze_core.db_admin.pipeline_parameter WHERE pipeline_id = '{pipeline_id}' and variable_name = 'failure_folder'"
        df = spark.sql(sql)
        new_id_row = df.collect()
        failure_folder = new_id_row[0]["variable_value"] if new_id_row else None
        failure_folder = failure_folder.rstrip("/") + "/"
        for src_path in source_files if source_files else {master_path}:
            src_file_name = src_path.split("/")[-1]
            print(f"Failed {src_file_name} -> {table_name}: {exc}")
            dest_path = f"{failure_folder.rstrip('/')}/{src_file_name}"
            try:
                dbutils.fs.mv(src_path, dest_path, True)
            except Exception:
                pass
        if file_id:
            spark.sql(f"""
                UPDATE {environment}_011_bronze_core.db_admin.pipeline_file
                SET file_status = 'Failed'
                WHERE file_id = {file_id}
            """)
        raise Exception(exc)
except Exception as e:
    print(f"Pipeline execution failed: {e}")
    raise Exception(e)
