"""
AWS ETL Pipeline - Single-file validation script
Validates:
  1. Source JSON files in S3
  2. S3 -> Lambda event notification configuration
  3. Lambda function configuration and recent invocation errors (CloudWatch)
  4. Staging Parquet files and their schema
  5. Glue crawler and Data Catalog table
  6. Athena table accessibility and row count

Prerequisites:
  pip install boto3 pandas pyarrow

Run:
  set AWS_DEFAULT_REGION=ap-south-1
  set ATHENA_OUTPUT=s3://rw21-etl-project/athena-results/
  python validate_etl_pipeline.py

The AWS identity used to run this script needs read permissions for S3, Lambda,
Glue, CloudWatch Logs and Athena, plus Athena query-result write permissions.
"""

import json
import os
import sys
import time
import tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote_plus

import boto3
import pandas as pd


# --------------------------- CONFIGURATION ---------------------------

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

BUCKET = os.getenv("ETL_BUCKET", "rw21-etl-project")
SOURCE_PREFIX = os.getenv("SOURCE_PREFIX", "orders_json_incoming/")
STAGING_PREFIX = os.getenv("STAGING_PREFIX", "orders_parquet_datalake/")

LAMBDA_FUNCTION = os.getenv("LAMBDA_FUNCTION", "my_etl_pipeline")
GLUE_CRAWLER = os.getenv("GLUE_CRAWLER", "etl_pipeline_crawler")
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "etl")
GLUE_TABLE = os.getenv("GLUE_TABLE", "orders_parquet_datalake")

# Athena requires an S3 output location. Change this if you use another location.
ATHENA_OUTPUT = os.getenv(
    "ATHENA_OUTPUT",
    f"s3://{BUCKET}/athena-results/"
)

EXPECTED_COLUMNS = [
    "order_id",
    "order_date",
    "total_amount",
    "customer_id",
    "customer_name",
    "email",
    "address",
    "product_id",
    "product_name",
    "category",
    "price",
    "quantity",
]

# Set to True if the S3 event should invoke this Lambda for .json files
CHECK_JSON_SUFFIX = True

# Set to True to fail when no source JSON or staging Parquet files are found
REQUIRE_SOURCE_FILES = True
REQUIRE_PARQUET_FILES = True

# --------------------------------------------------------------------

s3 = boto3.client("s3", region_name=AWS_REGION)
lam = boto3.client("lambda", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)
logs = boto3.client("logs", region_name=AWS_REGION)
athena = boto3.client("athena", region_name=AWS_REGION)

passed = 0
failed = 0
warnings = 0


def report(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        status = "PASS"
    else:
        failed += 1
        status = "FAIL"
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))


def warn(name, detail):
    global warnings
    warnings += 1
    print(f"[WARN] {name} - {detail}")


def list_objects(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    items = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        items.extend(page.get("Contents", []))
    return items


def flatten_expected_count(data):
    """Return the expected flattened row count for the uploaded JSON structure."""
    if not isinstance(data, list):
        raise ValueError("Top-level JSON must be an array of orders")

    count = 0
    for index, order in enumerate(data):
        if not isinstance(order, dict):
            raise ValueError(f"Order at index {index} is not a JSON object")
        products = order.get("products")
        if not isinstance(products, list):
            raise ValueError(f"Order at index {index} has no valid products array")
        count += len(products)
    return count


def check_source_json():
    print("\n--- 1. SOURCE S3 / JSON VALIDATION ---")
    objects = [
        obj for obj in list_objects(SOURCE_PREFIX)
        if not obj["Key"].endswith("/")
    ]

    if not objects:
        report(
            "Source JSON files exist",
            not REQUIRE_SOURCE_FILES,
            f"No objects found under s3://{BUCKET}/{SOURCE_PREFIX}",
        )
        return

    report("Source JSON files exist", True, f"{len(objects)} file(s) found")

    valid_files = 0
    total_expected_rows = 0

    for obj in objects:
        key = obj["Key"]
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            data = json.loads(body.decode("utf-8"))
            expected_rows = flatten_expected_count(data)
            valid_files += 1
            total_expected_rows += expected_rows
            print(f"       JSON OK: {key} | expected flattened rows: {expected_rows}")
        except Exception as exc:
            report(f"Valid JSON: {key}", False, str(exc))
            continue

    report(
        "All source JSON files are valid",
        valid_files == len(objects),
        f"{valid_files}/{len(objects)} valid; {total_expected_rows} expected flattened rows across source files",
    )


def check_lambda_trigger():
    print("\n--- 2. S3 EVENT -> LAMBDA TRIGGER ---")
    try:
        cfg = s3.get_bucket_notification_configuration(Bucket=BUCKET)
        configs = cfg.get("LambdaFunctionConfigurations", [])

        matching = []
        for item in configs:
            events = item.get("Events", [])
            rule = item.get("Filter", {}).get("Key", {}).get("FilterRules", [])
            rule_map = {r.get("Name"): unquote_plus(r.get("Value", "")) for r in rule}

            prefix_ok = rule_map.get("Prefix") == SOURCE_PREFIX
            suffix_ok = (not CHECK_JSON_SUFFIX) or rule_map.get("Suffix") == ".json"
            event_ok = any(e == "s3:ObjectCreated:*" or e.startswith("s3:ObjectCreated:") for e in events)

            if prefix_ok and suffix_ok and event_ok:
                matching.append(item)

        report(
            "S3 notification configured for source prefix",
            bool(matching),
            f"Expected prefix={SOURCE_PREFIX}, suffix={'.json' if CHECK_JSON_SUFFIX else 'not enforced'}",
        )

        function_cfg = lam.get_function_configuration(FunctionName=LAMBDA_FUNCTION)
        function_arn = function_cfg["FunctionArn"]
        linked = any(item.get("LambdaFunctionArn") == function_arn for item in matching)
        report("S3 notification points to expected Lambda", linked, LAMBDA_FUNCTION)

        report(
            "Lambda function is active",
            function_cfg.get("State") in (None, "Active"),
            f"State={function_cfg.get('State', 'Active')}; Runtime={function_cfg.get('Runtime')}",
        )
    except Exception as exc:
        report("S3/Lambda trigger configuration", False, str(exc))


def check_lambda_logs():
    print("\n--- 3. LAMBDA EXECUTION / CLOUDWATCH ---")
    log_group = f"/aws/lambda/{LAMBDA_FUNCTION}"
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=24)
        response = logs.filter_log_events(
            logGroupName=log_group,
            startTime=int(start.timestamp() * 1000),
            endTime=int(end.timestamp() * 1000),
            filterPattern='"Task timed out"'
        )
        timeout_events = response.get("events", [])
        report(
            "No Lambda timeout messages in last 24 hours",
            len(timeout_events) == 0,
            f"{len(timeout_events)} timeout message(s)",
        )

        response = logs.filter_log_events(
            logGroupName=log_group,
            startTime=int(start.timestamp() * 1000),
            endTime=int(end.timestamp() * 1000),
            filterPattern='"Traceback"'
        )
        errors = response.get("events", [])
        report(
            "No Python traceback messages in last 24 hours",
            len(errors) == 0,
            f"{len(errors)} traceback message(s)",
        )
    except logs.exceptions.ResourceNotFoundException:
        warn("Lambda CloudWatch log group", f"{log_group} not found; function may not have run yet")
    except Exception as exc:
        warn("Lambda CloudWatch log inspection", str(exc))


def check_parquet():
    print("\n--- 4. STAGING PARQUET VALIDATION ---")
    objects = [
        obj for obj in list_objects(STAGING_PREFIX)
        if obj["Key"].lower().endswith(".parquet")
    ]

    if not objects:
        report(
            "Staging Parquet files exist",
            not REQUIRE_PARQUET_FILES,
            f"No Parquet objects under s3://{BUCKET}/{STAGING_PREFIX}",
        )
        return 0

    report("Staging Parquet files exist", True, f"{len(objects)} file(s) found")

    valid_count = 0
    total_rows = 0

    for obj in objects:
        key = obj["Key"]
        try:
            response = s3.get_object(Bucket=BUCKET, Key=key)
            payload = response["Body"].read()
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                tmp.write(payload)
                tmp.flush()
                df = pd.read_parquet(tmp.name, engine="pyarrow")

            missing = sorted(set(EXPECTED_COLUMNS) - set(df.columns))
            report(
                f"Parquet schema: {key}",
                not missing,
                "All expected columns present" if not missing else f"Missing columns: {missing}",
            )
            report(
                f"Parquet has records: {key}",
                len(df) > 0,
                f"{len(df)} row(s)",
            )
            if not missing and len(df) > 0:
                valid_count += 1
            total_rows += len(df)

        except Exception as exc:
            report(f"Readable Parquet: {key}", False, str(exc))

    report(
        "All staging Parquet files pass schema and non-empty checks",
        valid_count == len(objects),
        f"{valid_count}/{len(objects)} valid; {total_rows} total rows across all staging files",
    )
    return total_rows


def check_glue():
    print("\n--- 5. AWS GLUE CRAWLER / CATALOG ---")
    try:
        crawler = glue.get_crawler(Name=GLUE_CRAWLER)["Crawler"]
        state = crawler.get("State")
        report("Glue crawler is READY", state == "READY", f"State={state}")

        last_crawl = crawler.get("LastCrawl", {})
        crawl_status = last_crawl.get("Status")
        if crawl_status:
            report(
                "Most recent Glue crawler run succeeded",
                crawl_status == "SUCCEEDED",
                f"Last crawl status={crawl_status}",
            )
        else:
            warn("Glue crawler last run", "No LastCrawl status is available yet")

    except Exception as exc:
        report("Glue crawler configuration", False, str(exc))

    try:
        db = glue.get_database(Name=GLUE_DATABASE)["Database"]
        report("Glue database exists", db.get("Name") == GLUE_DATABASE, GLUE_DATABASE)
    except Exception as exc:
        report("Glue database exists", False, str(exc))

    try:
        table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)["Table"]
        columns = table.get("StorageDescriptor", {}).get("Columns", [])
        actual_names = {c.get("Name") for c in columns}
        missing = sorted(set(EXPECTED_COLUMNS) - actual_names)
        report(
            "Glue Catalog table has expected columns",
            not missing,
            "All expected columns present" if not missing else f"Missing: {missing}",
        )
        report("Glue Catalog table has location", bool(table.get("StorageDescriptor", {}).get("Location")),
               table.get("StorageDescriptor", {}).get("Location", "No location"))
    except Exception as exc:
        report("Glue Catalog table exists", False, str(exc))


def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown Athena error")
        raise RuntimeError(f"Athena query {state}: {reason}")

    return athena.get_query_results(QueryExecutionId=query_id)


def check_athena():
    print("\n--- 6. AMAZON ATHENA VALIDATION ---")
    try:
        results = run_athena_query(
            f'SELECT COUNT(*) FROM "{GLUE_TABLE}"'
        )
        rows = results.get("ResultSet", {}).get("Rows", [])
        count_text = rows[1]["Data"][0].get("VarCharValue", "0") if len(rows) > 1 else "0"
        row_count = int(float(count_text))
        report("Athena table query succeeds", True, f"COUNT(*)={row_count}")
        report("Athena table contains records", row_count > 0, f"{row_count} row(s)")
    except Exception as exc:
        report("Athena table query", False, str(exc))


def main():
    print("=" * 72)
    print("AWS ETL PIPELINE - AUTOMATED VALIDATION")
    print("=" * 72)
    print(f"Region:          {AWS_REGION}")
    print(f"Bucket:          {BUCKET}")
    print(f"Source prefix:   {SOURCE_PREFIX}")
    print(f"Staging prefix:  {STAGING_PREFIX}")
    print(f"Lambda:          {LAMBDA_FUNCTION}")
    print(f"Glue crawler:    {GLUE_CRAWLER}")
    print(f"Glue database:   {GLUE_DATABASE}")
    print(f"Glue table:      {GLUE_TABLE}")
    print(f"Athena output:   {ATHENA_OUTPUT}")

    # Confirm the configured AWS identity before running service checks.
    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        identity = sts.get_caller_identity()
        print(f"AWS Account:     {identity.get('Account')}")
        print(f"AWS Principal:   {identity.get('Arn')}")
    except Exception as exc:
        print(f"\nUnable to authenticate to AWS: {exc}")
        sys.exit(2)

    check_source_json()
    check_lambda_trigger()
    check_lambda_logs()
    check_parquet()
    check_glue()
    check_athena()

    print("\n" + "=" * 72)
    print(f"SUMMARY: {passed} PASSED | {failed} FAILED | {warnings} WARNING(S)")
    print("=" * 72)

    if failed:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
