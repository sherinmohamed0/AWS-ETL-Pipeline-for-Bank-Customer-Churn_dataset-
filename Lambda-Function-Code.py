import boto3
import pandas as pd
import io

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

s3 = boto3.client("s3")
glue = boto3.client('glue')

def lambda_handler(event, context):

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    print("Lambda triggered for bucket:", bucket)
    print("File key:", key)

    if not key.lower().endswith(".csv"):
        print("Skipping non-CSV file.")
        return {"status": "skipped", "message": "Not a CSV file"}

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print("CSV read successfully. Rows:", len(df))
    except Exception as e:
        print("Error reading CSV:", e)
        return {"status": "error", "message": str(e)}

    # --- CLEANING ---
    df = df.drop_duplicates()
    df = df.dropna(how="all")

    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(0)

    obj_cols = df.select_dtypes(include=['object']).columns
    df[obj_cols] = df[obj_cols].fillna("unknown")

    print("Cleaning done. Rows after cleaning:", len(df))

    # --- Output path ---
    if key.startswith("customer-dataCSV-incoming/"):
        target_key = key.replace(
            "customer-dataCSV-incoming/",
            "Customer-dataLake-parquet/"
        ).replace(".csv", ".parquet" if PARQUET_AVAILABLE else ".csv")
    else:
        target_key = "Customer-dataLake-parquet/" + key.split("/")[-1].replace(
            ".csv",
            ".parquet" if PARQUET_AVAILABLE else ".csv"
        )

    print("Target key for processed file:", target_key)

    # --- Write output ---
    try:
        if PARQUET_AVAILABLE:
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            buffer.seek(0)
            s3.put_object(Bucket=bucket, Key=target_key, Body=buffer.getvalue())
            print("Parquet file uploaded successfully.")
        else:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=target_key, Body=csv_buffer.getvalue())
            print("CSV file uploaded successfully.")
    except Exception as e:
        print("Error uploading file:", e)
        return {"status": "error", "message": str(e)}

    # --- START GLUE CRAWLER ---
    try:
        crawler_name = 'bank-cutomers-crwlr'
        glue.start_crawler(Name=crawler_name)
        print("Glue crawler started.")
    except Exception as e:
        print("Error starting crawler:", e)

    return {"status": "success", "output": target_key}
