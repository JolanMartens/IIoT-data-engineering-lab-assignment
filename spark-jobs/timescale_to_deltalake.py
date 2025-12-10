from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER_NAME = "datalake"

JDBC_URL = "jdbc:postgresql://timescaledb:5432/iiot"
JDBC_PROPERTIES = { "user": "admin", "password": "admin", "driver": "org.postgresql.Driver" }

def get_spark_session():
    return (SparkSession.builder
        .appName("TimescaleDB_to_DeltaLake_ETL")
        # Dependencies
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4,org.postgresql:postgresql:42.6.0")
        # Delta Lake Configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Azure/Azurite Connectivity Configs
        .config(f"fs.azure.account.key.{AZURITE_ACCOUNT_NAME}.blob.core.windows.net", AZURITE_ACCOUNT_KEY)
        .config("fs.azure.secure.mode", "false")           # Disable Security
        .config("fs.azure.always.use.https", "false")      # FORCE HTTP
        .config("fs.azure.io.retry.max.retries", "1")      # Fail fast if connection bad
        # Randomness Fix
        .config("spark.driver.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .config("spark.executor.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .getOrCreate())

def extract_data(spark, table_name):
    print(f"Extracting data from {table_name}...")
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)

def load_data(df, folder_name):
    path = f"wasb://{CONTAINER_NAME}@{AZURITE_ACCOUNT_NAME}.blob.core.windows.net/{folder_name}"
    print(f"Writing to {path}...")
    df.withColumn("year", year(col("timestamp"))).withColumn("month", month(col("timestamp"))).withColumn("day", dayofmonth(col("timestamp"))) \
      .write.format("delta").mode("overwrite").partitionBy("year", "month", "day").save(path)

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        load_data(extract_data(spark, "machine_sensors"), "machine_sensors_delta")
        # Rename window_end to timestamp so partitioning works
        load_data(extract_data(spark, "sensor_aggregates").withColumnRenamed("window_end", "timestamp"), "sensor_aggregates_delta")
        print("ETL Job Completed Successfully.")
    except Exception as e:
        print(f"Job Failed: {e}")
        spark.stop() 
        raise e
    
    spark.stop()

if __name__ == "__main__":
    main()