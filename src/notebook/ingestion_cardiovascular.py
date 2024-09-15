import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def setup_session():
    builder = SparkSession.builder.appName("Ingestão Cardio") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

def read_csv(spark, path='D:\curso_impacta\dataops\dataopsPipeline\data_sources\cardiovascular.csv'):
    logging.info("realizando leitura do arquivo")
    return spark.read.format("csv").option("header", "true").load(path)

def rename_columns(df):
    logging.info("renomeando coluna")
    return df.withColumnRenamed("Height_(cm)", "Height_cm").withColumnRenamed("Weight_(kg)", "Weight_kg")


def save_delta(df):
    logging.inf("Armazenando dados")
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("General_Health").save("D:\curso_impacta\dataops\dataopsPipeline\storage\hospital\rw\cardiovascular")


def main():
    spark = setup_session()
    df = read_csv(spark)
    df = rename_columns(df)
    save_delta(df)
    spark.stop()

if __name__ == '__main__':
    main()
    