from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


from utils import save_data_agg


def read_base_cep(
    spark: SparkSession, file_path: str, delimiter: str = ";"
) -> DataFrame:
    """
    Reads a CSV file with potentially inconsistent delimiters, standardizes to
    a single delimiter, and returns a DataFrame with appropriate columns based
    on the file's first row as headers.

    Parameters:
    - spark (SparkSession): The Spark session object used to read the CSV.
    - file_path (str): The path to the CSV file to be read.
    - delimiter (str): The primary delimiter expected in the file
    (default is ';').

    Returns:
    - DataFrame: A PySpark DataFrame with columns derived from the first row
    of the CSV file, where delimiters have been standardized.

    Note:
    - The function is designed to handle files where '||' may be used
    interchangeably with '|'.
      It replaces '||' with '|' before processing columns.
    """

    df = spark.read.option("header", "false").csv(file_path)

    header = df.first()[0].replace("||", "|").split("|")

    df = df.withColumn("_c0", F.regexp_replace("_c0", "\|\|", "|"))

    split_cols = F.split(df["_c0"], "\|")
    df = df.select([split_cols[i].alias(header[i])
                   for i in range(len(header))])

    return df


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename specific columns in a PySpark DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame.

    Returns:
    - DataFrame: A new DataFrame with renamed columns.

    Renames the following columns:
    - 'CEP' to 'cep'
    - 'Estado' to 'state'
    - 'Região' to 'region'
    """
    return (df.withColumnRenamed("CEP", "cep")
            .withColumnRenamed("Estado", "state")
            .withColumnRenamed("Região", "region"))


def input_on_silver_base_cep():
    spark = SparkSession.builder.appName("my-session").getOrCreate()
    df = read_base_cep(spark, "./input/BaseCEP.csv", ",")
    df = rename_columns(df)
    save_data_agg(df, "./opt/airflow/output/silver/base_cep/")


if __name__ == "__main__":
    input_on_silver_base_cep()
