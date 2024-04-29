from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils import save_data_agg, read_csv_to_df


def treat_deslocated_columns(df: DataFrame) -> DataFrame:
    """
    Normalizes and cleans multiple columns in a DataFrame based on specific
    conditions to ensure data consistency and integrity.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame.

    Returns:
    - DataFrame: The DataFrame after applying normalization and cleaning
    operations.

    Operations performed:
    - Fills null values in specific columns based on values from other related
    columns.
    - Removes duplicates by setting columns to None where values are identical
    to those in related columns.
    - Corrects specific typos and formatting issues.
    """

    df = df.withColumn(
        "Nível",
        F.when(
            F.col("Nível").isNull(),
            F.col("Área")
        ).otherwise(F.col("Nível"))
    )

    level_list = ["Estagiário", "Analista",
                  "Coordenador", "Gerente", "Diretor"]

    df = df.withColumn(
        "Área",
        F.when(F.col("Área").isin(level_list),
               F.lit(None)).otherwise(F.col("Área"))
    )

    df = df.withColumn(
        "Área",
        F.regexp_replace(F.col("Área"), "^@+([^@]+)$", "$1")
    )

    df = df.withColumn(
        "Área",
        F.regexp_replace("Área", "Logísitca", "Logística")
    )

    df = df.withColumn(
        "Área",
        F.when(
            F.col("Área").isNull(),
            F.col("COD Área")
        ).otherwise(F.col("Área"))
    )

    df = df.withColumn(
        "COD Área",
        F.when(
            F.col("Área") == F.col("COD Área"),
            F.lit(None)
        ).otherwise(F.col("COD Área"))
    )

    df = df.withColumn(
        "COD Área",
        F.when(
            F.col("COD Área").isNull(),
            F.col("COD Nível")
        ).otherwise(F.col("COD Área"))
    )

    df = df.withColumn(
        "COD Nível",
        F.when(
            F.col("COD Nível") == F.col("COD Área"),
            F.lit(None)
        ).otherwise(F.col("COD Nível"))
    )

    df = df.withColumn(
        "COD Nível",
        F.when(
            F.col("COD Nível").isNull(),
            F.col("Quadro")
        ).otherwise(F.col("COD Nível"))
    )

    df = df.withColumn(
        "Quadro",
        F.when(
            F.col("COD Nível") == F.col("Quadro"),
            F.lit(None)
        ).otherwise(F.col("Quadro"))
    )

    df = df.withColumn(
        "Quadro",
        F.when(
            F.col("Quadro").isNull(),
            F.col("Bonus")
        ).otherwise(F.col("Quadro"))
    )

    df = df.withColumn(
        "Bonus",
        F.when(
            F.col("Bonus") == F.col("Quadro"),
            F.lit(None)
        ).otherwise(F.col("Bonus"))
    )

    df = df.withColumn(
        "Bonus",
        F.when(
            F.col("Bonus").isNull(),
            F.col("Contratacao")
        ).otherwise(F.col("Bonus"))
    )

    df = df.withColumn(
        "Contratacao",
        F.when(
            F.col("Bonus") == F.col("Contratacao"),
            F.lit(None)
        ).otherwise(F.col("Contratacao"))
    )

    return df


def rename_columns(df):
    """
    Renames specific columns of a DataFrame to English names following a
    predefined naming convention.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame with columns in Portuguese.

    Returns:
    - DataFrame: The DataFrame with columns renamed to English.

    Usage:
    >>> spark = SparkSession.builder.appName("Rename Columns").getOrCreate()
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> renamed_df = rename_columns(df)
    """

    column_mapping = {
        "Cargo": "level",
        "Nível": "level_description",
        "Área": "area",
        "COD Área": "cod_area",
        "COD Nível": "cod_level",
        "Quadro": "quadro",
        "Bonus": "bonus",
        "Contratacao": "hiring",
    }

    for portuguese_name, english_name in column_mapping.items():
        df = df.withColumnRenamed(portuguese_name, english_name)

    return df


def input_on_silver_base_cargos():
    spark = SparkSession.builder.appName("my-session").getOrCreate()

    df = read_csv_to_df(spark, "./input/BaseCargos.csv", ";")
    df = treat_deslocated_columns(df)
    df = rename_columns(df)

    save_data_agg(df, "./opt/airflow/output/silver/base_cargos/")


if __name__ == "__main__":
    input_on_silver_base_cargos()
