
from pyspark.sql import SparkSession,  DataFrame
from pyspark.sql import functions as F

from utils import save_data_agg, read_csv_to_df


def select_valid_level_description(df: DataFrame) -> DataFrame:
    """
    Filters the input DataFrame to include only rows where the
    "Descrição Nível" column values are within a predefined list
    of valid level descriptions.

    Parameters:
    - df (DataFrame): A PySpark DataFrame containing the column
    "Descrição Nível".

    Returns:
    - DataFrame: A filtered DataFrame containing only rows with valid
    "Descrição Nível" entries.

    """
    level_list = ["Estagiário", "Analista",
                  "Coordenador", "Gerente", "Diretor"]
    df = df.filter(F.col("Descrição Nível").isin(level_list))
    return df


def rename_and_select_valid_columns(df: DataFrame) -> DataFrame:
    """
    Renames selected columns in the input DataFrame to more standardized
    column names.

    Parameters:
    - df (DataFrame): A PySpark DataFrame with columns that need renaming.

    Returns:
    - DataFrame: The DataFrame with renamed columns.

    Example of column renaming:
    - "Nível" is renamed to "level"
    - "Descrição Nível" is renamed to "level_description"
    - "Tempo no Nível" is renamed to "time_on_level"
    """
    df = df.withColumnRenamed("Nível", "level") \
        .withColumnRenamed("Descrição Nível", "level_description") \
        .withColumnRenamed("Tempo no Nível", "time_on_level")

    df = df.select(["level", "level_description", "time_on_level"])

    return df


def input_on_silver_base_nivel():
    spark = SparkSession.builder \
        .appName("my-session") \
        .getOrCreate()

    df = read_csv_to_df(spark, "./input/BaseNível.csv", "%")
    df = select_valid_level_description(df)
    df = rename_and_select_valid_columns(df)

    save_data_agg(df, "./opt/airflow/output/silver/base_nivel")


if __name__ == "__main__":
    input_on_silver_base_nivel()
