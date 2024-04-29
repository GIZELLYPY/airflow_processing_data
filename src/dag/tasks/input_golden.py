import sys
sys.path.insert(0, "/opt/airflow/dags/dag/tasks")

from pyspark.sql import SparkSession,  DataFrame
from pyspark.sql import functions as F
import pandas as pd
from typing import Tuple
from utils import save_data_agg


def join_and_group_by_region(base_funcionarios: DataFrame,
                             base_cep: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Joins the 'base_funcionarios' DataFrame with the 'base_cep' DataFrame on
    the 'cep' column and groups the resulting DataFrame by 'region', counting
    the distinct encrypted CPFs (cpf_encrypted) for each region. The resulting
    DataFrame is ordered by the total number of employees in each region.

    Parameters:
        base_funcionarios (DataFrame): DataFrame containing employee data.
        base_cep (DataFrame): DataFrame containing address data.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
            - The first DataFrame is the joined DataFrame with address
            information.
            - The second DataFrame is grouped by region with the total number
            of distinct employees in each region.
    """

    df = base_funcionarios.join(
        base_cep.select("cep", "state", "region"),
        on="cep",
        how="left"
    )
    df = df.drop_duplicates()

    region = df.groupBy("region").agg(
        F.countDistinct(F.col("cpf_encrypted")).alias("total_employees")
    ).orderBy("total_employees", ascending=False)

    return df, region


def calculate_average_time_on_job(df: DataFrame,
                                  base_cargos: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Calculates the average time on the job in years for each level description.

    Joins the 'df' DataFrame with the 'base_cargos' DataFrame on the 'level' 
    and 'position' columns and calculates the average time on the job in years 
    for each level description.

    Parameters:
        df (DataFrame): DataFrame containing joined data.
        base_cargos (DataFrame): DataFrame containing cargo data.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
            - The first DataFrame is the joined DataFrame with cargo
            information.
            - The second DataFrame contains the average time on the job in
            years for each level description.
    """
    df = df.join(
        base_cargos.select("level",
                           "level_description",
                           "area",
                           "cod_area",
                           "cod_level",
                           "quadro",
                           "bonus"),
        base_cargos.level == df.position,
        how="left"
    )
    df = df.drop_duplicates()

    average_time_on_job = df.groupBy("level_description").agg(
        F.avg(df.time_on_job / 365.25).alias("average_time_on_job_in_years")
    ).orderBy("average_time_on_job_in_years", ascending=False)

    return df, average_time_on_job



def classify_generation_in_dataframe(df: DataFrame) -> DataFrame:
    """
    Classifies ages in a DataFrame into generations based on predefined age
    ranges.

    This function takes a DataFrame 'df' containing a column 'person_age', and
    classifies each age into one of the predefined generations: Gen Z,
    Millennial, Gen X, Boomer, or Silent.

    Parameters:
        df (DataFrame): A PySpark DataFrame containing the age information in a
        column named 'person_age'.

    Returns:
        DataFrame: A new DataFrame with an additional column 'generation'
        containing the generation
                   classification for each age.
    """
    generation_ranges = {
                        "Gen Z": (0, 24),
                        "Millennial": (25, 40),
                        "Gen X": (41, 56),
                        "Boomer": (57, 75),
                        "Silent": (76, 100)
                        }


    def classify_generation(age):
        for generation, (start, end) in generation_ranges.items():
            if start <= age <= end:
                return generation
        return "Unknown"


    df = df.withColumn("generation", F.udf(classify_generation)(df["person_age"]))

    return df


def calculate_generation_average_time_on_job(df: DataFrame) -> DataFrame:
    """
    Calculates the average time on the job in years for each generation and
    level description.

    Groups the 'df' DataFrame by 'generation' and 'level_description'
    columns, and calculates the average time on the job in years for each
    group.

    Parameters:
        df (DataFrame): DataFrame containing joined data with columns
        'generation' and 'level_description'.

    Returns:
        DataFrame: DataFrame grouped by generation and level description with
        the average time on the job in years.
    """
    generation_average_time_on_job = (df.groupBy("generation",
                                                       "level_description").agg(
        F.avg(df.time_on_job / 365.25)
        .alias("average_time_on_jog_per_age"))
        .orderBy("average_time_on_jog_per_age", ascending=False))

    return generation_average_time_on_job


def calculate_area_generation_average_time_on_job(df: DataFrame) -> DataFrame:
    """
    Calculates the average time on the job in years for each generation and
    area.

    Groups the 'df' DataFrame by 'generation' and 'area' columns,
    and calculates the average time on the job in years for each group.

    Parameters:
        df (DataFrame): DataFrame containing joined data with columns
        'generation' and 'area'.

    Returns:
        DataFrame: DataFrame grouped by generation and area with the average
        time on the job in years.
    """
    area_generation_average_time_on_job = df.groupBy("generation",
                                                            "area").agg(
        F.avg(df.time_on_job / 365.25).alias("average_time_on_jog_per_age")
    ).orderBy("average_time_on_jog_per_age", ascending=False)

    return area_generation_average_time_on_job


def calculate_generation_region(df: DataFrame) -> DataFrame:
    """
    Calculates the total number of employees for each generation in each
    region.

    Groups the 'df' DataFrame by 'region' and 'generation' columns,
    and counts the distinct encrypted CPFs ('cpf_encrypted') for each group.

    Parameters:
        df (DataFrame): DataFrame containing joined data with columns
        'region' and 'generation'.

    Returns:
        DataFrame: DataFrame grouped by region and generation with the total
        number of employees.
    """
    generation_region = df.groupBy("region", "generation").agg(
        F.countDistinct("cpf_encrypted").alias("total_employees")
    ).orderBy("total_employees", ascending=False)

    return generation_region


def process_and_input_on_golden():

    spark = SparkSession.builder \
        .appName("my-session") \
        .getOrCreate()

    base_cep = spark.read.parquet("opt/airflow/output/silver/base_cep")
    base_funcionarios = spark.read.parquet("opt/airflow/output/silver/base_funcionarios")
    base_cargos = spark.read.parquet("opt/airflow/output/silver/base_cargos")

    df, df_agg1 = join_and_group_by_region(base_funcionarios, base_cep)
    save_data_agg(df_agg1, "./opt/airflow/output/golden/region")

    df, df_agg2 = calculate_average_time_on_job(df, base_cargos)
    save_data_agg(df_agg2, "./opt/airflow/output/golden/average_time_on_job")

    df = classify_generation_in_dataframe(df)
    df_agg3 = calculate_generation_average_time_on_job(df)
    save_data_agg(df_agg3, "./opt/airflow/output/golden/generation_average_time_on_job")
    
    df_agg4 = calculate_area_generation_average_time_on_job(df)
    save_data_agg(df_agg4, "./opt/airflow/output/golden/area_generation_average_time_on_job")

    df_agg5 = calculate_generation_region(df)
    save_data_agg(df_agg5, "./opt/airflow/output/golden/generation_region")

if __name__ == "__main__":
    process_and_input_on_golden()
