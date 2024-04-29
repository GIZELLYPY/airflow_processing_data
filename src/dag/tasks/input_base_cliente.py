from pyspark.sql import SparkSession,  DataFrame
from pyspark.sql import functions as F
import great_expectations as ge

from utils import save_data_contract_start_date_partition, read_csv_to_df


def preprocess_annual_contract_value(df: DataFrame) -> DataFrame:
    """
    Processes the input DataFrame by filtering out rows based on conditions on
    the 'Cliente' column, casting 'Valor Contrato Anual' to float, and handling
    null values by conditionally replacing them with values from 'Quantidade
    de Serviços'.

    :param df: The input DataFrame to be processed.
    :return: The processed DataFrame with the transformations applied.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    >>> processed_df = preprocess_data(df)
    """

    df = df.filter(
        (~df["Cliente"].rlike("^[A-Z]{2,}$")) &
        (F.col("Cliente").isNotNull())
    ).orderBy("Valor Contrato Anual")

    df = df.withColumn("Valor Contrato Anual", F.col(
        "Valor Contrato Anual").cast("float"))

    df = df.withColumn(
        "Valor Contrato Anual",
        F.when(
            F.col("Valor Contrato Anual").isNull() & ~F.col(
                "Quantidade de Serviços").cast("date").isNotNull(),
            F.col("Quantidade de Serviços")
        ).otherwise(F.col("Valor Contrato Anual"))
    ).withColumn("Valor Contrato Anual",
                 F.col("Valor Contrato Anual").cast("float"))

    return df


def preprocess_number_of_services(df: DataFrame) -> DataFrame:
    """
    Preprocess the 'Quantidade de Serviços' column in the given DataFrame.

    The function performs the following operations:
    - Sets the value to None if the current value can be cast to a date.
    - Casts non-null values to integers.
    - Replaces null values or values greater than or equal to 200 with the
    corresponding 'Cargo Responsável' value.
    - Ensures all values in 'Quantidade de Serviços' are cast to integers.

    :param df: The input DataFrame with a 'Quantidade de Serviços' column to
    preprocess.
    :return: The DataFrame with the 'Quantidade de Serviços' column
    preprocessed.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    >>> processed_df = preprocess_number_of_services(df)
    """

    df = df.withColumn(
        "Quantidade de Serviços",
        F.when(
            F.col("Quantidade de Serviços").cast("date").isNotNull(),
            F.lit(None)
        ).otherwise(
            F.col("Quantidade de Serviços").cast("int")
        )
    )

    df = df.withColumn(
        "Quantidade de Serviços",
        F.when(
            (F.col("Quantidade de Serviços").isNull()) | (
                F.col("Quantidade de Serviços") >= 200),
            F.col("Cargo Responsável")
        ).otherwise(F.col("Quantidade de Serviços"))
    )

    df = df.withColumn("Quantidade de Serviços", F.col(
        "Quantidade de Serviços").cast("int"))

    return df


def preprocess_responsible_position(df: DataFrame) -> DataFrame:
    """
    Preprocess the 'Cargo Responsável' column in the DataFrame.

    The function performs the following operations:
    - Sets the value to None if the current value can be cast to an integer.
    - Ensures that non-null 'Cargo Responsável' values are cast to strings.
    - For 'Cargo Responsável' values that are null and corresponding 'CEP'
    cannot be cast to an integer,
      it replaces 'Cargo Responsável' with the value from 'CEP'.

    :param df: The input DataFrame with a 'Cargo Responsável' column to
    preprocess.
    :return: The DataFrame with the 'Cargo Responsável' column preprocessed.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    >>> processed_df = preprocess_responsible_position(df)
    """

    df = df.withColumn(
        "Cargo Responsável",
        F.when(
            F.col("Cargo Responsável").cast("int").isNotNull(),
            F.lit(None)
        ).otherwise(
            F.col("Cargo Responsável").cast("string")
        )
    )

    df = df.withColumn(
        "Cargo Responsável",
        F.when(
            (F.col("Cargo Responsável").isNull()) & ~F.col(
                "CEP").cast("int").isNotNull(),
            F.col("CEP")
        ).otherwise(F.col("Cargo Responsável"))
    )

    return df


def preprocess_cep(df: DataFrame) -> DataFrame:
    """
    Preprocesses the 'CEP' column in a PySpark DataFrame.

    This function performs the following steps:
    1. Validates the 'CEP' column values using a regex pattern that matches
    strings of 5 or more digits and sets the value to null if it does not
    match.
    2. If 'CEP' is null and 'Data Início Contrato' cannot be cast to a date,
       then 'Data Início Contrato' is used to replace 'CEP'.

    :param df: The input DataFrame with a 'CEP' column.
    :return: The DataFrame with processed 'CEP' column.

    Usage:
    >>> processed_df = preprocess_cep(df)
    """

    df = df.withColumn(
        "CEP",
        F.when(

            F.col("CEP").rlike("^[0-9]{5,}$"),
            F.col("CEP")
        ).otherwise(
            F.lit(None)
        )
    )

    df = df.withColumn(
        "CEP",
        F.when(
            (F.col("CEP").isNull()) & (
                ~F.col("Data Início Contrato").cast("date").isNotNull()),
            F.col("Data Início Contrato")
        ).otherwise(F.col("CEP"))
    )

    return df


def preprocess_contract_start_date(df: DataFrame) -> DataFrame:
    """
    Preprocesses the 'Data Início Contrato' column in a PySpark DataFrame.

    The function performs the following operations:
    1. If 'Data Início Contrato' can be cast to an integer, it sets the value
    to None.
    2. Otherwise, it tries to convert 'Data Início Contrato' to a date using
    the format 'dd/MM/yyyy'.
    3. If 'Data Início Contrato' is still null and cannot be cast to an
    integer, it attempts to convert the 'Nivel de Importancia' column to a date
    using the same format.

    Parameters:
    - df (DataFrame): The input DataFrame with a 'Data Início Contrato' and
    'Nivel de Importancia' columns.

    Returns:
    - DataFrame: The DataFrame with the 'Data Início Contrato' column
    processed.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True)
    >>> processed_df = preprocess_contract_start_date(df)
    """

    df = df.withColumn(
        "Data Início Contrato",
        F.when(
            F.col("Data Início Contrato").cast("int").isNotNull(),
            F.lit(None)
        ).otherwise(
            F.to_date(F.col("Data Início Contrato"), "dd/MM/yyyy")
        )
    )

    df = df.withColumn(
        "Data Início Contrato",
        F.when(
            (F.col("Data Início Contrato").isNull()) & ~F.col(
                "Data Início Contrato").cast("int").isNotNull(),
            F.to_date(F.col("Nivel de Importancia"), "dd/MM/yyyy")
        ).otherwise(F.col("Data Início Contrato"))
    )

    return df


def preprocess_importance_level(df: DataFrame) -> DataFrame:
    """
    Processes and conditions the 'Nivel de Importancia' column in the
    DataFrame based on date validation and other column values.

    Steps:
    1. Sets 'Nivel de Importancia' to None if 'Data Início Contrato' cannot
    be parsed as a date.
    2. Otherwise, attempts to cast 'Nivel de Importancia' as an integer.
    3. If 'Nivel de Importancia' is null after these operations and a
    secondary column '_c7' can be cast to an integer,
    then 'Nivel de Importancia' is set to the value of '_c7'.

    Parameters:
    - df (DataFrame): The input DataFrame that includes the
    'Nivel de Importancia' and 'Data Início Contrato' columns.

    Returns:
    - DataFrame: The DataFrame with updated 'Nivel de Importancia' values.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True)
    >>> processed_df = preprocess_importance_level(df)
    """

    df = df.withColumn(
        "Nivel de Importancia",
        F.when(
            F.to_date(F.col("Data Início Contrato"), "dd/MM/yyyy").isNull(),
            F.lit(None)
        ).otherwise(
            F.col("Nivel de Importancia").cast("int")
        )
    )

    df = df.withColumn(
        "Nivel de Importancia",
        F.when(
            (F.col("Nivel de Importancia").isNull()) & (
                F.col("_c7").cast("int").isNotNull()),
            F.col("_c7")
        ).otherwise(F.col("Nivel de Importancia"))
    )

    return df


def capitalize_client_names(df: DataFrame) -> DataFrame:
    """
    Capitalizes the first letter of each word in the 'Cliente' column of
    the provided DataFrame.

    This function modifies the 'Cliente' column by applying the initcap
    function, which converts the
    first character of each word to uppercase and the rest to lowercase.

    Parameters:
    - df (DataFrame): The input DataFrame that contains the 'Cliente' column.

    Returns:
    - DataFrame: The DataFrame with the 'Cliente' column capitalized.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True)
    >>> processed_df = capitalize_client_names(df)
    """

    df = df.withColumn("Cliente", F.initcap(F.col("Cliente")))

    return df


def filter_non_null_responsibles(df: DataFrame,
                                 column_name: str = "Cargo Responsável") -> DataFrame:
    """
    Filters rows in the DataFrame where the specified column contains non-null
    values.

    This function ensures that all data returned in the DataFrame has valid,
    non-null entries in the 'Cargo Responsável' column, or a specified column.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - column_name (str): The name of the column to check for non-null values.
    Defaults to 'Cargo Responsável'.

    Returns:
    - DataFrame: A DataFrame with rows where the specified column is non-null.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True)
    >>> filtered_df = filter_non_null_responsibles(df)
    """

    df_filtered = df.filter(F.col(column_name).isNotNull())

    return df_filtered


def select_and_rename_columns(df: DataFrame) -> DataFrame:
    """
    Selects specific columns from the DataFrame and renames them to more
    descriptive names.

    This function simplifies the DataFrame by selecting only essential columns
    and renaming them for consistency and easier access in downstream
    processing.

    Parameters:
    - df (DataFrame): The input DataFrame.

    Returns:
    - DataFrame: A DataFrame with selected columns renamed.

    Usage:
    >>> df = spark.read.csv(file_path, header=True, inferSchema=True)
    >>> processed_df = select_and_rename_columns(df)
    """

    df_selected = df.select([
        "Cliente",
        "Valor Contrato Anual",
        "Quantidade de Serviços",
        "Cargo Responsável",
        "CEP",
        "Data Início Contrato",
        "Nivel de Importancia"
    ])

    df_renamed = df_selected.withColumnRenamed("Cliente", "customer") \
        .withColumnRenamed("Valor Contrato Anual", "annual_contract_value") \
        .withColumnRenamed("Quantidade de Serviços", "number_of_services") \
        .withColumnRenamed("Cargo Responsável", "responsible_position") \
        .withColumnRenamed("Data Início Contrato", "contract_start_date") \
        .withColumnRenamed("Nivel de Importancia", "importance_level") \
        .withColumnRenamed("CEP", "cep")

    return df_renamed


def check_quality(df: DataFrame) -> None:
    """
    Validates several aspects of a DataFrame using Great Expectations.
    Throws an exception if any expectation is not met.

    Validates that several columns should not be null, checks specific
    columns for minimum values, and verifies that a column's values are
    within a specific set.

    Parameters:
    - df (DataFrame): The input DataFrame to validate.

    Raises:
    - Exception: If any of the validations fail.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> validate_dataframe(df)
    """

    ge_df = ge.dataset.SparkDFDataset(df)

    expectations = [
        {"column": "customer",
         "expectation": "expect_column_values_to_not_be_null"},
        {"column": "annual_contract_value",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "number_of_services",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "responsible_position",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "contract_start_date",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "importance_level",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "importance_level",
            "expectation": "expect_column_values_to_be_in_set",
            "args": [[1, 2, 3, 4]]}
    ]

    for exp in expectations:
        expectation_method = getattr(ge_df, exp["expectation"])
        if "args" in exp:
            expectation_method(exp["column"], *exp["args"])
        else:
            expectation_method(exp["column"])

    result = ge_df.validate()
    if not result["success"]:
        failed_tests = [exp for exp in result["results"] if not exp["success"]]
        raise Exception(f"Data validation failed for: {failed_tests}")


def input_on_silver_base_cliente():
    spark = SparkSession.builder \
        .appName("my-session") \
        .getOrCreate()

    df = read_csv_to_df(spark, "./input/BaseClientes.csv")

    df = preprocess_annual_contract_value(df)
    df = preprocess_number_of_services(df)
    df = preprocess_responsible_position(df)
    df = preprocess_cep(df)
    df = preprocess_contract_start_date(df)
    df = preprocess_importance_level(df)
    df = capitalize_client_names(df)
    df = filter_non_null_responsibles(df)
    df = select_and_rename_columns(df)

    check_quality(df)

    save_data_contract_start_date_partition(df, "./opt/airflow/output/silver/base_clientes")


if __name__ == "__main__":
    input_on_silver_base_cliente()
