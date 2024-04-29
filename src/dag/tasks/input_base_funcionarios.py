
from pyspark.sql import SparkSession,  DataFrame
from pyspark.sql import functions as F
import great_expectations as ge
from datetime import date

from pyspark.sql.types import DateType


from utils import (
    save_data_hire_date_partition,
    generate_key,
    encrypt_value,
    save_encrypted_columns_and_key,
    read_csv_to_df
)


def drop_unnecessary_columns(df: DataFrame) -> DataFrame:
    """
    Drops a predefined list of unnecessary columns from the input DataFrame.

    This function improves the DataFrame by removing columns that are not
    needed for further analysis or processing. The columns are identified
    by their labels.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame from which columns will be
    dropped.

    Returns:
    - DataFrame: The DataFrame after removing the specified columns.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> cleaned_df = drop_unnecessary_columns(df)
    """

    columns_to_drop = [
        '_c1', '_c3', '_c5', '_c7', '_c9', '_c11', '_c13', '_c15', '_c17',
        '_c19', '_c21', '_c23', '_c25', '_c27', '_c29', '_c31', '_c33', '_c35',
        '_c37', '_c39', '_c41', '_c43', '_c45', '_c47', '_c49', '_c51'
    ]

    df = df.drop(*columns_to_drop)

    return df


def process_date_columns(df: DataFrame) -> DataFrame:
    """
    Converts Excel serial date numbers in three specific columns
    ('Data de Nascimento', 'Data de Contratacao', and 'Data de Demissao')
    to standard date formats. This function assumes the input dates are stored
    as Excel serial date numbers and need conversion to standard date formats
    suitable for further processing and analysis.

    The function also ensures that the data types of these date columns are
    correctly converted to Spark SQL DateType. If the conversion fails for any
    of these columns, it raises a ValueError.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame containing the date columns.

    Returns:
    - DataFrame: The DataFrame with the date columns converted to standard
    date format.

    Raises:
    - ValueError: If any of the specified date columns does not convert to
    DateType, indicating an issue with the data conversion.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> processed_df = process_date_columns(df)
    """

    df = (df.withColumn(
        "Data de Nascimento",
        F.expr("date_add(to_date('1899-12-30'), `Data de Nascimento`)")
            .cast(DateType())
    ))

    df = (df.withColumn(
        "Data de Contratacao",
        F.expr("date_add(to_date('1899-12-30'), `Data de Contratacao`)")
            .cast(DateType())
    ))

    df = (df.withColumn(
        "Data de Demissao",
        F.expr("date_add(to_date('1899-12-30'), `Data de Demissao`)")
            .cast(DateType())
    ))

    # Verify the data type of the column
    for column_name in ["Data de Nascimento",
                        "Data de Contratacao",
                        "Data de Demissao"]:
        if df.schema[column_name].dataType != DateType():
            raise ValueError(f"{column_name} is not of DateType.")

    return df


def calculate_time_on_job(df: DataFrame) -> DataFrame:
    """
    Calculates the duration of employment from the 'Data de Contratacao' date
    to the 'Data de Demissao' date.
    If 'Data de Demissao' is null, it uses the current date to calculate the
    ongoing duration of employment.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame that must include
            'Data de Contratacao' and 'Data de Demissao' columns with dates.

    Returns:
    - DataFrame: The modified DataFrame that includes a new column
                'time_on_job' which holds the duration of employment in days.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> result_df = calculate_time_on_job(df)
    """

    df = df.withColumn("temp_data_demissao",
                       F.when(F.col("Data de Demissao").isNull(),
                              F.lit(date.today().strftime("%Y-%m-%d")))
                       .otherwise(F.col("Data de Demissao")))

    df = df.withColumn("time_on_job", F.datediff(
        F.col("temp_data_demissao"), F.col("Data de Contratacao")))

    df = df.drop("temp_data_demissao")

    return df


def calculate_person_age(df: DataFrame) -> DataFrame:
    """
    Calculates a person's age from the 'Data de Nascimento' (birth date)
    column, assuming it is given in a standard date format. The age is
    calculated based on the current date.

    Parameters:
    - df (DataFrame): The input PySpark DataFrame that must include
                    'Data de Nascimento' column with dates.

    Returns:
    - DataFrame: The modified DataFrame that includes a new column 'person_age'
                    which holds the age of the person in full years.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> result_df = calculate_person_age(df)
    """

    df = (df.withColumn("person_age", F.expr("floor(months_between(current_date(), \
                               `Data de Nascimento`) / 12)")))

    return df


def rename_columns(df: DataFrame) -> DataFrame:
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
        'ID RH': 'hr_id',
        'RG': 'id_card_number_rg',
        'CPF': 'cpf',
        'Ramal': 'extension',
        'Estado Civil': 'marital_status',
        'Nome Completo': 'full_name',
        'Login': 'login',
        'Data de Nascimento': 'birth_date',
        'CEP': 'cep',
        'Data de Contratacao': 'hire_date',
        'Data de Demissao': 'termination_date',
        'Dias Uteis Trabalhados Ano Orcamentario': 'workdays_in_fiscal_year',
        'Salario Base': 'base_salary',
        'Impostos': 'taxes',
        'Beneficios': 'benefits',
        'VT': 'transport_voucher',
        'VR': 'meal_voucher',
        'Cargo': 'position',
        'Bandeira': 'flag',
        'Codigos': 'codes',
        'Quantidade de Acessos': 'access_count',
        'Ferias Acumuladas': 'accumulated_vacation',
        'Ferias Remuneradas': 'paid_vacation',
        'Horas Extras': 'overtime_hours',
        'Valores Adicionais': 'additional_amounts',
        'ID de Pessoal': 'personnel_id',
        'ID da area': 'department_id'
    }

    for portuguese_name, english_name in column_mapping.items():
        df = df.withColumnRenamed(portuguese_name, english_name)

    return df


def cast_column_types(df: DataFrame) -> DataFrame:
    """
    Casts the data types of specific columns in a PySpark DataFrame. In this
    function, the 'taxes' column is cast to a float type, and the 'cep' column
    is cast to a string type. This can be useful for ensuring data consistency
    for numerical calculations and text processing.

    Parameters:
    - df (DataFrame): The input DataFrame containing at least the 'taxes' and
    'cep' columns.

    Returns:
    - DataFrame: The DataFrame with the 'taxes' column cast to float and the
    'cep' column cast to string.

    Usage:
    >>> spark = SparkSession.builder.appName("Cast Column Types").getOrCreate()
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> updated_df = cast_column_types(df)
    """

    df = df.withColumn("taxes", F.col("taxes").cast("float"))

    df = df.withColumn("cep", F.col("cep").cast("string"))

    return df


def check_quality(df: DataFrame) -> None:
    """
    Validates several aspects of a DataFrame using Great Expectations.
    Throws an exception if any expectation is not met.

    Validates that several columns should not be null, checks specific columns
    for minimum values, and verifies that a column's values are within a
    specific set.

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
        {"column": "id_card_number_rg",
            "expectation": "expect_column_values_to_not_be_null"},
        {"column": "cpf",
         "expectation": "expect_column_values_to_not_be_null"},
        {"column": "cpf",
         "expectation": "expect_column_values_to_be_unique"},
        {"column": "hr_id",
         "expectation": "expect_column_values_to_be_unique"},
        {"column": "birth_date",
         "expectation": "expect_column_values_to_be_between",
            "args": ["1900-01-01", str(date.today())]},
        {"column": "base_salary",
            "expectation": "expect_column_values_to_be_between",
            "args": [0, None]},
        {"column": "taxes",
         "expectation": "expect_column_values_to_match_regex",
            "args": [r'^\d+\.?\d*$']}

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


def input_on_silver_base_funcionarios():
    spark = SparkSession.builder \
        .appName("my-session") \
        .getOrCreate()

    df = read_csv_to_df(spark, "./input/BaseFuncionarios.csv", "|")
    df = drop_unnecessary_columns(df)
    df = process_date_columns(df)
    df = calculate_time_on_job(df)
    df = calculate_person_age(df)
    df = rename_columns(df)
    df = cast_column_types(df)

    check_quality(df)

    key = generate_key()
    encrypt_udf = encrypt_value(key)
    df = df.withColumn("cpf_encrypted", encrypt_udf(df["cpf"]))
    df = df.withColumn("rg_encrypted", encrypt_udf(df["id_card_number_rg"]))

    df = save_encrypted_columns_and_key(df, key, "anonymization_employees")

    save_data_hire_date_partition(df, "./opt/airflow/output/silver/base_funcionarios")


if __name__ == "__main__":
    input_on_silver_base_funcionarios()
