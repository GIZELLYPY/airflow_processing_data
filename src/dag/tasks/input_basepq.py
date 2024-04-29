from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark.sql.types import DateType

from utils import (
    save_data_hire_date_partition,
    generate_key,
    encrypt_value,
    save_encrypted_columns_and_key,
    read_csv_to_df
)


def select_and_rename_columns(df):
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

    df = df.select(
        [
            "ID RH",
            "RG",
            "CPF",
            "Ramal",
            "Estado Civil",
            "Nome Completo",
            "Login",
            "Data de Nascimento",
            "CEP",
            "Data de Contratacao",
            "Data de Demissao",
            "Dias Uteis Trabalhados Ano Orcamentario",
            "Salario Base",
            "Impostos",
            "Beneficios",
            "VT",
            "VR",
            "Cargo17",
            "Nível43",
            "Nível51",
            "Bandeira",
            "Codigos",
            "Quantidade de Acessos",
            "Ferias Acumuladas",
            "Ferias Remuneradas",
            "Horas Extras",
            "Valores Adicionais",
        ]
    )

    column_mapping = {
        "ID RH": "hr_id",
        "RG": "id_card_number_rg",
        "CPF": "cpf",
        "Ramal": "extension",
        "Estado Civil": "marital_status",
        "Nome Completo": "full_name",
        "Login": "login",
        "Data de Nascimento": "birth_date",
        "CEP": "cep",
        "Data de Contratacao": "hire_date",
        "Data de Demissao": "termination_date",
        "Dias Uteis Trabalhados Ano Orcamentario": "workdays_in_fiscal_year",
        "Salario Base": "base_salary",
        "Impostos": "taxes",
        "Beneficios": "benefits",
        "VT": "transport_voucher",
        "VR": "meal_voucher",
        "Cargo": "position",
        "Bandeira": "flag",
        "Codigos": "codes",
        "Quantidade de Acessos": "access_count",
        "Ferias Acumuladas": "accumulated_vacation",
        "Ferias Remuneradas": "paid_vacation",
        "Horas Extras": "overtime_hours",
        "Valores Adicionais": "additional_amounts",
        "ID de Pessoal": "personnel_id",
        "ID da area": "department_id",
        "Cargo17": "responsible_position",
        "Nível43": "level_description",
        "Nível51": "level",
    }

    for portuguese_name, english_name in column_mapping.items():
        df = df.withColumnRenamed(portuguese_name, english_name)

    return df


def process_date_columns(df: DataFrame) -> DataFrame:
    """
    Converts Excel serial date numbers in three specific columns
    ('Data de Nascimento','Data de Contratacao', and 'Data de Demissao') to
    standard date formats. This function assumes the input dates are stored as
    Excel serial date numbers and need conversion to standard date formats
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

    df = df.withColumn(
        "birth_date",
        F.expr("date_add(to_date('1899-12-30'), `birth_date`)").cast(DateType()),
    )

    df = df.withColumn(
        "hire_date",
        F.expr("date_add(to_date('1899-12-30'), `hire_date`)").cast(DateType()),
    )

    df = df.withColumn(
        "termination_date",
        F.expr("date_add(to_date('1899-12-30'), `termination_date`)").cast(DateType()),
    )

    # Verify the data type of the column
    for column_name in ["birth_date", "hire_date", "termination_date"]:
        if df.schema[column_name].dataType != DateType():
            raise ValueError(f"{column_name} is not of DateType.")

    return df


def input_on_silver_basepq():

    spark = SparkSession.builder.appName("my-session").getOrCreate()

    df = read_csv_to_df(spark, "./input/BasePQ.csv", ";")

    df = select_and_rename_columns(df)

    df = process_date_columns(df)

    key = generate_key()
    encrypt_udf = encrypt_value(key)
    df = df.withColumn("cpf_encrypted", encrypt_udf(df["cpf"]))
    df = df.withColumn("rg_encrypted", encrypt_udf(df["id_card_number_rg"]))

    df = save_encrypted_columns_and_key(df, key, "anonymization_basepq")

    save_data_hire_date_partition(df, "./opt/airflow/output/silver/basepq")


if __name__ == "__main__":
    input_on_silver_basepq()
