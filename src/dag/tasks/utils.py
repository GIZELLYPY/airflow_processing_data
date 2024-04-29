from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
import pytz
from cryptography.fernet import Fernet
from pyspark.sql.types import BinaryType
import hashlib


def read_csv_to_df(
    spark: SparkSession, file_path: str, delimiter: str = ";"
) -> DataFrame:
    """
    Reads a CSV file into a PySpark DataFrame.

    :param spark: The SparkSession object.
    :param file_path: The path to the CSV file.
    :param delimiter: The delimiter used in the CSV file. Default is ';'.
    :return: A PySpark DataFrame with the contents of the CSV file.

    Usage:
    >>> spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    >>> file_path = "file_path.csv"
    >>> df = read_csv_to_df(spark, file_path)
    """

    df = spark.read.csv(file_path, header=True,
                        inferSchema=True, sep=delimiter)
    return df


def save_data_agg(df: DataFrame, output_path: str) -> None:
    """
    Saves aggregated data to a specified output path in Parquet format.

    This function takes a DataFrame 'df' containing aggregated data and saves
    it to the specified 'output_path' in Parquet format. It adds a 'created_at'
    column to the DataFrame with the current UTC timestamp before writing to
    Parquet.

    Parameters:
        df (DataFrame): The DataFrame containing aggregated data to be saved.
        output_path (str): The output path where the Parquet file will be 
        saved.

    Returns:
        None
    """
    utc_now = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn("created_at", F.lit(utc_now))
    df.write.parquet(output_path, mode="overwrite")


def save_data_hire_date_partition(df: DataFrame, output_path: str) -> None:
    """
    Processes the input DataFrame by adding a 'created_at' column with the
    current UTC datetime, extracting year and month from 'contract_start_date',
    and then writing the DataFrame to a partitioned Parquet file.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - output_path (str): The file path where the partitioned Parquet files
    will be saved.

    The function adds columns for the year and month of the contract start
    date and writes the DataFrame to disk partitioned by these new columns.
    This facilitates efficient storage and querying based on the date
    components.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> output_path = "silver/base_cliente"
    >>> process_and_save_data(df, output_path)
    """

    df = df.withColumn("year_hire_date", F.year("hire_date")).withColumn(
        "month_hire_date", F.month("hire_date")
    )

    utc_now = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = df.withColumn("created_at", F.lit(utc_now))

    df.write.partitionBy("year_hire_date", "month_hire_date").parquet(
        output_path, mode="overwrite"
    )

    
def save_data_contract_start_date_partition(df: DataFrame,
                                            output_path: str) -> None:
    """
    Processes the input DataFrame by adding a 'created_at' column with the
    current UTC datetime, extracting year and month from 'contract_start_date',
    and then writing the DataFrame to a partitioned Parquet file.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - output_path (str): The file path where the partitioned Parquet files will
    be saved.

    The function adds columns for the year and month of the contract start date
    and writes
    the DataFrame to disk partitioned by these new columns. This facilitates
    efficient storage
    and querying based on the date components.

    Usage:
    >>> df = spark.read.csv("file_path", header=True, inferSchema=True)
    >>> output_path = "silver/base_cliente"
    >>> process_and_save_data(df, output_path)
    """

    df = df.withColumn("year_contract_start_date",
                       F.year("contract_start_date")) \
           .withColumn("month_contract_start_date",
                       F.month("contract_start_date"))

    utc_now = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn("created_at", F.lit(utc_now))

    df.write.partitionBy("year_contract_start_date",
                         "month_contract_start_date") \
        .parquet(output_path, mode="overwrite")


def generate_key():
    """Generate and return a Fernet encryption key."""
    return Fernet.generate_key()


def encrypt_value(key):
    """
    Create a user-defined function (UDF) for Apache Spark to deterministically
    encrypt values. This function applies a SHA-256 hash to the input value to
    ensure deterministic output for the same input values across multiple
    executions, and then encrypts the hash using the Fernet symmetric
    encryption scheme.

    The deterministic nature of this function is achieved by hashing the input,
    which ensures that the same input value will always result in the same
    encrypted output, making it suitable for operations that require consistent
    encrypted results, such as counting distinct values in an encrypted column.
    Note that the hashing step makes the encryption non-reversible directly
    to the original value.

    Parameters:
    - key (bytes): A Fernet encryption key generated using
                    Fernet.generate_key(),
                   which is used to encrypt the hashed input value.

    Returns:
    - function: A Spark UDF function that takes a column containing the values
                to encrypt.
                The output is a BinaryType column with the encrypted data.
                The UDF will return None for any input value that is None.

    Example usage in a PySpark DataFrame:
    >>> key = generate_key()
    >>> encrypt_udf = deterministic_encrypt_value(key)
    >>> df = df.withColumn("cpf_encrypted", encrypt_udf(df["cpf"]))
    >>> df = df.withColumn("rg_encrypted", encrypt_udf(df["id_card_number_rg"]))
    """
    def encrypt(val):
        if val is not None:
      
            hasher = hashlib.sha256()
            hasher.update(str(val).encode('utf-8'))
            hashed_value = hasher.digest()
            cipher_suite = Fernet(key)
            encrypted_value = cipher_suite.encrypt(hashed_value)
            return encrypted_value
        return None
    return F.udf(encrypt, BinaryType())


def save_encrypted_columns_and_key(df: DataFrame,
                                   key: bytes,
                                   name: str) -> DataFrame:
    """
    Saves the encryption key to a file and stores a subset of DataFrame
    columns in a Parquet format while anonymizing sensitive data. It then
    drops the sensitive columns from the original DataFrame and returns the
    modified DataFrame with a timestamp of the operation.

    Parameters:
    - df (DataFrame): The PySpark DataFrame containing sensitive data that
    needs anonymization.
    - key (bytes): The Fernet encryption key used for encrypting sensitive
    data, provided as bytes.

    Returns:
    - DataFrame: The original DataFrame with sensitive columns removed and a
    new 'created_at' column indicating the timestamp when the data was
    processed.

    Side Effects:
    - Writes the encryption key to a binary file named 'encryption_key'.
    - Writes a subset of the DataFrame including encrypted data to a Parquet
    file under the path 'silver/security_zone/anonymization_employees'.

    Usage:
    >>> df = spark.read.parquet("path_to_employee_data")
    >>> key = Fernet.generate_key()
    >>> cleaned_df = save_encrypted_columns_and_key(df, key)
    """
    with open(f"encryption_key{name}", 'wb') as file:
        file.write(key)
    anonymization = df.select(["id_card_number_rg",
                               "cpf",
                               "full_name",
                               "cpf_encrypted",
                               "rg_encrypted",
                               "login"])
    utc_now = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn("created_at", F.lit(utc_now))
    anonymization.write.parquet(f"silver/security_zone/{name}",
                                mode="overwrite")
    to_drop = ["id_card_number_rg", "cpf", "full_name", "login"]
    encryption = df.drop(*to_drop)
    return encryption
