import os
import sys
import re
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Generator

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType

sys.path.append('src')
from utils.logger import get_logger

LOGGER = get_logger(Path(__file__).stem)
APPNAME = 'simple-ETL'


@contextmanager
def spark_manager(env: str) -> Generator[SparkSession, None, None]:
    """_summary_

    Args:
        env (str): _description_

    Yields:
        Generator[SparkSession, None, None]: _description_
    """
    spark = SparkSession \
        .builder \
        .appName(APPNAME) \
        .getOrCreate()

    try:
        yield spark
    except Exception as e:
        print(e)
    finally:
        spark.stop()


def extract(spark: SparkSession, file_path: str) -> DataFrame:
    """_summary_

    Args:
        spark (SparkSession): _description_
        file_path (str): _description_

    Returns:
        DataFrame: _description_
    """
    try:
        HOST = os.environ.get('HOST')
        PORT = os.environ.get('PORT')
        DATABASE = os.environ.get('DATABASE')
        USERNAME = os.environ.get('USERNAME')
        PASSWORD = os.environ.get('PASSWORD')

        url = f"jdbc:sqlserver://{HOST}:{PORT};databaseName={DATABASE}"
        properties = {
            "user": {USERNAME},
            "password": PASSWORD
        }
        return spark.read.jdbc(url, "scripts", properties=properties)

    except Exception as e:
        return spark.read.csv(file_path, sep="~", header=True)


def transform(df: DataFrame) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    def clean_text(text):
        text = text.lower()
        text = re.sub(r'@\S+', '', text)
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'pic.\S+', '', text)
        text = re.sub(r"[^a-zA-Z+']", ' ', text)
        text = re.sub(r'\s+[a-zA-Z]\s+', ' ', text+' ')
        text = re.sub("\s[\s]+", " ", text).strip()
        return text

    clean_text_udf = udf(clean_text, StringType())
    cleaned_df = df.select(col(
        df.columns[0]),
        *[clean_text_udf(column).alias(column) for column in df.columns[1:]]
    )
    return cleaned_df


def load(df: DataFrame, write_path: str = None) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        write_path (str, optional): _description_. Defaults to None.

    Returns:
        DataFrame: _description_
    """
    formatted_datetime = datetime.now().strftime("%d%m%Y_%H%M")
    if write_path:
        df.coalesce(1).write.option("sep", ":::").mode("overwrite").csv(write_path)
    else:
        df.coalesce(1).write.option("sep", ":::").csv(f'/opt/spark-output/{APPNAME}-{formatted_datetime}', header=True)


def run_job():
    """_summary_
    """
    with spark_manager(env='dev') as spark:
        df = extract(spark=spark, file_path="/opt/spark-jobs/job_2/data/processed_data.csv")
        df = transform(df)
        load(df)


if __name__ == '__main__':
    run_job()
