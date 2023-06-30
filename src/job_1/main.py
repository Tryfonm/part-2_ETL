import sys
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Generator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, sum
from pyspark.sql.types import IntegerType

sys.path.append('src')
from utils.logger import get_logger
LOGGER = get_logger(Path(__file__).stem)


@contextmanager
def spark_manager(env: str) -> Generator[SparkSession, None, None]:

    APPNAME = 'mySimleApp'
    spark = SparkSession \
        .builder \
        .appName(APPNAME) \
        .getOrCreate()

    try:
        yield spark
    except Exception as e:
        LOGGER.debug(e)
    finally:
        spark.stop()


def run_job() -> None:

    APPNAME = 'mySimleApp'
    formatted_datetime = datetime.now().strftime("%d%m%Y_%H%M")

    with spark_manager(env='dev') as spark:
        df = spark.read.text('/opt/spark-jobs/job_1/input.txt')

        def count_letter_a(string):
            return string.lower().count('a')

        count_letter_a_udf = udf(count_letter_a, IntegerType())
        df_with_count = df.withColumn("letter_a_count", count_letter_a_udf(col("value")))
        df_count_sum = df_with_count.select(sum("letter_a_count"))

        df_count_sum.write.csv(f'/opt/spark-output/{APPNAME}-{formatted_datetime}', header=True)

        df_with_count.show()
        df_count_sum.show()


if __name__ == '__main__':
    run_job()
