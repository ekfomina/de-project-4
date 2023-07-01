from typing import Dict, List

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import *  # noqa
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, StructField, TimestampType


def get_spark_session(task: str) -> SparkSession:
    return (
        SparkSession.builder.appName(task)
        .config("spark.sql.streaming.metricsEnabled", True)
        .enableHiveSupport()
        .getOrCreate()
    )


class SparkWrapper:
    """
    Класс-обертка для работы со Spark. Содержит объект Spark-сессия.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.logger = spark._jvm.org.apache.log4j
        self.log = self.logger.LogManager.getLogger(__name__)
        self.log.info("Initializing")
        self.time_zone = dict(spark.sparkContext.getConf().getAll()).get("spark.sql.session.timeZone")
        if not self.time_zone:
            raise ValueError("Cannot define spark timezone from param: spark.sql.session.timeZone")

    def convert_timestamp_to_utc(self, column: Column) -> Column:
        return to_utc_timestamp(to_timestamp(column / 1000000000), self.time_zone)

    def deduplicate(self, df: DataFrame, columns: List, order: str) -> DataFrame:
        from pyspark.sql.window import Window

        W = Window.partitionBy(columns).orderBy(desc(order))
        return df.withColumn("rn", row_number().over(W)).where("rn = 1").drop("rn")

    def json_conf_to_dict(self, conf: str) -> Dict:
        import json

        JSON_HEAD = "{"
        JSON_TAIL = "}"

        if not conf.startswith(JSON_HEAD):
            conf = JSON_HEAD + conf
        if not conf.endswith(JSON_TAIL):
            conf += JSON_TAIL

        return json.loads(conf)

    def get_case_insensitive_filter_conditions(self, col1: Column, col2: Column) -> bool:
        return lower(col1) == lower(col2)

    def calc_shecksum(
        self,
        df: DataFrame,
        cols_to_checksum: List[str],
        drop_concat_column: bool = True,
        delimetr: str = "-",
        hash_func_name: str = "sha1",
    ) -> DataFrame:  # noqa
        """Расчитываем чексумму аналогично sql server"""

        def convert_col_to_checksum(c: StructField) -> Column:
            if c.dataType == BooleanType():
                return coalesce(col(c.name).cast("int"), lit(""))
            elif c.dataType == TimestampType():
                return coalesce(date_format(col(c.name), "yyyy-MM-dd HH:mm:ss.SSS0000").cast("string"), lit(""))
            else:
                return coalesce(col(c.name), lit(""))

        checksum_fields = list(filter(lambda c: c.name in cols_to_checksum, df.schema.fields))

        if hash_func_name == "sha1":
            hash_func = sha1
            col_name = "ChecksumSHA1"
        elif hash_func_name == "md5":
            hash_func = md5
            col_name = "ChecksumMD5"
        else:
            raise ValueError("hash_func_name must be sha1 or md5")

        checksum_df = df.select(
            "*", concat_ws(delimetr, *[convert_col_to_checksum(c) for c in checksum_fields]).alias("ConcatCols")
        ).withColumn(col_name, hash_func(col("ConcatCols")))

        if drop_concat_column:
            return checksum_df.drop("ConcatCols")
        else:
            return checksum_df
