from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from kafka_writer import KafkaTopicParser

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType, StringType

class RabbitMqRenewalOfferAcceptedEvent(KafkaTopicParser):
    schema = StructType([
        StructField("properties_type", StringType(), True),
        StructField("message_date", StringType(), True),
        StructField("message_id", StringType(), True),
        StructField("delivery_mode", StringType(), True),
        StructField("headers", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("rabbit_entity_payload", StringType(), True)
        ])
    def parse_data(self) -> DataFrame:
        return self.df.select(
            self.df.content_type.cast("string").alias("content_type"),
            self.df.delivery_mode.alias("delivery_mode"),
            self.df.headers.alias("headers"),
            self.df.message_id.alias("message_id"),
            self.df.properties_type,
            self.df.rabbit_entity_payload.alias("rabbit_entity_payload"),
            current_timestamp().cast("timestamp").alias("load_timestamp"),
            self.df.message_date.cast("timestamp").alias("message_timestamp"),
            self.df.message_date.cast("date").alias("message_date")
        )
