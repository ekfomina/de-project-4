import logging
import pika
import datetime
import json
import dateutil

from pika.exceptions import ChannelWrongStateError, StreamLostError, AMQPConnectionError
from datetime import datetime
from pyspark.sql import DataFrame
from typing import List

from spark_wrapper import SparkWrapper
from queue_parser import RabbitMqRenewalOfferAcceptedEvent


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class RabbitConsumer:
    def __init__(
            self,
            spark_wrapper: SparkWrapper,
            queue_parser: RabbitMqRenewalOfferAcceptedEvent,
            queue_name: str,
            rabbitmq_servers: str,
            rabbitmq_port: int,
            rabbit_user: str,
            rabbit_password: str,
            rouring_key: str = '',
            batch_size: int = 5000
    ) -> None:
        self.spark_wrapper = spark_wrapper
        self.queue_parser = queue_parser
        self.queue_name = queue_name
        self.rouring_key = rouring_key
        self.batch_size = batch_size
        self.rabbitmq_servers = rabbitmq_servers
        self.rabbitmq_port = rabbitmq_port
        self.rabbit_user = rabbit_user
        self.rabbit_password = rabbit_password
        self.channel = None
        self.connection = None
        self.queue = None
        self.messages = []
        self.messages_reject = []
        self.delivery_tags = []

    def _get_credentials(self):
        LOGGER.info('Getting credentials')
        return pika.PlainCredentials(username=self.rabbit_user, password=self.rabbit_password)

    def _get_parameters(self):
        LOGGER.info('Connecting to %s', self.rabbitmq_servers)
        return pika.ConnectionParameters(host=self.rabbitmq_servers,
                                         port=self.rabbitmq_port,
                                         credentials=self._get_credentials())

    def connect(self) -> None:
        LOGGER.info('Connecting to %s', self.rabbitmq_servers)
        self.connection = pika.BlockingConnection(self._get_parameters())

    def open_channel(self) -> None:
        LOGGER.info('Creating a new channel')
        self.channel = self.connection.channel()

    def setup_queue(self) -> None:
        LOGGER.info('Declaring queue %s', self.queue_name)
        self.queue = self.channel.queue_declare(self.queue_name, durable=True, exclusive=False, auto_delete=False)

    def _validate_date_format(self, time_stamp):
        try:
            if isinstance(time_stamp, int):
                return datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                return dateutil.parser.parse(time_stamp).strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            return None

    def ack_batch_message(self) -> None:
        if self.channel.is_open:
            LOGGER.info('Acknowledge one or more messages')
            for delivery_tag in self.delivery_tags:
                self.channel.basic_ack(delivery_tag)
        else:
            LOGGER.info('Channel is already closed ')
            pass

    def _stop_consuming(self):
        LOGGER.info('Cancel all consumers by stop_consuming')
        self.channel.stop_consuming()

    def channel_close(self) -> None:
        if self.channel.is_open:
            LOGGER.info('Close channel')
            self._stop_consuming()
            self.channel.close()
        else:
            pass

    def connection_close(self) -> None:
        if self.connection.is_open:
            LOGGER.info('Close connection')
            self.connection.close()
        else:
            pass

    def _subtract_dataframes(self,
                             df_left: DataFrame,
                             df_right: DataFrame,
                             column_left: str,
                             column_right: str,
                             join: str) -> DataFrame:
        cond = (df_left[column_left] == df_right[column_right])

        return df_left. \
            join(df_right, on=cond, how=join). \
            select(df_left['*']). \
            where(df_right[column_right].isNull())

    def insert_into_hdfs(self, table: str, messages: List) -> None:
        if self.spark_wrapper.spark.catalog._jcatalog.tableExists(table):
            print(f"{table} existed.")

            df = self.spark_wrapper.spark.createDataFrame(messages, self.queue_parser.schema)

            df_new = self.queue_parser(df).parse_data()
            df_old = self.spark_wrapper.spark.table(table)

            df_result = self._subtract_dataframes(df_left=df_new, df_right=df_old, column_left='message_id',
                                                  column_right='message_id', join='left')
            df_result.write.insertInto(table, overwrite=False)
        else:
            print("It is necessary to create a table!")

    def on_batch_messages(self):
        queue_length = self.queue.method.message_count

        if not queue_length:
            return self.messages

        msgs_limit = self.batch_size if queue_length > self.batch_size else queue_length

        try:
            for method_frame, properties, body in self.channel.consume(self.queue_name):
                data = {}

                data['properties_type'] = properties.type if hasattr(properties, 'type') else None
                data['message_date'] = properties.timestamp if hasattr(properties, 'timestamp') else None
                if data['message_date']:
                    data['message_date'] = self._validate_date_format(data['message_date'])

                data['message_id'] = properties.message_id if hasattr(properties, 'message_id') else None
                data['delivery_mode'] = properties.delivery_mode if hasattr(properties, 'delivery_mode') else None
                data['headers'] = properties.headers if hasattr(properties, 'headers') else None
                data['content_type'] = properties.content_type if hasattr(properties, 'content_type') else None

                try:
                    data['rabbit_entity_payload'] = json.loads(bytes.decode(body))
                    self.messages.append(data)
                except:
                    LOGGER.info(
                        f"Rabbit Consumer : Received message in wrong format {str(body)}, {data['message_date']}")
                    data['rabbit_entity_payload'] = bytes.decode(body)
                    self.messages_reject.append(data)

                self.delivery_tags.append(method_frame.delivery_tag)

                if method_frame.delivery_tag == msgs_limit:
                    LOGGER.info(f'{msgs_limit} messages have been read')
                    self.channel.cancel()
                    break
        except (ChannelWrongStateError, StreamLostError, AMQPConnectionError) as e:
            LOGGER.info(f'Connection Interrupted: {str(e)}')
