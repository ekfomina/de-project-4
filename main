import argparse
import importlib

from pyspark.sql import SparkSession

from rabbit_wrapper import RabbitConsumer
from spark_wrapper import SparkWrapper



def main() -> None:
    task="rbmq"
    
    spark = (
            SparkSession.builder.appName(task)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("hive.exec.dynamic.partition","true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate()
        )
        
      spark_wrapper = SparkWrapper(spark)
       
      rmq = RabbitConsumer( 
              spark_wrapper=spark_wrapper,
              queue_parser=RabbitMqRenewalOfferAcceptedEvent,
              queue_name= 'ESB.Messages.Events.ECommerceSubscriptions.RenewalOffers',    
              rabbitmq_servers='10.16.26.111',
              rabbitmq_port=5672,
              rabbit_user='guest',
              rabbit_password='guest',
              exchange='ESB.Messages.Events.ECommerceSubscriptions',      
              rouring_key='Renewal_Offers',
              batch_size=6000)
      
      rmq.connect() 
      rmq.open_channel() 
      rmq.setup_queue()
      rmq.on_batch_messages()
      
      # # # ## Сохранить на HDFS 
      rmq.insert_into_hdfs(table='dl_nexway.renewal_offers', messages=rmq.messages)
      rmq.insert_into_hdfs(table='dl_nexway.renewal_offers_reject', messages=rmq.messages_reject)
      
      rmq.ack_batch_message()
      rmq.channel_close()
      rmq.connection_close()

if __name__ == "__main__":
    main()
