from pyspark.sql.functions import expr
from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .master("yarn") \
        .getOrCreate()

    logger = Log4j(spark)
## Read
## Reading json data from folder
    raw_df = spark.readStream \
            .format("json") \
            .option("path", "/home/enes/Applications/input") \
            .option("maxFilesPerTrigger", "1") \
            .option("cleanSource", "delete") \
            .load()
# Process only 1 json file every batch
    #raw_df.printSchema()

## Transform
## Transform data
    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                      "CustomerType", "PaymentMethod", "DeliveryType",
                      "DeliveryAddress.City", "DeliveryAddress.State",
                      "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    #explode_df.printSchema()

    flattened_df = explode_df \
                    .withColumn("ItemCode", expr("LineItem.ItemCode")) \
                    .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
                    .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
                    .withColumn("ItemQty", expr("LineItem.ItemQty")) \
                    .withColumn("TotalValue", expr("LineItem.TotalValue")) \
                    .drop("LineItem")

## Sink
## Move data to another folder
    invoice_writer_query = flattened_df.writeStream.format("json") \
            .option("path", "/home/enes/Applications/output") \
            .option("checkpointLocation", "Filestream/chk-point-dir") \
            .outputMode("append") \
            .queryName("Flattened Invoice Writer") \
            .trigger(processingTime="1 minute") \
            .start()
            
    logger.info("Flattened Invoice Writer started")
    invoice_writer_query.awaitTermination()
