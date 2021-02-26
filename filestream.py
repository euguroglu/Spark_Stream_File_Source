from pyspark.sql.functions import expr
from pyspark.sql import *


if __name__ == "__main__":
    spark = SparkSession.builder.appName("File Streaming").config("spark.streaming.stopGracefullyOnShutdown", "true").config("spark.sql.streaming.schemaInference", "true").getOrCreate()


## Read
## Reading json data from folder
    raw_df = spark.readStream \
            .format("json") \
            .option("path", "input") \
            .option("maxFilesPerTrigger", "1") \ # Process only 1 json file every batch
            .option("cleanSource", "delete") \
            .load()

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
            .option("path", "output") \
            .option("checkpointLocation", "chk-point-dir") \
            .outputMode("append") \
            .queryName("Flattened Invoice Writer") \
            .trigger(processingTime="1 minute") \ # trigger processing every 1 minute
            .start()

    invoice_writer_query.awaitTermination()
