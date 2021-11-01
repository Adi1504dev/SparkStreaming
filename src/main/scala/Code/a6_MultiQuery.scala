package Code

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object a6_MultiQuery extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  /*
   *
   * Start zookeeper and kafka
   * create topic-->kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices
   * create Output topic-->kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic notifications
   * Write to topic through Producer-->kafka-console-producer.bat --broker-list localhost:9092 --topic invoices
   * Send atleast one record and then run program
   *kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic notifications --from-beginning
   */

  def main(args:Array[String]): Unit =
  {
    //If input source is same and we have 2 operations(Write Query)
    //like we created 2 applications above one to write in file sink one to write in kafka sink we can do it in same program.

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Kafka Sink Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

    val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

    val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
      """to_json(named_struct('CustomerCardNo', CustomerCardNo,
        |'TotalAmount', TotalAmount,
        |'EarnedLoyaltyPoints', TotalAmount * 0.2
        |)) as value""".stripMargin)

    val notificationWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Notification Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "notifications")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir/notify")//each query should have unique check point location
      .start()

    val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
      "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
      "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    val invoiceWriterQuery = flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir/flatten")//each query should have unique check point location
      .start()

    //Both of the queries will run in parallel
    //by below command and and application will stop only in one condition if any query fails or program is stopped for maintenance


    logger.info("Waiting for Queries")
    spark.streams.awaitAnyTermination()//If sny onr of the query is terminated the program will stop.
  }

}
