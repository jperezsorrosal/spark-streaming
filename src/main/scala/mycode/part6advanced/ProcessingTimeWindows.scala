package mycode.part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import org.apache.spark.sql.functions._
  import spark.implicits._

  case class Purchase(id: String, time: java.sql.Timestamp, item: String, quantity: Int)
  /*
  {"id":"bba25784-487c-408b-9bba-d11ffec32010","time":"2019-02-28T17:02:41.675+02:00","item":"Watch","quantity":1}
   */
  val onlinePurchasesSchema = StructType(
    StructField("id", StringType) ::
    StructField("time", TimestampType) ::
    StructField("item", StringType) ::
    StructField("quantity", IntegerType) ::
    Nil
  )

  def readPurchasesFromSocket() = {

    val purchasesDS = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select( from_json('value, onlinePurchasesSchema) as "purchase")
      .selectExpr("purchase.*")
      .as[Purchase]

    purchasesDS
  }

  def readPurchasesFromFile() = spark.readStream
      .schema(onlinePurchasesSchema)
      .json("src/main/resources/data/purchases")
      .as[Purchase]

  def aggregateByProcessingTime() = {

    val linesCharacterCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select('value, current_timestamp() as "processingTime")
      .groupBy(window('processingTime, "10 seconds") as "window")
      .agg(sum(length('value)) as "charCount") // counting characters every 10 seconds by processing time
      .select($"window.*", 'charCount)


    linesCharacterCountByWindowDF
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }
}
