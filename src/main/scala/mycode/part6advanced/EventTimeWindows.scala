package mycode.part6advanced

import org.apache.spark.sql.catalyst.expressions.{Days, Hours}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import part2structuredstreaming.StreamingDatasets.spark
import twitter4j.Status


object EventTimeWindows {

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

  def aggregatePurchaesBySlidingWindow() = {
    val purchasesDS = readPurchasesFromSocket()

    val windowByDay = purchasesDS
      .groupBy(window('time, "1 day", "1 hour") as "time") // time is now a composite column fields {start, end}
      .agg(sum("quantity") as "totalQuantity")
        .selectExpr("time.*", "totalQuantity")

    windowByDay
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def aggregatePurchaesByTumblingWindow() = {
    val purchasesDS = readPurchasesFromSocket()

    val windowByDay = purchasesDS
      .groupBy(window('time, "1 day") as "time") // tumbling window : sliding duration = window duration (windows do not overlap)
      .agg(sum("quantity") as "totalQuantity")
      .selectExpr("time.*", "totalQuantity")

    windowByDay
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  /*
    Exercises
    - 1 show the best selling product of every day, +quantity sold
    - 2 show the best selling product of every 24 hours, updated every hour
   */

  def bestSellingProductPerDay() = {
    //val purchasesDS = readPurchasesFromSocket()
    val purchasesDS = readPurchasesFromFile()

    val bestSelling = purchasesDS
      .groupBy('item, window('time, "1 day") as "time") // tumbling window : sliding duration = window duration (windows do not overlap)
      .agg(sum("quantity") as "totalQuantity")
      .selectExpr("time.*", "item", "totalQuantity")
      .orderBy('start, 'totalQuantity.desc)

    bestSelling
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

    def bestSellingProductEvery24h() = {
      val purchasesDS = readPurchasesFromFile()

      val windowByDay = purchasesDS
        .groupBy($"item", window('time, "1 day", "1 hour") as "time") // time is now a composite column fields {start, end}
        .agg(sum("quantity") as "totalQuantity")
        .selectExpr("time.*", "item", "totalQuantity")
        .orderBy('start, 'totalQuantity.desc)

      windowByDay
        .writeStream
        .format("console")
        .outputMode(OutputMode.Complete())
        .start()
        .awaitTermination()
  }

  /*
    For window functions, windows start at Jan 1 1970, 12am GMT
   */

  def main(args: Array[String]): Unit = {
    //aggregatePurchaesBySlidingWindow()
    //aggregatePurchaesByTumblingWindow()
    //bestSellingProductPerDay()
    bestSellingProductEvery24h()
  }
}
