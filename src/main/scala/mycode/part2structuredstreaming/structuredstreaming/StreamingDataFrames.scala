package mycode.part2structuredstreaming.structuredstreaming

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("StreamingDataFrames app")
    .master("local[4]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  def readFromSocket() = {

    // Read a DataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    val shortLines = lines.filter(length(trim('value)) < 10)

    // Consume the DataFrame
    val query: StreamingQuery = shortLines.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }


  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .option("encoding", "UTF-8")
      .load()

    lines.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(2.seconds) // run query every 2 seconds
        //Trigger.Once() // run query one and terminate
        //Trigger.Continuous(2.seconds)
      )
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    //readFromSocket()
    demoTriggers()
  }

}
