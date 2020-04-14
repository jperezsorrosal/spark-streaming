package mycode.part2structuredstreaming.structuredstreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("StreamingAggregations app")
    .master("local[4]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._


  def streamingCount() = {
    // Read a DataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    val lineCount = lines.select(count("*") as "lineCount")


    // AGGREGATIONS WITH DISTINCT ARE NOT SUPPORTED
    // otherwise Spark will have to keep everything, and streams are unbounded

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update do not support aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column) = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    val aggDF: DataFrame = lines
      .select(aggFunction($"value".cast(IntegerType)) as "suma" )

    aggDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }


  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    val names = lines
      .select('value as "name")
      .groupBy('name) // RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //streamingCount()
    //numericalAggregations(sum)
    groupNames()
  }
}
