package mycode.part2structuredstreaming.lowlevelstreams

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsWindowTransformations  {

  val spark = SparkSession.builder()
    .appName("DStreams Window Transformations")
    .master("local[2]")
    .getOrCreate()

  // WE NEED a STREAMING context
  // is entry poing to DStreams API
  // -- nees spark context
  // -- needs duration: batch interval

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  ssc.checkpoint("checkpoint")

  import common._
  import spark.implicits._

  def readLines(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  def linesByWindow() = readLines().window(Seconds(10), Seconds(5))

  def countLinesByWindow() = readLines().countByWindow(Seconds(10), Seconds(5))

  def sumAllTextByWindow() = readLines()
    .map(_.length)
    .window(Seconds(10), Seconds(5))
    .reduce(_ + _)

  def sumAllTextByWindowAlt() = readLines()
    .map(_.length)
    .reduceByWindow(_+_, Seconds(10), Seconds(5))

  def linesByTumblingWindow() = readLines().window(Seconds(5), Seconds(5))

  def computeWordOccurrencesByWindow() = {

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _, // inverse function to delete values out of the windows when the fall off
        Seconds(60),
        Seconds(30)
      )
  }

  // words larger thatn 10 gives 2$
  def game() = {
    readLines()
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .map(_ => 2)
      .reduceByWindow(_+_, Seconds(30), Seconds(10))
  }

  def main(args: Array[String]): Unit = {

    game.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
