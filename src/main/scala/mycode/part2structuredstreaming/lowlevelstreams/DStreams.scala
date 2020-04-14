package mycode.part2structuredstreaming.lowlevelstreams

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams  {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[4]")
    .getOrCreate()

  // WE NEED a STREAMING context
  // is entry poing to DStreams API
  // -- nees spark context
  // -- needs duration: batch interval

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import common._
  import org.apache.spark.sql.functions._
  import spark.implicits._


  /*
    - define input sources by creating a DStream
    - define transformations
    - start ALL computation with ssc.start()
    - await termination or stop
      - you cannot restart a computation
      -- you cannot restart ssc.start() again
   */

  def readFromSocket() = {
    val socketStream : DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation
    val wordsStream : DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    //wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words")
    // each folder = RDD == batch, each file = partition of the RDD

    ssc.start()
    ssc.awaitTermination()
  }


  def createNewFile() = {
    new Thread( () => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks-$nFiles.csv")

      newFile.createNewFile()

      val writer = new FileWriter(newFile)

      writer.write(
        """
          |AAPL,Apr 8 2020, 12.88
          |""".stripMargin.trim
      )

      writer.close()
    }).start()
  }

  def readFromFile () = {


    createNewFile() // this is asyncronous operates in different thread, needed by ssc.textFileStream

    val stocksFilePath = "src/main/resources/data/stocks"


    // ssc.textFileStream, only monitores new files
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)


    val dateFormat = new SimpleDateFormat("MMM DD YYYY")

    val stocksStream: DStream[Stock] = textStream
      .map { line =>
        val tokens = line.split(",")
        val company = tokens(0)
        val date = new Date(dateFormat.parse(tokens(1)).getTime())
        val price = tokens(2).toDouble

        Stock(company, date, price)
      }

    stocksStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
    //readFromFile()
  }
}
