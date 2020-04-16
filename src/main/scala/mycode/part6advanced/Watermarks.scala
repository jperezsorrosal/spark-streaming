package mycode.part6advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import mycode.part6advanced.ProcessingTimeWindows.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import part6advanced.DataSender.printer

import scala.concurrent.duration._

object Watermarks {

  val spark = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // format: <timestamp in millis>,<color>

  def debugQuery(query: StreamingQuery) = {
    // userful skill for debugging
    new Thread(() => {
      1 to 100 foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermark() = {
    val dataDS = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)
      }
      .toDF("created", "color")


    val watermarkedDF = dataDS
      .withWatermark("created", "2 seconds")
      .groupBy(window('created, "2 seconds"), 'color)
      .count()
      .select($"window.*", 'color, 'count)

    /*
      A 2 second watermark =
      - a window will only be considered until the watermark surpases the window end
      - an element / row / record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermark()
  }
}

object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() //blocking call

  val printer = new PrintStream(socket.getOutputStream)

  println(s"Socket accepted: ${socket.getRemoteSocketAddress}")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")

    Thread.sleep(1000)
    printer.println("8000,green")

    Thread.sleep(4000)
    printer.println("8000,green")

    Thread.sleep(14000)
    printer.println("8000,blue")

    Thread.sleep(1000)
    printer.println("9000,red")

    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000, blue")

    Thread.sleep(1000)
    printer.println("13000,green")

    Thread.sleep(500)
    printer.println("21000,green")

    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped

    Thread.sleep(2000)
    printer.println("17000, green")
  }

  def example2(): Unit = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("5000,blue")

    Thread.sleep(7000)

    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")

  }

  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }

  def main(args: Array[String]): Unit = {
    example3()
  }
}



