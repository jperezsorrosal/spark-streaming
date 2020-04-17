package mycode.part7science

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ScienceSparkAggregator {

  val spark = SparkSession.builder()
    .appName("EventTimeWindows")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import spark.implicits._
  import org.apache.spark.sql.functions._

  case class UserResponse(sessionId: String, clickDuration: Long)

  case class UserAverageResponse(sessionId: String, averageDuration: Double)

  def readUserResponses(): Dataset[UserResponse] =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "science") // science topic
      .load() // load with columns specific to Kafka
      .select('value)
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val sessionId = tokens(0)
        val time = tokens(1).toLong

        UserResponse(sessionId, time)
      }

  def logUserResponses() = {
    readUserResponses()
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }


  // aggregate the ROLLING average response time over the past 10 clicks
  // its like a window function but depending on the last 10 clicks instead
  // very difficult to do in SparkSQL, we will use the API of MapGroupsWithState (StatefulCoputations)
  // 1 2 3 4 5  6  7  8  9  10
  //     6 9 12 15 18 21 24 27  // window of 3 elements: sum

  /*
    updateUserResponseTime("abc-uuid", [100, 200, 300, 400, 500, 600], Empty state) => Iterator(200, 300, 400, 500)

    window of 3 elements
    steps:
    100 -> state becomes [100]
    200 -> state becomes [100, 200]
    300 -> state becomes [100, 200, 300] -> average 200
    400 -> state becomes [200, 300, 400] -> average 300
    500 -> state becomes [300 ,400 ,500] -> average 400
    600 -> state becomes [400, 500, 600] -> average 500

    Iterator will contain 200, 300, 400, 500
   */
  def updateUserResponseTime(n: Int)
                            (sessionId: String, group: Iterator[UserResponse], state: GroupState[List[UserResponse]])
  : Iterator[UserAverageResponse] = {

    group.flatMap{ record =>

      val lastWindow: List[UserResponse] = state.getOption match {
        case Some(state) => state
        case _ => List.empty
      }

      val windowLength = lastWindow.length

      val newWindow =
        if (windowLength >= n) lastWindow.tail :+ record // pop one element, add new one
        else lastWindow :+ record

      // for spark to give us access to the state in the next batch
      state.update( newWindow)

      if (newWindow.length >= n) {
        val newAverage = newWindow.map(_.clickDuration).sum.toDouble / n
        Iterator(UserAverageResponse(sessionId, newAverage))
      } else {
        Iterator()
      }

    }
  }

  def getAverageResponseTime(n: Int): Unit = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //logUserResponses()
    getAverageResponseTime(3)
  }
}
