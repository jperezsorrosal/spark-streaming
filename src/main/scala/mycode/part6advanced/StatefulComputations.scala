package mycode.part6advanced

import java.sql.Timestamp

import mycode.part6advanced.Watermarks.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulComputations {

  val spark = SparkSession.builder()
    .appName("Stateful computations")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import org.apache.spark.sql.functions._
  import spark.implicits._

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)

  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)

  case class AveragePostStorage(postType: String, averageStorageUsed: Double)

  def readSocialUpdates() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .as[String]
    .filter{ _.trim.split(",").length == 3 }
    .map { line =>
      val tokens = line.trim.split(",")
      SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
    }

  def updateAverageStorage(postType: String,                       // key by which the grouping was made
                           group: Iterator[SocialPostRecord], // batch of data associated to the ley
                           state: GroupState[SocialPostBulk]  // like an option, wrapper around data SocialPostBulk. Manage this manually
                          ): AveragePostStorage = {           // A single value I will output per the entire group

    /*
      - start the state to start with
      - for all the items in the group, need to aggregate data:
        - summing up the total count
        - summing up the total storage
      - update the state with the new aggregated use
      - return a single value with the type AvgPostStorage
     */

    val previousBulk = state.getOption match {
      case Some(bulk) => bulk
      case None =>  SocialPostBulk(postType, 0, 0)
    }

    val totalAggregatedData: (Int, Int) = group.foldLeft((0, 0)){ (currentData, record) =>
      val (currentCount, currentStorage) = currentData

      (currentCount + record.count, currentStorage + record.storageUsed)
    }

    val (totalCount, totalStoraged) = totalAggregatedData

    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totalCount, previousBulk.totalStorageUsed + totalStoraged)

    state.update(newPostBulk)

    // this is issued once ber batch per key
    AveragePostStorage(postType, newPostBulk.totalStorageUsed.toDouble / newPostBulk.count.toDouble)

  }
  def getAveragePostStorage() = {
    val socialStream: Dataset[SocialPostRecord] = readSocialUpdates()

    val regularSqlAvgByPostStorage = socialStream
      .groupByKey(_.postType)
      .agg( sum('count).as("totalCount").as[Int], sum('storageUsed).as("totalStorage").as[Int])
      .select('key.as[String] as "postType", 'totalStorage.divide('totalCount).as[Double] as "avgStorage")

    // an alternative to the regularSqlAvgByPostStorage
    val avgByPostStorage = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage) // GroupStateTimeout is similar to watermark

    avgByPostStorage.writeStream
      .outputMode(OutputMode.Update()) // append not supported with mapGroupsWithState, gives an error with spark3-prev2 and "console"
      // .format("console") // we circunvent using per each batch:
      .foreachBatch{ (batch: Dataset[AveragePostStorage], batchId: Long) =>
        batch.show(false)
      }
      .start()
      .awaitTermination()

  }

  /*
    text,3,3000
    text,4,5000
    video,1,500000
    audio,3,60000
    text,1,2500

   */

  def main(args: Array[String]): Unit = {
    getAveragePostStorage()
  }
}
