package mycode.part5twitter

import java.net.Socket
import java.util.Properties

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

import scala.concurrent.{Future, Promise}
import scala.io.Source



object TwitterProject {

  val spark = SparkSession.builder()
    .appName("Twitter Project")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS
  import common._
  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readTwitter() = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followersCount = status.getUser.getFollowersCount
      val text = status.getText

      s"User: $username [$followersCount followers] says: ${text}"
    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readTwitter()

  }
}
