package mycode.part5twitter

import java.net.Socket
import java.util.Properties

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
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

  def getAverageTwitterLength(): DStream[Double] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)

    val averages: DStream[Double] = tweets
      .map(status => (status.getText.length, 1))
      .reduceByWindow((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2), Seconds(5), Seconds(5))
      .map { case (tweetLengthSum, tweetCount) => tweetLengthSum * 1.0 / tweetCount }

    averages
  }

  def mostPopularHashTags(): DStream[(String, Int)] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    ssc.checkpoint("checkpoints")

    tweets
      .flatMap( status => status.getText.split(" "))
      .filter(_.startsWith("#"))
      .map( hashtag => (hashtag.toLowerCase, 1))
      .reduceByKeyAndWindow( (x, y) => x + y, (x, y) => x - y, Seconds(60), Seconds(10) )
      .transform(rdd => rdd.sortBy{ case (hashtag, count) => - count})
  }


  def main(args: Array[String]): Unit = {
    //readTwitter()
    mostPopularHashTags().print()
    ssc.start()
    ssc.awaitTermination()

  }
}
