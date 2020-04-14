package mycode.part5twitter

import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}

import scala.concurrent.{Future, Promise}
import scala.io.Source

class TwitterReceiver (language: String = "en") extends Receiver[Status](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[TwitterStream] = Promise[TwitterStream]()
  val socketFuture = socketPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

    override def onStallWarning(warning: StallWarning): Unit = ()

    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  // called asynchronously
  override def onStart(): Unit = {
    val twitterStream = new TwitterStreamFactory("src/main/resources/twitter4j.properties")
      .getInstance()
        .addListener(simpleStatusListener)
        .sample(language) // call the twitter sample endpoint for English tweets

    socketPromise.success(twitterStream)
  }


  // called asynchronously
  override def onStop(): Unit = socketFuture.foreach{ twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }
}
