package mycode.part5twitter

import java.io.{OutputStream, PrintStream}
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

  /*
    we want to ignore the output of the NLP library
    System error redirects the output to this PrintStream that does nothing
    System error will call this methods write that will do nothing
   */
  private def redirectSystemError() = System.setErr(new PrintStream(new OutputStream {
    override def write(b: Int): Unit = () // ignore
    override def write(b: Array[Byte]): Unit = () // ignore
    override def write(b: Array[Byte], off: Int, len: Int): Unit = () // ignore
  }))


  // called asynchronously
  override def onStart(): Unit = {

      redirectSystemError()

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
