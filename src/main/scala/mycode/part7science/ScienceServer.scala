package mycode.part7science

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.concurrent.Future
import scala.io.Source

object ScienceServer {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val kafkaTopic = "science"
  val kafkaBootstrapServer = "localhost:9092"

  def getProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[Long, String](props)
  }

  def getRoute(producer: KafkaProducer[Long, String]) = {
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
          )
        )
      }
    } ~
    path("api" /  "report") {
      (parameter("sessionId".as[String]) & parameter("time".as[Long])) { (sessionId: String, time: Long) =>
        println(s"Session {${sessionId}}, time = ${time}")

        val record = new ProducerRecord[Long, String](kafkaTopic, 0, s"$sessionId,$time")
        producer.send(record)
        producer.flush()

        complete(StatusCodes.OK)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    import scala.concurrent.ExecutionContext.Implicits.global

    val kafkaProducer = getProducer()

    val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(getRoute(kafkaProducer), "localhost", 9988)

    //cleanup
    bindingFuture.foreach{ binding =>
      binding.whenTerminated.onComplete(_ => kafkaProducer.close())
    }
  }
}
