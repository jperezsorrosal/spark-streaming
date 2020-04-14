package mycode.part2structuredstreaming.integrations.akka

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import common.Car
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import part4integrations.ReceiverSystem.EntryPoint

import scala.collection.JavaConverters._

object IntegratingAkkaWithAkkaStreams {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[4]")
    .getOrCreate()

  // includes encoders for DF to DS

  import common._
  import spark.implicits._

  // foreach batch
  // the receiver is in a different JVM


  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"

  val connectionProperties = Map(
    "user" -> "docker",
    "password" -> "docker"
  )

  val jdbcProperties = new Properties
  jdbcProperties.putAll(connectionProperties.asJava)


  def writeCarsToAkka() = {
    val carDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    val table = "public.cars"

    carDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        // each executor can control the batch
        // batch is a static dataset

        batch.foreachPartition{ cars: Iterator[Car] =>
          // this code is run as a single executor

          val system = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          cars.foreach{ car => entryPoint ! car }
        }
        batch.write.mode(SaveMode.Overwrite)
          .jdbc(url, table, jdbcProperties)
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToAkka()

    spark.close()
  }
}

object ReceiverSystemWithAkkaStreams {

  implicit val actorSystem = ActorSystem(
    "ReceiverSystem",
    ConfigFactory
      .load("akkaconfig/remoteActors")
      .getConfig("remoteSystem")
  )
  implicit val actorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive: Receive = {
      case m => log.info(m.toString)
    }
  }

  class ProxyEntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {
      case m =>
        log.info(s"Received: ${m}")
        destination ! m
    }
  }

  object ProxyEntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  def main(args: Array[String]): Unit = {

    val source = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Car](car => println(s"Sink: $car"))

    val runnableGraph = source.to(sink)

    val destination : ActorRef = runnableGraph.run()

    //val destination = actorSystem.actorOf(Props[Destination], "destination")
    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")
  }
}
