package mycode.part2structuredstreaming.integrations.kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka DStreams app")
    .master("local[4]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  // includes encoders for DF to DS

  import org.apache.kafka.clients.producer.ProducerConfig._
  import org.apache.kafka.clients.consumer.ConsumerConfig._

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {

    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // distribute partitions evenly across the Spark cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)

    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD{ rdd =>
      rdd.foreachPartition{ partition =>
        // inside this lambda, the code is run by a single executor

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach( pair =>
          kafkaHashMap.put(pair._1, pair._2))

        // producer can insert records into the Kafka topics
        // available on this executors
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach{ value =>
            val message = new ProducerRecord[String, String](kafkaTopic, null, value)

          producer.send(message)
        }

        producer.close()
      }

    }

    ssc.start()
    ssc.awaitTermination()

  }


  def main(args: Array[String]): Unit = {

    println("Write to kafka")
    writeToKafka()
  }
}
