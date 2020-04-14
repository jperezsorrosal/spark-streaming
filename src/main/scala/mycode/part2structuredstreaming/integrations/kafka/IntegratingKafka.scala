package mycode.part2structuredstreaming.integrations.kafka

import akka.remote.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka app")
    .master("local[4]")
    .getOrCreate()

  // includes encoders for DF to DS
  import common._
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def readFromKafka() = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select('topic, 'value.cast(StringType) as "actualValue")
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }


  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF
      .select(upper('Name) as "key", 'name as "value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()
  }

  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF
      .select(upper('Name) as "key",
        to_json(struct(
          'Name,
          'Miles_per_Gallon,
          'Cylinders,
          'Displacement,
          'Horsepower,
          'Weight_in_lbs,
          'Acceleration,
          'Year,
          'Origin)
        ) as "value"
      )

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //readFromKafka()
    writeCarsToKafka()
  }
}
