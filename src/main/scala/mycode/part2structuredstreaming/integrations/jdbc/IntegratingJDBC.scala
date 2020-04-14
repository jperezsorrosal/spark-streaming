package mycode.part2structuredstreaming.integrations.jdbc

import java.util.Properties

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import scala.collection.JavaConverters._

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[4]")
    .getOrCreate()

  // includes encoders for DF to DS
  import common._
  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"

  val connectionProperties = Map(
    "user" -> "docker",
    "password" -> "docker"
  )

  val jdbcProperties = new Properties
  jdbcProperties.putAll(connectionProperties.asJava)


  def writeStreamToPostgres() = {
    val carDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carDS = carDF.as[Car]

    val table = "public.cars"

    carDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        // each executor can control the batch
        // batch is a static dataset

        batch.write.mode(SaveMode.Overwrite)
          .jdbc(url, table, jdbcProperties)
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()

    spark.close()
  }
}
