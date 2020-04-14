package mycode.part2structuredstreaming.integrations.cassandra

import java.util.Properties

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import part4integrations.IntegratingCassandra.CarCassandraForeachWriter

object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .getOrCreate()

  // includes encoders for DF to DS
  import common._
  import spark.implicits._


  def writeStreamToCassandraInBatches() = {
    val carDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    val table = "cars"

    carDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        // batch is a static dataset
        // each executor can control the batch
        // batch is a static dataset
        // save this batch in cassandra in a single table write
        batch
          .select('Name, 'Horsepower)
          .write
          .cassandraFormat(table, "public")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  class CarCassandraForeachWriter extends ForeachWriter[Car] {

    /*
      - On every batch, on every partitionId
        - on every epoch = chunk of data
          - call the open method; if false skip this chunk
          - for each entry in this chunk, call the process method
          - call the end method either at the end of the chunk or
            with and error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open Connection")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo{ session =>
        session.execute{
          s"""
            |insert into $keyspace.$table("Name", "Horsepower")
            |values('${car.Name}', ${car.Horsepower.orNull})
            |""".stripMargin
        }
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing Connection")
  }

  def writeStreamToCassandra() = {
    val carDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    val table = "cars"

    carDS
      .writeStream
      .foreach { new CarCassandraForeachWriter }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToCassandra()
  }
}
