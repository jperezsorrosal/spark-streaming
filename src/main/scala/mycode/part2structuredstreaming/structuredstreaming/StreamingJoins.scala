package mycode.part2structuredstreaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("StreamingDataFrames app")
    .master("local[4]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val guitarPlayers = spark.read
    .option("inferschema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferschema", true)
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferschema", true)
    .json("src/main/resources/data/bands")


  val guitaristsBands =
    guitarPlayers.join(bands, guitarPlayers("band") === bands("id"), "inner")


  def joinStreamWithStatic() = {

    val streamBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load() // a DF with single column "value" of type String
      .select(from_json($"value", bands.schema) as "band")
      .select($"band.id" as "id", $"band.name" as "name", $"band.hometown" as "hometown", $"band.year" as "year")

    // JOIN HAPPENS PER BATCH
    val streamedBandsGuitraristsDF =
      streamBandsDF
        .join(guitarPlayers, streamBandsDF("id") === guitarPlayers("band"), "left_outer")

    streamedBandsGuitraristsDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def joinStreamWithStream() = {
    val streamBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load() // a DF with single column "value" of type String
      .select(from_json($"value", bands.schema) as "band")
      .select($"band.id" as "id", $"band.name" as "name", $"band.hometown" as "hometown", $"band.year" as "year")

    val player = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12346")
      .load() // a DF with single column "value" of type String
      .select(from_json($"value", guitarPlayers.schema) as "player")
      .select($"player.id" as "id", $"player.name" as "name", $"player.guitars" as "guitars", $"player.band" as "band")


    streamBandsDF
      .join(player, streamBandsDF("id") === player("band"), "inner")
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(
        Trigger.ProcessingTime(10.seconds) // run query every 2 seconds
        //Trigger.Once() // run query one and terminate
        //Trigger.Continuous(2.seconds)
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    joinStreamWithStream()
  }
}
