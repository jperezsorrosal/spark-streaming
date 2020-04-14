package mycode.part2structuredstreaming.structuredstreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("StreamingDatasets app")
    .master("local[4]")
    .getOrCreate()

  // includes encoders for DF to DS
  import common._
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def readCars() = {

    // alternatively to the implicits encoders imported to transform DF to DS
    //val carEncoder = Encoders.product[Car]

    val stringRowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"

    val carDS: Dataset[Car] = stringRowDF
      .select( from_json('value,  carsSchema) as  "car") // composite column
      .select("car.*") // DF with multiple columns
      .as[Car] //(carEncoder) // I pass explicitly the car encoder too

    carDS
  }

  def showCarNames(): Unit = {
    val carsDS = readCars()

    val carNamesDF: DataFrame = carsDS.select('Name)

    // collection transformations maintain TYPE info
    val carNamesDS: Dataset[String] = carsDS.map(_.Name)

    carNamesDS.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }


  def powerfulCarrs(): Unit = {
    val carsDS = readCars()

    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }

  def avgHorsepowerEntireDataset() = {
    val carsDS = readCars()

    carsDS
      .select(avg('Horsepower))
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }


  def countCarsByOrigin() = {
    val carsDS = readCars()

    val carCount: DataFrame = carsDS
      .groupBy('Origin)
      .count()

    val carCountDS: Dataset[(String, Long)] = carsDS
        .groupByKey(_.Origin)
        .count()

    carCountDS
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //showCarNames()
    //powerfulCarrs()
    //avgHorsepowerEntireDataset()
    countCarsByOrigin()
  }
}
