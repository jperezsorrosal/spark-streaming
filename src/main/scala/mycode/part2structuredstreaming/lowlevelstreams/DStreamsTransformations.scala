package mycode.part2structuredstreaming.lowlevelstreams

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsTransformations  {

  val spark = SparkSession.builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  // WE NEED a STREAMING context
  // is entry poing to DStreams API
  // -- nees spark context
  // -- needs duration: batch interval

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import common._
  import spark.implicits._

  def readPeople() = {
    ssc.socketTextStream("localhost", 12345).map{ line =>
      val tokens = line.split(":")

      Person(
        tokens(0).toInt,
        tokens(1),
        tokens(2),
        tokens(3),
        tokens(4),
        Date.valueOf(tokens(5)),
        tokens(6),
        tokens(7).toInt
      )
    }
  }

  def peoplesAges() = readPeople().map { person =>
      val age = Period.between(person.birthDate.toLocalDate, LocalDate.now).getYears

    (s"${person.firstName} ${person.lastName}", age)
  }


  def peopleSmallNames() = readPeople().flatMap{ person =>
    List(person.firstName, person.middleName)
  }
  /*
    - define input sources by creating a DStream
    - define transformations
    - start ALL computation with ssc.start()
    - await termination or stop
      - you cannot restart a computation
      -- you cannot restart ssc.start() again
   */

  def highIncomePeople() = readPeople().filter(_.salary > 100000)

  def countPeople() = readPeople().count() // count the number of people in every batch

  def countNames() = readPeople().map(_.firstName).countByValue()

  // this is whtat the implementation of countByValue does:
  def countNamesReduce() = readPeople()
    .map(_.firstName)
    .map(name => (name, 1L))
    .reduceByKey( _ + _)


  def saveToJson() = readPeople().foreachRDD{ rdd =>
    val ds: Dataset[Person] = spark.createDataset(rdd)
    val f = new File("src/main/resources/data/people")
    val nfiles = f.listFiles().length

    val path = s"src/main/resources/data/people/people-$nfiles.json"

    ds.write.json(path)
  }
  def main(args: Array[String]): Unit = {
//    val stream = countNames()
//    stream.print()

    saveToJson()

    ssc.start()
    ssc.awaitTermination()
  }
}
