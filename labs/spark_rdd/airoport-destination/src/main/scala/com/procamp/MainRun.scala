package com.procamp
import org.apache.spark.sql.SparkSession


object MainRun {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("airoport-destination")
    .getOrCreate()

    val sc = spark.sparkContext
    val mapAcc = sc.collectionAccumulator("My Accumulator")
    val flights = sc.textFile("/bdpc/spark/input/flights.csv")
    val flightsWithoutHeader = flights.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    val airports = sc.textFile("/bdpc/spark/input/airports.csv")
    val airportsWithoutHeader = airports.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    val flightsKeyPair =flightsWithoutHeader.map(fs =>(fs.split(",")(8),fs.split(",")(0)+','+fs.split(",")(1)))
    val airportsKeyPair= airportsWithoutHeader.map(ar =>(ar.split(",")(0),ar.split(",")(1)))

    val joinFlightsInfo = flightsKeyPair.join(airportsKeyPair).map(fi =>(fi._2._1+','+fi._2._2 ,1)).reduceByKey(_ + _)
    val flightsCounts =joinFlightsInfo.map(fc =>(fc._1.split(",")(0)+','+fc._1.split(",")(1), (fc._1.split(",")(2),fc._2 ))).groupByKey()

    val flightsMax =flightsCounts.map({case(key, v) =>
      val max = v.toList.sortBy(tup => tup._2).last
      (key, max)
    })

    flightsMax.map({case(a, b) =>
      var line = a.split(",")(0) + "\t" + a.split(",")(1) + "\t" + b._1 + "\t" + b._2
      line
    }).saveAsTextFile("/bdpc/spark/output/");

    spark.stop()
  }

}
