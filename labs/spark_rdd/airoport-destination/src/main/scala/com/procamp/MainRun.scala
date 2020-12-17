package com.procamp
import org.apache.spark.sql.SparkSession


object MainRun {
  def main(args: Array[String]) {


    // GLC| There is no need to initialize DF session
    // GLC| Don't you think it's better to parameterise the app? spark conf / app conf
    val spark = SparkSession.builder.appName("airoport-destination")
      .getOrCreate()

    val sc = spark.sparkContext
    val mapAcc = sc.collectionAccumulator("My Accumulator")
    val flights = sc.textFile("/bdpc/spark/input/flights.csv")
    val flightsWithoutHeader = flights.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    // GLC| It's better to broadcast a small dataset
    val airports = sc.textFile("/bdpc/spark/input/airports.csv")
    val airportsWithoutHeader = airports.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter}
    // GLC| I's better to split only once .map(_.split(",")).map(x => x(8)...
    val flightsKeyPair =flightsWithoutHeader.map(fs =>(fs.split(",")(8),fs.split(",")(0)+','+fs.split(",")(1)))
    val airportsKeyPair= airportsWithoutHeader.map(ar =>(ar.split(",")(0),ar.split(",")(1)))

    // GLC| Using tuples default field getters is not a good style
    // GLC| It's better to use tuples instead of string fi._2._1+','+fi._2._2 -> (fi._2._1, fi._2._2)
    //
    val joinFlightsInfo = flightsKeyPair.join(airportsKeyPair).map(fi =>(fi._2._1+','+fi._2._2 ,1)).reduceByKey(_ + _)
    // GLC| Could you please explain what is heppaning here?
    // GLC| Is it possible to use reduceByKey instead of groupByKey?
    val flightsCounts =joinFlightsInfo.map(fc =>(fc._1.split(",")(0)+','+fc._1.split(",")(1), (fc._1.split(",")(2),fc._2 ))).groupByKey()

    val flightsMax =flightsCounts.map({case(key, v) =>
      // GLC| maxBy can be used instead of sortBy(tup => tup._2).last
      val max = v.toList.sortBy(tup => tup._2).last
      (key, max)
    })

    // GLC| Use meaningfull names in Pattern Matching
    flightsMax.map({case(a, b) =>
      // GLC| var is not best practice to use in scala. Always prefer val
      // GLC| There is no need to use local reference (val/var)
      // GLC| https://docs.scala-lang.org/overviews/core/string-interpolation.html -> s"${a} ${b}"
      var line = a.split(",")(0) + "\t" + a.split(",")(1) + "\t" + b._1 + "\t" + b._2
      line
    }).saveAsTextFile("/bdpc/spark/output/");

    // GLC| What do you need to change to make the application idempotent?

    // GLC| It's very hard to review the code :(

    spark.stop()
  }

}
