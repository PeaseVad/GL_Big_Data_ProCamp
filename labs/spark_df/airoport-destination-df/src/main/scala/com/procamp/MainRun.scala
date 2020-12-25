package com.procamp

import org.apache.spark.sql.{SaveMode, SparkSession}


object MainRun {
  def main(args: Array[String]) {

    // GLC| Could you please make the code more readable and add comments
    val spark = SparkSession.builder.appName("airoport-destination-df")
    .getOrCreate()

    val flights = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("/bdpc/spark/input/flights.csv");

    val airports = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("/bdpc/spark/input/airports.csv");
    flights.createOrReplaceTempView("flights")
    airports.createOrReplaceTempView("airports")
    val flightsJoin =spark.sql("select flights.YEAR,flights.MONTH,airports.AIRPORT, count(*) as flights_count  from flights join airports on flights.DESTINATION_AIRPORT =airports.IATA_CODE group by flights.YEAR,flights.MONTH,airports.AIRPORT")
    flightsJoin.createOrReplaceTempView("flightsJoin")

    val flightsResRank=spark.sql("select year,month,airport,flights_count,ROW_NUMBER() OVER ( partition BY YEAR,MONTH ORDER BY flights_count desc) AS high_flights_count  from flightsJoin")

     flightsResRank.select("year", "month", "airport","flights_count").where(flightsResRank("high_flights_count") === 1).coalesce(1).write.option("delimiter", "\t").mode("overwrite").csv("/bdpc/spark/output/")

    spark.stop()
  }

}
