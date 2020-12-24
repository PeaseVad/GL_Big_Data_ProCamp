package com.procamp

import org.apache.spark.sql.{SaveMode, SparkSession}


object MainRun {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("airoport-destination-df")
      .getOrCreate()

    // Read csv file to DataFrame with parsing schema in csv file
    val flights = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("gs://globallogic-procamp-bigdata-datasets/Flight_Delays_and_Cancellations/flights.csv");

    // Read csv file to DataFrame with parsing schema in csv file
    val airports = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("gs://globallogic-procamp-bigdata-datasets/Flight_Delays_and_Cancellations/airports.csv");

    // Create temp view to add ability work with DataFrame in Spark SQl
    flights.createOrReplaceTempView("flights")
    airports.createOrReplaceTempView("airports")

    val flightsJoin = spark.sql("select /*+  BROADCASTJOIN(small) */ flights.YEAR,flights.MONTH,airports.AIRPORT, count(*) as flights_count  from flights join airports on flights.DESTINATION_AIRPORT =airports.IATA_CODE group by flights.YEAR,flights.MONTH,airports.AIRPORT")
    flightsJoin.createOrReplaceTempView("flightsJoin")

    val flightsResRank = spark.sql("select year,month,airport,flights_count,ROW_NUMBER() OVER ( partition BY YEAR,MONTH ORDER BY flights_count desc) AS high_flights_count  from flightsJoin")

    flightsResRank.select("year", "month", "airport", "flights_count").where(flightsResRank("high_flights_count") === 1).coalesce(1).write.option("delimiter", "\t").mode("overwrite").csv("gs://globallogic-procamp-bigdata-datasets/Flight_Delays_and_Cancellations/spark/output/")

    spark.stop()
  }

}
