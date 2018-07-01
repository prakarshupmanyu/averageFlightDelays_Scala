package flightDelaysScala


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/*
* This class contains the functions compute the average flight delays and finding out the top k routes
* with largest average flight delays
*
* This job takes 4 arguments:
* 1 - Input directory of the the CSV files that contain the flight data
* 2 - Output directory to store the average flight delays per route
* 3 - Output directory to store the top k route with largest flight delays
* 4 - value to k
*
* Sample command to run the job
*
* bin/spark-submit --master local --class flightDelaysScala.AverageFlightDelays \
* ~/codingChallengeScala/target/codingChallenge-1.0-SNAPSHOT.jar \
* /home/prakarsh/flight_data/input/input/ \
* /home/prakarsh/outputs/output_task1_scala/ \
* /home/prakarsh/outputs/output_task2_scala/ \
* 100
*
* */

class AverageFlightDelayCalculator(sc: SparkContext, spark: SparkSession){
  /*
  * This function computes the average flight delays per route where route is defined as origin-destination.
  * This leverages Spark DataFrames as they make it easy to work with CSV data
  *
  * Input arguments:
  * 1 - inputDir - path to the CSV directory that contains the flight information
  * 2 - outputDir - path to the directory where to store the computed average flight delays
  * */
  def computeAverageFlightDelays(inputDir: String, outputDir: String) : Unit = {
    import spark.implicits._
    //read the data
    val flightData = spark.read.format("csv")
      .option("header", "true")
      .load(inputDir + "/*.csv")

    //process the input data - compute the route and filter the missing values
    val routeDelayData = flightData.select($"Origin", $"Dest", $"ArrDelay")
      .filter($"Origin" =!= "NA")
      .filter($"Dest" =!= "NA")
      .filter($"ArrDelay" =!= "NA")
      .select(concat_ws("-", $"Origin", $"Dest").alias("Route"), $"ArrDelay")

    //some more processing
    val cleanedRouteDelays = routeDelayData.withColumn("ArrDelay", routeDelayData("ArrDelay").cast("float"))
      .filter($"ArrDelay".isNotNull)

    //compute the average flight delays for each route
    val averageFlightDelays = cleanedRouteDelays.groupBy($"Route")
      .avg("ArrDelay")
      .withColumnRenamed("avg(ArrDelay)", "AvgArrDelay")

    //save the output
    averageFlightDelays.write.format("csv").option("header", "true").save(outputDir)
  }

  /*
  * This function computes the top k flight routes that have  the largest average flight delays.
  * This function again leverages the spark DataFrames.
  *
  * Input arguments:
  * 1 - inputDir - path to directory that contains the average flight delays of each route.
  * 2 - outputDir - path to directory where to store the top k flight routes with largest average flight delays
  * 3 - value of k
  * */
  def computeTopKDelayedRoutes(inputDir: String, outputDir: String, k: Int): Unit = {
    import spark.implicits._

    //read the average flight delays
    val averageFlightDelayData = spark.read.format("csv")
      .option("header", "true")
      .load(inputDir + "/*.csv")

    //sort the data in each partition according to average flight delays and then pick the top k from each partition
    val sortedFlightDelays = averageFlightDelayData
      .withColumn("AvgArrDelay", averageFlightDelayData("AvgArrDelay").cast("double"))
      .withColumn("partition", ceil(rand*100)%7)
      .withColumn("rn", row_number.over(Window.partitionBy("partition").orderBy($"AvgArrDelay".desc)))
      .filter($"rn" <= k)
      .drop($"partition")
      .drop($"rn")

    //find the top k amongst all the partitions and save the output
    sortedFlightDelays.orderBy($"AvgArrDelay".desc)
      .limit(k)
      .write.format("csv")
      .option("header", "true")
      .save(outputDir)
  }

}

object AverageFlightDelays {
  def main(args: Array[String]) {

    if (args.length != 4){
      System.err.println("This job requires 4 arguments. Exiting...")
      System.exit(1)
    }
    val csvDir = args(0)      //"/home/prakarsh/fight_data/input/input/"
    val outputDir1 = args(1)  //"/home/prakarsh/outputs/output_task1_scala/"
    val outputDir2 = args(2)  //"/home/prakarsh/outputs/output_task2_scala/"
    val k = args(3).toInt     //100
    val conf = new SparkConf().setAppName("FlightDelayCalculator").setMaster("local")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val flightDelayCalculatorObj = new AverageFlightDelayCalculator(context, spark)
    flightDelayCalculatorObj.computeAverageFlightDelays(csvDir, outputDir1)
    flightDelayCalculatorObj.computeTopKDelayedRoutes(outputDir1, outputDir2, k)
  }

}
