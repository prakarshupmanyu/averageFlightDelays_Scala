# averageFlightDelays_Scala
Map-reduce jobs to compute the average arrival delays of individual flight routes and then finding out the top k routes with maximum average delays using top k design pattern.

The Java on Hadoop version of this project can be found [here](https://github.com/prakarshupmanyu/averageFlightDelays)

The data for the tasks can be found at [this link](https://drive.google.com/file/d/1U2PjP5m8G5FP-G3eqRF3fqsjdQzkirvC/edit)

"Route" is defined as ordered pair of source and destination cities. For example, BOS-NYC (Boston to New York) id a route and NYC-BOS (New York to Boston) is another route. The input files are in CSV format. The only columns required for these tasks are **Origin, Destination** and **ArrDelay**. Missing is marked as "NA". I have ignored those records for the purpose of this project.

The two tasks computed in this project are:
1. For each route, calculate the average number of minutes that a flight is delayed.
2. Find top k routes with largest average  arrival delay (k can be provided via command line).

I managed this project using maven. Following command is used to run the map-reduce job:

bin/spark-submit --master local \
  --class flightDelaysScala.AverageFlightDelays \
  ~/mySparkProject/codingChallengeScala/target/codingChallenge-1.0-SNAPSHOT.jar \
  <path_to_CSV_files> \
  <path_to_store_average_flight_delays> \
  <path_to_store_top_k_routes> \
  <value_of_k>
