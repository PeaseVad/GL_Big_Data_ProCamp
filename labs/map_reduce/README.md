Instruction to run

1)gsutil cp gs://globallogic-procamp-bigdata-datasets/Flight_Delays_and_Cancellations/airlines.csv .
2)gsutil cp gs://globallogic-procamp-bigdata-datasets/Flight_Delays_and_Cancellations/flights.csv .

2)hdfs dfs -mkdir -p  /bdpc/hadoop_mr/input
3)hdfs dfs -mkdir -p  /bdpc/hadoop_mr/dict/

4)hdfs dfs -copyFromLocal ./flights.csv /bdpc/hadoop_mr/input/

5)hdfs dfs -copyFromLocal ./airlines.csv  /bdpc/hadoop_mr/dict/

6)yarn jar top_airlines-1.0-jar-with-dependencies.jar com.globallogic.bdpc.mapreduce.airlines.TopAirlines /bdpc/hadoop_mr/input/ /bdpc/hadoop_mr/stg /bdpc/hadoop_mr/output


