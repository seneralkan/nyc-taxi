# Project Description

This project mainly based on Apache Spark Streaming, Kafka, Hadoop using New York Taxi dataset.

Data Source:
`https://www.kaggle.com/c/nyc-taxi-trip-duration/data`

Data Generator:

Data Generator provides the dataset as a streaming like real world scenarios. When you use the necessary CLI commands, it will produce dataset as streaming data source and it has capabilities to send to Apache Kafta topic or it will save the related data as log file in your directory.

There are 2 python scripts: one for stream data to file (`dataframe_to_log.py`) and the other to Kafka (`dataframe_to_kafka.py`).
You must use ** Python3 **. It is recommended to use virtual environment.

You can find installation guide in the data-generator directory.

`git clone https://github.com/erkansirin78/data-generator.git`

`cd data-generator`

`python dataframe_to_kafka.py -h`

`python dataframe_to_kafka.py -i ~/datasets/nyc_taxi_subset.csv -t test1`

`python dataframe_to_log -h `

Mainly, there are 2 sections as below;

##  1-) Data Engineering Section

The main purpose of this section was maintaned ETL process for the streaming NYC Taxi dataset and create live dashboard using Kibana.

![Streaming Pipeline](https://github.com/seneralkan/nyc-taxi/blob/master/Streaming%20Pipeline.png)

![Live Dashboard 1](https://github.com/seneralkan/nyc-taxi/blob/master/img/Live%20Dashboard-1.png)

![Live Dashboard 2](https://github.com/seneralkan/nyc-taxi/blob/master/img/Live%20Dashboard-2.png)

## 2-) Machine Learning and Streaming Section

The main purpose of this section was predict Estimated Time Arrival(ETA) while streaming NYC Taxi dataset and direct to different Kafka topics based on ETA value.

![ML Pipeline](https://github.com/seneralkan/nyc-taxi/blob/master/ML%20Pipeline.png)