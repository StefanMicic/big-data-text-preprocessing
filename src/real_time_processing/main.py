r"""
 Run the example
    
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/real_time.py zoo1:2181 room-1
    $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 $SPARK_HOME/real_time.py zoo1:2181 room-1
"""
import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def getSparkSessionInstance(sparkConf):
    if "sparkSessionSingletonInstance" not in globals():
        globals()[
            "sparkSessionSingletonInstance"
        ] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic1>", file=sys.stderr)
        sys.exit(-1)

    conf = (
        SparkConf().setAppName("Step 3").setMaster("spark://spark-master:7077")
    )
    sc = SparkContext(conf=conf)

    quiet_logs(sc)

    ssc = StreamingContext(sc, 1)

    zooKeeper, topic1 = sys.argv[1:]

    kvs = KafkaUtils.createStream(
        ssc, zooKeeper, "spark-streaming-consumer", {topic1: 1}
    )

    lines = kvs.map(lambda x: x[1])

    data = lines.map(
        lambda line: (
            line.split()[0],
            line.split()[1]
        )
    )


    def process(time, rdd):
        print("========= %s USAO =========" % str(time))

        try:
            spark = getSparkSessionInstance(rdd.context.getConf())
            row_rdd = rdd.map(
                lambda w: Row(
                    overall=w[0], reviewText=w[1]
                )
            )
            text_dataframe = spark.createDataFrame(row_rdd)
            text_dataframe.write.format("csv").mode("append").save(
                HDFS_NAMENODE + "/home/output.csv"
            )

        except Exception as e:
            print(e)


    data.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
